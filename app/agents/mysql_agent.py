"""MySQLAgent —— Deep Diagnosis 的 MySQL 数据库健康专项 subagent。

与 InfraAgent 错位:
  - InfraAgent 关心容器 / 端口 / DNS / HTTP 等基础设施健康
  - MySQLAgent 关心 MySQL 实例: 连通性、连接数是否打满、慢查询、主从复制延迟

只使用只读工具 (check_mysql_* / query_mysql_*)。即使 MySQL MCP 未连接,
也会退化成"本机无法访问 MySQL"的明确说明, 不影响 deep graph 继续输出报告。
"""

from typing import Any, Dict, List

from loguru import logger

from app.agents.state_deep import DeepDiagnosisState
from app.incidents.models import EvidenceSource
from app.runtime.transitions import DEEP_AGENT_DONE, make_transition


# 本科室只挑 MySQL MCP Server 提供的 4 个只读工具
_MCP_MYSQL_TOOL_NAMES = {
    "check_mysql_connectivity",
    "query_mysql_status",
    "query_mysql_processlist",
    "query_mysql_replication",
}


def _load_mysql_tools() -> List[Any]:
    """从 MCP 工具池里挑选本科室的工具 (全部来自 MySQL MCP Server)."""
    from app.core.mcp_client import mcp_client_manager

    tools: List[Any] = []
    seen: set[str] = set()
    for tool in mcp_client_manager.tools:
        name = getattr(tool, "name", "")
        if name in _MCP_MYSQL_TOOL_NAMES and name not in seen:
            tools.append(tool)
            seen.add(name)
    return tools


# 岗位说明书，你是谁，你要干啥
_SYSTEM_PROMPT = (
    "你是 SRE 数据库专家 (MySQL Agent), 隶属于一个多 Agent 诊断团队。\n"
    "你的职责: 围绕给定故障现象, 只读排查 MySQL 实例健康, 包括: 连通性、"
    "连接数是否打满、慢查询堆积、主从复制延迟。\n"
    "硬性约束:\n"
    "1. 只能做只读取证, 禁止任何写操作 (不执行 UPDATE / DELETE / DDL / 重启)。\n"
    "2. 优先判断: 实例能否连接、当前连接数是否接近上限、是否有长时间运行的慢查询、"
    "主从复制是否中断或延迟。\n"
    "3. 优先使用 query_mysql_processlist 定位慢查询, min_seconds 按业务 SLA 设置 "
    "(如正常查询应在 1 秒内, 则设 2 秒筛出异常慢的)。\n"
    "4. 若 MySQL 工具不可用或连接被拒, 明确说明只完成了本机视角检查, 不要编造数据库状态。\n"
    "5. summary 必须点名关键异常或明确说未观察到异常, <=350 字。"
)


# 把告警现象拼成任务单发送给AI
def _build_user_prompt(incident_text: str) -> str:
    text = (incident_text or "").strip() or "(未提供现象, 默认检查 MySQL 实例健康)"
    return (
        "故障现象:\n"
        f"{text}\n\n"
        "请按上述约束做 MySQL 数据库取证, 输出一段 summary。"
    )


# 从AI的回答中提取总结，记录它用了哪些工具
def _summarize_messages(messages: List[Any]) -> tuple[str, List[Dict[str, Any]]]:
    last = messages[-1] if messages else None
    raw = getattr(last, "content", "") if last is not None else ""
    summary = (raw if isinstance(raw, str) else str(raw)).strip() or "(mysql_agent 无输出)"

    tool_calls: List[Dict[str, Any]] = []
    for msg in messages or []:
        if getattr(msg, "type", None) == "tool":
            tool_calls.append({
                "name": getattr(msg, "name", ""),
                "preview": str(getattr(msg, "content", ""))[:500],
            })
    return summary, tool_calls


def _evidence(summary: str, content: Dict[str, Any], *, tool_call_count: int, error: str = "") -> Dict[str, Any]:
    metadata: Dict[str, Any] = {"agent": "mysql_agent", "tool_call_count": tool_call_count}
    if error:
        metadata["error_type"] = error
    return {
        "source": str(EvidenceSource.MCP_TOOL_RESULT),
        "type": "mysql_snapshot",
        "summary": summary[:2000],
        "content": content,
        "metadata": metadata,
    }


# 科室入口：加载工具-让AI干活-打包证据-返回
async def run_mysql_agent(state: DeepDiagnosisState) -> DeepDiagnosisState:
    """Deep graph 的 mysql_agent 节点入口."""
    incident_text = state.get("input") or ""
    task_id = state.get("task_id") or ""

    try:
        from app.core.llm import get_chat_llm
        from app.runtime.agent_harness import get_agent_harness
        from app.runtime.tool_runner import run_parallel_agent

        harness = get_agent_harness()
        llm = get_chat_llm(model=harness.executor_model(), temperature=0, streaming=False)
        result = await run_parallel_agent(
            llm=llm,
            tools=_load_mysql_tools(),
            system_prompt=_SYSTEM_PROMPT,
            inputs={"messages": [("user", _build_user_prompt(incident_text))]},
            max_iters=4,
            max_parallel=4,
            decisions=None,
        )
        summary, tool_calls = _summarize_messages(result.get("messages") or [])
        logger.info(f"[deep] mysql_agent: tools={len(tool_calls)} summary={summary[:80]!r}")
        ev = _evidence(
            summary,
            content={"tool_calls": tool_calls, "task_id": task_id},
            tool_call_count=len(tool_calls),
        )
        return {
            "evidences": [ev],
            "transition_history": [make_transition("mysql_agent", DEEP_AGENT_DONE, f"tools={len(tool_calls)}")],
        }
    except Exception as exc:
        logger.exception(f"[deep] mysql_agent failed: {exc}")
        ev = _evidence(
            summary=f"mysql_agent 执行失败: {type(exc).__name__}: {exc}",
            content={"error": True, "task_id": task_id},
            tool_call_count=0,
            error=type(exc).__name__,
        )
        return {
            "evidences": [ev],
            "transition_history": [make_transition("mysql_agent", DEEP_AGENT_DONE, f"error: {type(exc).__name__}")],
        }
