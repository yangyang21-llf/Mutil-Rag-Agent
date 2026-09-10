# -*- coding: utf-8 -*-
"""
run_test_suite.py — 二次开发「MySQL 诊断专家」正规测试集

设计 30 条故障告警用例, 统计三个指标:
  1) 模式准确率      : 预期 fast/deep 模式 vs 实际 diagnosis_mode
  2) 路由准确率      : deep 用例中, 实际被调度的 Agent 是否覆盖预期 Agent 集合
  3) MySQL 工具调用成功率 : Postgres tool_calls 表中 mysql 工具 (success) / 总数
  4) 端到端诊断成功率     : status=succeeded 且 报告非空 且 证据>=1

用法:
  python scripts/run_test_suite.py            # 全量运行
  python scripts/run_test_suite.py --send-only  # 只发送告警, 不统计
  python scripts/run_test_suite.py --report-only  # 只统计上次已发送的结果

说明:
  - 发送遵守 webhook 限流: 单 IP 每秒 1 条 → 发送间隔 1.2s
  - 结果写入 scripts/test_results.json (测试证据, 可提交仓库)
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone

BASE_URL = "http://localhost:9900"
HISTORY_URL = f"{BASE_URL}/api/v1/webhook/history"
WEBHOOK_URL = f"{BASE_URL}/api/v1/webhook/alertmanager"
SEND_INTERVAL = 1.2          # 限流: 单 IP 每秒 1 条, 留 0.2s 余量
POLL_INTERVAL = 15
MAX_WAIT_SEC = 25 * 60       # 25 分钟上限 (上一批 30 条仍在队列中, 需等排空)
RESULTS_PATH = "scripts/test_results.json"

# ============================================================
# 测试用例集 (30 条)
# expected_agents 由 _PLAN_KEYWORDS 词表人工推导:
#   G1 metric: cpu/内存/磁盘/负载/io...
#   G2 log   : alert/error/日志/失败/5xx...(query 里必有 "alert", 故 deep 恒含 log_agent)
#   G3 infra : 超时/connection refused/dns/网络/容器/依赖/延迟/慢请求...
#   G4 runbook: runbook/sop/手册/流程/步骤...(带 runbook_url 时必命中)
#   G5 mysql : mysql/数据库/慢查询/连接数/threads/主从/复制/replica...
# ============================================================
TEST_CASES: list[dict] = [
    # ---------- A. MySQL 分支 (8 条, deep) ----------
    {"id": "A01", "alertname": "MySQLLongRunningQuery", "severity": "critical",
     "service": "mysql", "instance": "mysql-primary-01:3306",
     "summary": "MySQL 慢查询堆积 45 条, 连接数打满",
     "description": "mysql_slow_queries 新增 45 条, 当前连接数 148/151, 上游服务出现数据库连接超时",
     "runbook_url": "https://example.com/runbooks/mysql-slow-query",
     "expected_mode": "deep", "expected_agents": ["log_agent", "infra_agent", "mysql_agent", "runbook_agent"]},
    {"id": "A02", "alertname": "MySQLReplicationLag", "severity": "critical",
     "service": "mysql", "instance": "mysql-slave-02:3306",
     "summary": "主从复制延迟 120 秒",
     "description": "replica 复制延迟持续 120s, 主从数据不一致, 从库查询返回旧数据",
     "runbook_url": "https://example.com/runbooks/mysql-replication",
     "expected_mode": "deep", "expected_agents": ["log_agent", "mysql_agent", "runbook_agent"]},
    {"id": "A03", "alertname": "MySQLConnectionLimit", "severity": "critical",
     "service": "mysql", "instance": "mysql-primary-01:3306",
     "summary": "MySQL 连接数打满 148/151",
     "description": "连接数接近上限, threads 大量堆积, 新连接无法建立",
     "runbook_url": "https://example.com/runbooks/mysql-connection",
     "expected_mode": "deep", "expected_agents": ["log_agent", "mysql_agent", "runbook_agent"]},
    {"id": "A04", "alertname": "MySQLSlowQueryAlert", "severity": "critical",
     "service": "mysql", "instance": "mysql-primary-01:3306",
     "summary": "MySQL 慢查询超过 5 秒",
     "description": "slow query 堆积, mysql 慢查询日志 10 分钟新增 30 条",
     "runbook_url": None,
     "expected_mode": "deep", "expected_agents": ["log_agent", "mysql_agent"]},
    {"id": "A05", "alertname": "MySQLDeadlockDetected", "severity": "critical",
     "service": "mysql", "instance": "mysql-primary-01:3306",
     "summary": "数据库死锁 12 次",
     "description": "检测到数据库死锁 12 次, 事务批量回滚, 业务写入失败",
     "runbook_url": "https://example.com/runbooks/mysql-deadlock",
     "expected_mode": "deep", "expected_agents": ["log_agent", "mysql_agent", "runbook_agent"]},
    {"id": "A06", "alertname": "MySQLDataDiskFull", "severity": "critical",
     "service": "mysql", "instance": "mysql-primary-01:3306",
     "summary": "MySQL 数据盘空间不足 3%",
     "description": "mysql 数据目录磁盘空间不足, 实例即将进入只读模式",
     "runbook_url": "https://example.com/runbooks/mysql-disk-full",
     "expected_mode": "deep", "expected_agents": ["log_agent", "metric_agent", "mysql_agent", "runbook_agent"]},
    {"id": "A07", "alertname": "MySQLConnectionTimeout", "severity": "critical",
     "service": "mysql", "instance": "mysql-primary-01:3306",
     "summary": "数据库连接超时",
     "description": "数据库连接超时, 上游订单服务 5xx 失败率 30%",
     "runbook_url": None,
     "expected_mode": "deep", "expected_agents": ["log_agent", "infra_agent", "mysql_agent"]},
    {"id": "A08", "alertname": "MySQLThreadsRunningHigh", "severity": "critical",
     "service": "mysql", "instance": "mysql-primary-01:3306",
     "summary": "Threads_running 峰值 80",
     "description": "Threads_running 峰值 80, 连接数打满, 慢查询堆积",
     "runbook_url": "https://example.com/runbooks/mysql-slow-query",
     "expected_mode": "deep", "expected_agents": ["log_agent", "mysql_agent", "runbook_agent"]},

    # ---------- B. 网络 / 依赖分支 (6 条, deep) ----------
    {"id": "B01", "alertname": "ServiceUnreachable", "severity": "critical",
     "service": "order-service", "instance": "order-svc-01:8080",
     "summary": "服务不可达",
     "description": "user-service 服务不可达, connection refused, 依赖该服务的调用全部失败",
     "runbook_url": "https://example.com/runbooks/service-unreachable",
     "expected_mode": "deep", "expected_agents": ["log_agent", "infra_agent", "runbook_agent"]},
    {"id": "B02", "alertname": "DNSResolutionFailed", "severity": "critical",
     "service": "gateway", "instance": "gw-01",
     "summary": "DNS 解析失败",
     "description": "dns 解析失败, 上游依赖不可用, 网关无法路由请求",
     "runbook_url": "https://example.com/runbooks/dns-failure",
     "expected_mode": "deep", "expected_agents": ["log_agent", "infra_agent", "runbook_agent"]},
    {"id": "B03", "alertname": "APILatencyHigh", "severity": "critical",
     "service": "api-gateway", "instance": "api-gw-02",
     "summary": "接口延迟飙高",
     "description": "接口 P99 延迟 3s, 慢请求堆积, 调用链超时",
     "runbook_url": None,
     "expected_mode": "deep", "expected_agents": ["log_agent", "infra_agent"]},
    {"id": "B04", "alertname": "PortUnavailable", "severity": "critical",
     "service": "app", "instance": "app-server-05",
     "summary": "端口无法监听",
     "description": "端口 8080 无法监听, 容器多次重启, 健康检查失败",
     "runbook_url": "https://example.com/runbooks/port-failure",
     "expected_mode": "deep", "expected_agents": ["log_agent", "infra_agent", "runbook_agent"]},
    {"id": "B05", "alertname": "ServiceCallTimeout", "severity": "critical",
     "service": "payment-service", "instance": "payment-svc-03",
     "summary": "服务调用超时",
     "description": "服务调用 timeout 超时, trace 显示调用链断裂, 下游无响应",
     "runbook_url": None,
     "expected_mode": "deep", "expected_agents": ["log_agent", "infra_agent"]},
    {"id": "B06", "alertname": "NetworkPartition", "severity": "critical",
     "service": "k8s-cluster", "instance": "k8s-node-01",
     "summary": "网络分区",
     "description": "网络分区, 节点之间无法连接, docker 容器状态异常",
     "runbook_url": "https://example.com/runbooks/network-partition",
     "expected_mode": "deep", "expected_agents": ["log_agent", "infra_agent", "runbook_agent"]},

    # ---------- C. 资源分支 (4 条, deep) ----------
    {"id": "C01", "alertname": "CPUHighUsage", "severity": "critical",
     "service": "app-server", "instance": "app-server-07:9100",
     "summary": "CPU 使用率 95%",
     "description": "cpu 使用率持续 95%, 系统负载 20, 接口响应变慢",
     "runbook_url": "https://example.com/runbooks/cpu-high",
     "expected_mode": "deep", "expected_agents": ["log_agent", "metric_agent", "runbook_agent"]},
    {"id": "C02", "alertname": "ProcessMemoryLeak", "severity": "critical",
     "service": "payment-service", "instance": "payment-svc-pod-x9f3:8080",
     "summary": "内存使用率 90%",
     "description": "内存使用率 90%, 进程内存持续增长, GC 后不释放",
     "runbook_url": "https://example.com/runbooks/memory-leak",
     "expected_mode": "deep", "expected_agents": ["log_agent", "metric_agent", "runbook_agent"]},
    {"id": "C03", "alertname": "DiskSpaceLow", "severity": "critical",
     "service": "node", "instance": "k8s-node-03:9100",
     "summary": "磁盘空间不足 3%",
     "description": "磁盘空间不足, io 等待升高, 日志写入失败",
     "runbook_url": "https://example.com/runbooks/disk-full",
     "expected_mode": "deep", "expected_agents": ["log_agent", "metric_agent", "runbook_agent"]},
    {"id": "C04", "alertname": "OOMKilled", "severity": "critical",
     "service": "recommend-svc", "instance": "recommend-pod-88:8080",
     "summary": "进程被 OOM Kill",
     "description": "进程 OOM 被 kill, 内存耗尽, 容器重启 5 次",
     "runbook_url": None,
     "expected_mode": "deep", "expected_agents": ["log_agent", "metric_agent"]},

    # ---------- D. Runbook / 通用分支 (3 条, deep) ----------
    {"id": "D01", "alertname": "UnknownFault", "severity": "critical",
     "service": "unknown", "instance": "unknown",
     "summary": "未知故障求助",
     "description": "遇到未知故障, 怎么处理? 如何排查? 请按 sop 流程给出处置步骤",
     "runbook_url": None,
     "expected_mode": "deep", "expected_agents": ["log_agent", "runbook_agent"]},
    {"id": "D02", "alertname": "UnknownAlert", "severity": "critical",
     "service": "unknown", "instance": "unknown",
     "summary": "未知告警",
     "description": "收到一条未知告警, 现象不明, 需要全面诊断",
     "runbook_url": None,
     "expected_mode": "deep", "expected_agents": ["log_agent", "metric_agent"]},
    {"id": "D03", "alertname": "DatabaseRunbookConsult", "severity": "critical",
     "service": "mysql", "instance": "mysql-primary-01:3306",
     "summary": "数据库故障处置咨询",
     "description": "数据库故障, 请参考手册按流程给出排查步骤",
     "runbook_url": None,
     "expected_mode": "deep", "expected_agents": ["log_agent", "mysql_agent", "runbook_agent"]},

    # ---------- E. Fast 分支 (9 条, warning) ----------
    {"id": "E01", "alertname": "CPUHighUsageWarn", "severity": "warning",
     "service": "app-server", "instance": "app-server-07:9100",
     "summary": "CPU 使用率持续 92%",
     "description": "cpu 使用率持续 92%, 接口 P99 延迟从 80ms 上涨到 1.2s",
     "runbook_url": "https://example.com/runbooks/cpu-high",
     "expected_mode": "fast", "expected_agents": []},
    {"id": "E02", "alertname": "ProcessMemoryLeakWarn", "severity": "warning",
     "service": "payment-service", "instance": "payment-svc-pod-x9f3:8080",
     "summary": "内存泄漏迹象",
     "description": "进程内存从 800MB 增长到 3.2GB, 预计 90 分钟后 OOM",
     "runbook_url": None,
     "expected_mode": "fast", "expected_agents": []},
    {"id": "E03", "alertname": "KafkaISRShrinking", "severity": "warning",
     "service": "kafka", "instance": "kafka-broker-02:9092",
     "summary": "Kafka ISR 频繁收缩",
     "description": "topic=user-events 分区 3 ISR 过去 10 分钟收缩 7 次, 副本同步延迟超过 30s",
     "runbook_url": None,
     "expected_mode": "fast", "expected_agents": []},
    {"id": "E04", "alertname": "DiskSpaceLowWarn", "severity": "warning",
     "service": "node", "instance": "k8s-node-03:9100",
     "summary": "磁盘空间不足 8%",
     "description": "节点磁盘空间剩余 8%, 持续下降",
     "runbook_url": None,
     "expected_mode": "fast", "expected_agents": []},
    {"id": "E05", "alertname": "RedisMemoryHigh", "severity": "warning",
     "service": "redis", "instance": "redis-master-01:6379",
     "summary": "Redis 内存使用率 85%",
     "description": "redis 内存使用率 85%, 超过阈值, 需要关注",
     "runbook_url": "https://example.com/runbooks/redis-memory-high",
     "expected_mode": "fast", "expected_agents": []},
    {"id": "E06", "alertname": "MySQLSlaveSlowQuery", "severity": "warning",
     "service": "mysql", "instance": "mysql-slave-01:3306",
     "summary": "从库慢查询",
     "description": "从库存在超过 5 秒的慢查询, 可能影响读业务",
     "runbook_url": None,
     "expected_mode": "fast", "expected_agents": []},
    {"id": "E07", "alertname": "ServiceUnavailable", "severity": "warning",
     "service": "order-service", "instance": "order-svc-02:8080",
     "summary": "服务短暂不可用",
     "description": "服务出现短暂不可用, 已自动恢复, 需要观察",
     "runbook_url": None,
     "expected_mode": "fast", "expected_agents": []},
    {"id": "E08", "alertname": "GenericAlert", "severity": "warning",
     "service": "general", "instance": "node-01",
     "summary": "一般告警",
     "description": "一般性告警, 无明确故障现象",
     "runbook_url": None,
     "expected_mode": "fast", "expected_agents": []},
    {"id": "E09", "alertname": "UnknownAlertWarn", "severity": "warning",
     "service": "unknown", "instance": "unknown",
     "summary": "未知告警",
     "description": "未知告警, 缺少实例信息",
     "runbook_url": None,
     "expected_mode": "fast", "expected_agents": []},
]


def build_payload(case: dict) -> dict:
    """构造 Alertmanager v4 webhook payload (与 mock_alert.py 同构)."""
    now = datetime.now(timezone.utc).isoformat()
    annotations: dict = {"summary": case["summary"], "description": case["description"]}
    if case.get("runbook_url"):
        annotations["runbook_url"] = case["runbook_url"]
    return {
        "version": "4",
        "groupKey": f"{{}}:{{alertname='{case['alertname']}'}}",
        "truncatedAlerts": 0,
        "status": "firing",
        "receiver": "multi-agent-aiops",
        "groupLabels": {"alertname": case["alertname"]},
        "commonLabels": {"alertname": case["alertname"], "severity": case["severity"], "service": case["service"]},
        "commonAnnotations": {},
        "externalURL": "http://prometheus.example.com:9090",
        "alerts": [{
            "status": "firing",
            "labels": {
                "alertname": case["alertname"],
                "severity": case["severity"],
                "service": case["service"],
                "instance": case["instance"],
            },
            "annotations": annotations,
            "startsAt": now,
            "endsAt": "0001-01-01T00:00:00Z",
            "generatorURL": "http://prometheus.example.com:9090/graph",
            "fingerprint": f"test-{case['id'].lower()}-{int(time.time())}",
        }],
    }


def http_json(url: str, *, method: str = "GET", body: dict | None = None, timeout: int = 30):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json", "User-Agent": "test-suite/1.0"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def send_all() -> list[dict]:
    """逐条发送, 遵守限流, 返回每个 case 的 task_id 映射."""
    sent: list[dict] = []
    print(f"[发送] 共 {len(TEST_CASES)} 条, 间隔 {SEND_INTERVAL}s (遵守单源限流)...")
    for i, case in enumerate(TEST_CASES, 1):
        payload = build_payload(case)
        try:
            resp = http_json(WEBHOOK_URL, method="POST", body=payload)
            accepted = resp.get("accepted") or []
            task_id = accepted[0].get("task_id", "") if accepted else ""
            sent.append({"id": case["id"], "task_id": task_id, "resp": resp})
            print(f"  [{i:02d}/{len(TEST_CASES)}] {case['id']} {case['alertname']} "
                  f"({case['severity']}) -> task_id={task_id}")
        except Exception as e:
            sent.append({"id": case["id"], "task_id": "", "error": str(e)})
            print(f"  [{i:02d}/{len(TEST_CASES)}] {case['id']} 发送失败: {e}")
        time.sleep(SEND_INTERVAL)
    return sent


def fetch_history(limit: int = 200) -> list[dict]:
    items: list[dict] = []
    try:
        resp = http_json(f"{HISTORY_URL}?limit={limit}")
        items = resp.get("items", [])
    except Exception as e:
        print(f"[警告] 拉取 history 失败: {e}")
    return items


def extract_report_agents(report: str) -> list[str]:
    """从 deep 报告元数据 '产证成功 Agent: `a`, `b`' 解析实际调度 agents."""
    if not report:
        return []
    m = re.search(r"产证成功\s*Agent[:：]\s*`([^`]+(?:`\s*,\s*`[^`]+)*)`", report)
    if not m:
        return []
    return [x.strip("` ") for x in re.split(r"[`,，]+", m.group(1)) if x.strip("` ")]


def extract_evidence_count(report: str) -> int:
    if not report:
        return 0
    m = re.search(r"证据链\s*\(?共\s*(\d+)\s*条\)?", report)
    if m:
        return int(m.group(1))
    # fast 报告无"证据链"段落, 用"证据"出现次数兜底
    return report.count("Evidence") + report.count("evidence") + report.count("证据")


def query_mysql_tool_stats(start_iso: str) -> dict:
    """从 Postgres tool_calls 表统计 mysql 工具调用成功率."""
    sql = (
        "SELECT tc.tool_name, tc.status, count(*) FROM tool_calls tc "
        "JOIN diagnosis_tasks t ON tc.task_id = t.id "
        f"WHERE tc.tool_name LIKE '%mysql%' AND t.created_at >= '{start_iso}' "
        "GROUP BY tc.tool_name, tc.status ORDER BY tc.tool_name;"
    )
    cmd = ["docker", "compose", "exec", "-T", "postgres",
           "psql", "-U", "multi_agent", "-d", "multi_agent_aiops", "-t", "-A", "-F", "|", "-c", sql]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=60, cwd=".")
        rows = [l for l in out.stdout.strip().splitlines() if l.strip()]
        stats = {"total": 0, "success": 0, "failed": 0, "by_tool": {}}
        for line in rows:
            parts = line.split("|")
            if len(parts) != 3:
                continue
            tool, status, cnt = parts[0].strip(), parts[1].strip(), int(parts[2])
            stats["total"] += cnt
            if status == "ok":
                stats["success"] += cnt
            else:
                stats["failed"] += cnt
            stats["by_tool"].setdefault(tool, {"success": 0, "failed": 0, "total": 0})
            stats["by_tool"][tool]["total"] += cnt
            stats["by_tool"][tool][status] = stats["by_tool"][tool].get(status, 0) + 1
        return stats
    except Exception as e:
        return {"error": str(e)}


def main() -> None:
    ap = argparse.ArgumentParser(description="MySQL 诊断专家测试集")
    ap.add_argument("--send-only", action="store_true")
    ap.add_argument("--report-only", action="store_true")
    args = ap.parse_args()

    start_iso = datetime.now(timezone.utc).isoformat()
    sent: list[dict] = []

    if not args.report_only:
        sent = send_all()
        print(f"\n[完成] 已发送 {len(sent)} 条告警, 等待 worker 诊断...")
        with open(RESULTS_PATH, "w", encoding="utf-8") as f:
            json.dump({"started_at": start_iso, "sent": sent}, f, ensure_ascii=False, indent=2)
    else:
        with open(RESULTS_PATH, "r", encoding="utf-8") as f:
            saved = json.load(f)
        start_iso = saved["started_at"]
        sent = saved["sent"]

    if args.send_only:
        return

    # ---------- 轮询等待全部完成 ----------
    deadline = time.time() + MAX_WAIT_SEC
    print("[等待] 轮询诊断结果 (deep 约 40-60s/条, 3 worker 并行)...")
    history = fetch_history()
    by_id = {h.get("id"): h for h in history}
    while time.time() < deadline:
        pending = [s for s in sent if s.get("task_id") and by_id.get(s["task_id"], {}).get("status") in (None, "pending", "running")]
        done = len(sent) - len(pending)
        if len(sent) == 0:
            break
        if done >= len(sent):
            break
        time.sleep(POLL_INTERVAL)
        history = fetch_history()
        by_id = {h.get("id"): h for h in history}

    # ---------- 逐条统计 ----------
    rows: list[dict] = []
    mode_ok = route_ok = 0
    e2e_ok = 0
    deep_total = fast_total = 0
    for s in sent:
        h = by_id.get(s.get("task_id"), {}) if s.get("task_id") else {}
        case = next((c for c in TEST_CASES if c["id"] == s["id"]), {})
        status = h.get("status", "missing")
        mode = h.get("diagnosis_mode", "")
        report = ((h.get("payload") or {}).get("report")) or ""
        agents = extract_report_agents(report)
        ev_count = extract_evidence_count(report)

        row = {
            "id": case.get("id"), "alertname": case.get("alertname"),
            "severity": case.get("severity"), "status": status,
            "expected_mode": case.get("expected_mode"), "actual_mode": mode,
            "mode_ok": (mode == case.get("expected_mode")),
            "expected_agents": case.get("expected_agents", []), "actual_agents": agents,
            "route_ok": None, "evidence_count": ev_count, "report_chars": len(report),
            "e2e_ok": False,
        }
        if case.get("expected_mode") == "deep":
            deep_total += 1
            missing = [a for a in case.get("expected_agents", []) if a not in agents]
            row["route_ok"] = (len(missing) == 0)
            row["missing_agents"] = missing
            if row["route_ok"]:
                route_ok += 1
            row["e2e_ok"] = (status == "succeeded" and len(report) > 0 and ev_count >= 1)
            if row["e2e_ok"]:
                e2e_ok += 1
        else:
            fast_total += 1
            row["e2e_ok"] = (status == "succeeded" and len(report) > 0)
            if row["e2e_ok"]:
                e2e_ok += 1
        if row["mode_ok"]:
            mode_ok += 1
        rows.append(row)

    # ---------- MySQL 工具调用统计 ----------
    tool_stats = query_mysql_tool_stats(start_iso)

    # ---------- 汇总 ----------
    total = len(rows)
    route_denom = deep_total
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "test_started_at": start_iso,
        "total_cases": total,
        "deep_cases": deep_total,
        "fast_cases": fast_total,
        "mode_accuracy": {"correct": mode_ok, "total": total, "rate": round(mode_ok / total, 4) if total else 0},
        "routing_accuracy": {"correct": route_ok, "total": route_denom,
                             "rate": round(route_ok / route_denom, 4) if route_denom else 0},
        "mysql_tool_call": tool_stats,
        "e2e_success": {"correct": e2e_ok, "total": total,
                        "rate": round(e2e_ok / total, 4) if total else 0},
        "cases": rows,
    }
    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    # ---------- 打印报告 ----------
    print("\n" + "=" * 68)
    print("测试集结果汇总 (30 条用例)")
    print("=" * 68)
    print(f"用例总数            : {total}  (deep {deep_total} / fast {fast_total})")
    print(f"模式准确率          : {mode_ok}/{total} = {summary['mode_accuracy']['rate']*100:.1f}%")
    print(f"路由准确率 (deep)   : {route_ok}/{route_denom} = {summary['routing_accuracy']['rate']*100:.1f}%")
    ts = tool_stats
    if "error" in ts:
        print(f"MySQL 工具调用       : 查询失败 ({ts['error']})")
    else:
        t, s = ts.get("total", 0), ts.get("success", 0)
        print(f"MySQL 工具调用成功率 : {s}/{t} = {(s/t*100 if t else 0):.1f}%")
        for tool, v in ts.get("by_tool", {}).items():
            print(f"    - {tool}: success={v.get('ok',0)} failed={v.get('failed',0)} total={v['total']}")
    print(f"端到端诊断成功率    : {e2e_ok}/{total} = {summary['e2e_success']['rate']*100:.1f}%")
    print("-" * 68)
    print("逐条明细 (路由/端到端失败项):")
    for r in rows:
        problems = []
        if not r["mode_ok"]:
            problems.append(f"模式 {r['expected_mode']}->{r['actual_mode']}")
        if r.get("route_ok") is False:
            problems.append(f"缺 Agent: {r.get('missing_agents')}")
        if not r["e2e_ok"]:
            problems.append(f"端到端(status={r['status']},ev={r['evidence_count']},report={r['report_chars']}字)")
        if problems:
            print(f"  ✗ {r['id']} {r['alertname']}: {'; '.join(problems)}")
    ok_count = sum(1 for r in rows if not (
        (not r["mode_ok"]) or (r.get("route_ok") is False) or (not r["e2e_ok"])
    ))
    print(f"\n全部通过: {ok_count}/{total}  结果已保存: {RESULTS_PATH}")


if __name__ == "__main__":
    main()
