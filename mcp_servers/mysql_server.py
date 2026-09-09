"""MySQL 诊断 MCP Server.

提供 MySQL 数据库健康排查能力, 让 OnCall Agent 能真实诊断:
  - MySQL 连接不上 / 连接数打满 / 慢查询堆积 / 主从复制延迟

工具 (4 个):
  - check_mysql_connectivity: TCP 连通性 + 账号登录验证
  - query_mysql_status:      抓取关键运行指标 (连接数/慢查询数/运行时长)
  - query_mysql_processlist: 列出执行中的 SQL, 定位慢查询与锁等待
  - query_mysql_replication: 查看主从复制状态与延迟

底层: pymysql (纯 Python MySQL 客户端), 只做只读查询 (SHOW / SELECT),
绝不修改任何数据. 所有工具返回字符串 (成功或失败信息), 供 LLM 读取.
"""

import socket

import pymysql
from fastmcp import FastMCP

mcp = FastMCP(name="MySQLServer")

# ---------- 常量 (连接保护) ----------
_DEFAULT_PORT = 3306          # MySQL 默认端口
_CONNECT_TIMEOUT_SEC = 5      # 建立连接超时 (秒)
_QUERY_TIMEOUT_SEC = 10       # 查询超时 (秒)


# ---------- 公共辅助函数 ----------
# 进门先过安检
def _parse_host_port(host: str, port: int) -> tuple[str, int]:
    """校验并规范化 host/port, 非法输入直接抛错拒绝."""
    host = (host or "").strip()
    if not host:
        raise ValueError("host 不能为空")
    port = int(port or _DEFAULT_PORT)
    if not (1 <= port <= 65535):
        raise ValueError(f"port 超出范围: {port}")
    return host, port


def _connect(host: str, port: int, user: str, password: str):
    """建立 MySQL 连接, 带超时保护, 返回连接对象."""
    user = (user or "").strip()
    if not user:
        raise ValueError("user 不能为空")
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password or "",
        connect_timeout=_CONNECT_TIMEOUT_SEC,
        read_timeout=_QUERY_TIMEOUT_SEC,
        write_timeout=_QUERY_TIMEOUT_SEC,
        charset="utf8mb4",
    )


# ---------- 工具 1: 连通性检查 ----------
@mcp.tool(
    name="check_mysql_connectivity",
    description=(
        "检测 MySQL 是否可连接: 先做 TCP 端口探测, 再用账号密码登录验证. "
        "返回连通状态和 MySQL 版本号. 参数: host(IP或域名), port(默认3306), "
        "user, password. 用于诊断 '数据库连接不上 / 密码错误 / 端口不通'."
    ),
)
def check_mysql_connectivity(
    host: str,
    port: int = _DEFAULT_PORT,
    user: str = "root",
    password: str = "",
) -> str:
    try:
        host, port = _parse_host_port(host, port)
        # 1) TCP 端口探测 (先确认端口通不通)
        with socket.create_connection((host, port), timeout=_CONNECT_TIMEOUT_SEC):
            pass
        # 2) 登录验证 (再确认账号密码对不对)
        conn = _connect(host, port, user, password)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT VERSION()")
                version = cur.fetchone()[0]
        finally:
            conn.close()
        return f"[成功] MySQL 连接正常 | {host}:{port} | 版本={version}"
    except pymysql.err.OperationalError as e:
        return f"[失败] MySQL 登录被拒 (检查账号/密码/权限): {e}"
    except socket.timeout:
        return f"[失败] 连接 {host}:{port} 超时 ({_CONNECT_TIMEOUT_SEC}s)"
    except Exception as e:
        return f"[失败] 无法连接 MySQL {host}:{port}: {e}"


# ---------- 工具 2: 关键运行指标 ----------
@mcp.tool(
    name="query_mysql_status",
    description=(
        "抓取 MySQL 关键运行指标: 当前连接数 / 历史最大连接数 / 连接上限 / "
        "慢查询累计数 / 运行时长. 参数: host, port, user, password. "
        "用于诊断 '连接数打满 / 慢查询暴涨 / 实例卡顿'."
    ),
)
def query_mysql_status(
    host: str,
    port: int = _DEFAULT_PORT,
    user: str = "root",
    password: str = "",
) -> str:
    try:
        host, port = _parse_host_port(host, port)
        conn = _connect(host, port, user, password)
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW GLOBAL STATUS")
                rows = {k: v for k, v in cur.fetchall()}  # 全量状态 → 字典
                cur.execute("SHOW VARIABLES LIKE 'max_connections'")
                max_conn = cur.fetchone()[1]
        finally:
            conn.close()
        # 只挑对诊断有用的指标 (取不到就显示 N/A, 不编造)
        def g(key: str) -> str:
            return str(rows.get(key, "N/A"))

        return (
            f"## MySQL 关键指标 ({host}:{port})\n"
            f"- 运行时长 Uptime: {g('Uptime')} 秒\n"
            f"- 当前连接数 Threads_connected: {g('Threads_connected')}\n"
            f"- 历史最大连接数 Max_used_connections: {g('Max_used_connections')}\n"
            f"- 连接数上限 max_connections: {max_conn}\n"
            f"- 慢查询累计数 Slow_queries: {g('Slow_queries')}\n"
            f"- 累计查询数 Questions: {g('Questions')}\n"
            f"- 累计连接次数 Connections: {g('Connections')}"
        )
    except Exception as e:
        return f"[失败] 获取 MySQL 状态失败: {e}"


# ---------- 工具 3: 慢查询进程列表 ----------
@mcp.tool(
    name="query_mysql_processlist",
    description=(
        "列出 MySQL 当前正在执行的进程(SQL), 按执行时间从长到短排序, "
        "用于定位慢查询和锁等待. min_seconds 只显示执行超过该秒数的查询 "
        "(默认 0 = 全部). 参数: host, port, user, password, min_seconds."
    ),
)
def query_mysql_processlist(
    host: str,
    port: int = _DEFAULT_PORT,
    user: str = "root",
    password: str = "",
    min_seconds: int = 0,
) -> str:
    try:
        host, port = _parse_host_port(host, port)
        min_seconds = max(0, int(min_seconds or 0))
        conn = _connect(host, port, user, password)
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW FULL PROCESSLIST")
                cols = [d[0] for d in cur.description]  # 列名
                rows = cur.fetchall()
        finally:
            conn.close()
        if not rows:
            return "[信息] 当前没有任何活动连接"

        # 按执行时间 Time 列降序, 只保留超过 min_seconds 的
        time_idx = cols.index("Time")
        filtered = [
            r for r in rows
            if r[time_idx] is not None and int(r[time_idx]) >= min_seconds
        ]
        filtered.sort(key=lambda r: int(r[time_idx] or 0), reverse=True)
        if not filtered:
            return f"[信息] 没有执行超过 {min_seconds}s 的查询 (共 {len(rows)} 个活动连接)"

        # 渲染成 Markdown 表格 (LLM 最好读的格式)
        lines = ["| " + " | ".join(cols) + " |"]
        lines.append("|" + "---|" * len(cols))
        for r in filtered[:20]:  # 最多 20 行, 防止刷屏
            lines.append("| " + " | ".join(str(v) if v is not None else "" for v in r) + " |")
        return f"## 活动进程 (按耗时排序, 共 {len(rows)} 连接)\n\n" + "\n".join(lines)
    except Exception as e:
        return f"[失败] 获取进程列表失败: {e}"


# ---------- 工具 4: 主从复制状态 ----------
@mcp.tool(
    name="query_mysql_replication",
    description=(
        "查看 MySQL 主从复制状态: IO 线程 / SQL 线程是否运行、复制延迟秒数. "
        "参数: host, port, user, password. 用于诊断 '主从延迟 / 复制中断'."
    ),
)
def query_mysql_replication(
    host: str,
    port: int = _DEFAULT_PORT,
    user: str = "root",
    password: str = "",
) -> str:
    try:
        host, port = _parse_host_port(host, port)
        conn = _connect(host, port, user, password)
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW SLAVE STATUS")
                row = cur.fetchone()
                if not row:
                    # MySQL 8.0 改了命令名, 试新写法
                    cur.execute("SHOW REPLICA STATUS")
                    row = cur.fetchone()
                    if not row:
                        return "[信息] 该实例未配置主从复制 (可能为单机)"
                cols = [d[0] for d in cur.description]
                data = dict(zip(cols, row))
        finally:
            conn.close()
        # 兼容新旧两种列名
        io_running = data.get("Replica_IO_Running") or data.get("Slave_IO_Running") or "?"
        sql_running = data.get("Replica_SQL_Running") or data.get("Slave_SQL_Running") or "?"
        behind = data.get("Seconds_Behind_Source") or data.get("Seconds_Behind_Master") or "?"
        return (
            f"## MySQL 主从复制状态\n"
            f"- IO 线程 (拉取 binlog): {io_running}\n"
            f"- SQL 线程 (执行 relay log): {sql_running}\n"
            f"- 复制延迟: {behind} 秒"
        )
    except Exception as e:
        return f"[失败] 获取复制状态失败: {e}"


if __name__ == "__main__":
    print("[mcp] mysql_server starting on http://0.0.0.0:8012/mcp ...")
    mcp.run(transport="streamable-http", host="0.0.0.0", port=8012)
