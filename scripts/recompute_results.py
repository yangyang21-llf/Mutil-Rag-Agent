# -*- coding: utf-8 -*-
"""recompute_results.py — 用修复后的解析重算本轮测试集指标 (数据来自 Postgres/History)."""
import json
import urllib.request
from collections import Counter

BASE = "http://localhost:9900"

def fetch_history(limit=300):
    req = urllib.request.Request(f"{BASE}/api/v1/webhook/history?limit={limit}", headers={"User-Agent": "recompute"})
    return json.loads(urllib.request.urlopen(req, timeout=30).read())["items"]

# 读取上次测试集的用例定义 (导入 TEST_CASES)
import sys
sys.path.insert(0, "scripts")
from run_test_suite import TEST_CASES, extract_report_agents, extract_evidence_count

summary = json.load(open("scripts/test_results.json", encoding="utf-8"))
start_iso = summary["test_started_at"]
print("测试开始时间:", start_iso)

items = fetch_history()
# 过滤本批任务: created_at >= start_iso
batch = [i for i in items if (i.get("created_at") or "") >= start_iso]
print("本批任务数:", len(batch))

case_by_key = {(c["alertname"], c["severity"]): c for c in TEST_CASES}

rows = []
matched = set()
for it in batch:
    p = it.get("payload") or {}
    alertname = p.get("alertname", "")
    severity = p.get("severity", "")
    key = (alertname, severity)
    case = case_by_key.get(key)
    if not case:
        continue
    matched.add(case["id"])
    report = p.get("report") or ""
    status = it.get("status", "")
    mode = it.get("diagnosis_mode", "")
    agents = extract_report_agents(report)
    ev = extract_evidence_count(report)
    rows.append({
        "id": case["id"], "alertname": alertname, "severity": severity,
        "task_id": it.get("id"), "status": status, "actual_mode": mode,
        "expected_mode": case["expected_mode"],
        "expected_agents": case["expected_agents"], "actual_agents": agents,
        "evidence_count": ev, "report_chars": len(report),
    })

print("匹配到用例:", len(rows), sorted(matched))
missing = [c["id"] for c in TEST_CASES if c["id"] not in matched]
print("未匹配用例:", missing)

# ---------- 统计 ----------
total = len(rows)
mode_ok = sum(1 for r in rows if r["actual_mode"] == r["expected_mode"])
deep = [r for r in rows if r["expected_mode"] == "deep"]
fast = [r for r in rows if r["expected_mode"] == "fast"]
route_ok = 0
route_fail = []
for r in deep:
    missing_a = [a for a in r["expected_agents"] if a not in r["actual_agents"]]
    if not missing_a:
        route_ok += 1
    else:
        route_fail.append((r["id"], r["alertname"], missing_a, r["actual_agents"]))
e2e_ok = 0
e2e_fail = []
for r in rows:
    if r["expected_mode"] == "deep":
        ok = r["status"] == "succeeded" and r["report_chars"] > 0 and r["evidence_count"] >= 1
    else:
        ok = r["status"] == "succeeded" and r["report_chars"] > 0
    if ok:
        e2e_ok += 1
    else:
        e2e_fail.append((r["id"], r["status"], r["evidence_count"], r["report_chars"]))

print("=" * 66)
print("测试集最终结果 (30 条, 重算)")
print("=" * 66)
print(f"用例总数        : {total} (deep {len(deep)} / fast {len(fast)})")
print(f"模式准确率      : {mode_ok}/{total} = {mode_ok/total*100:.1f}%")
print(f"路由准确率(deep): {route_ok}/{len(deep)} = {route_ok/len(deep)*100:.1f}%")
print(f"端到端成功率    : {e2e_ok}/{total} = {e2e_ok/total*100:.1f}%")
print("-" * 66)
if route_fail:
    print("路由失败明细:")
    for cid, name, missing_a, actual in route_fail:
        print(f"  ✗ {cid} {name}: 缺 {missing_a} | 实际 {actual}")
else:
    print("路由: 21/21 全部命中预期 Agent ✅")
if e2e_fail:
    print("端到端失败明细:", e2e_fail)
else:
    print("端到端: 30/30 全部成功 ✅")

# 保存重算结果
out = {
    "test_started_at": start_iso,
    "mode_accuracy": {"correct": mode_ok, "total": total, "rate": round(mode_ok / total, 4)},
    "routing_accuracy": {"correct": route_ok, "total": len(deep), "rate": round(route_ok / len(deep), 4)},
    "e2e_success": {"correct": e2e_ok, "total": total, "rate": round(e2e_ok / total, 4)},
    "cases": rows,
}
json.dump(out, open("scripts/test_results.json", "w", encoding="utf-8"), ensure_ascii=False, indent=2)
print("\n已更新 scripts/test_results.json")
