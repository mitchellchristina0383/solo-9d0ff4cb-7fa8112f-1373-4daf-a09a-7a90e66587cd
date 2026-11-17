#!/usr/bin/env python3
"""端到端自测（仅依赖标准库）：用临时目录拉起两个后端 + 一个代理并逐项核验。

覆盖：
  1. 普通写入落现役节点，备用节点无该写入（无重复投递）；
  2. 慢回执写入在途时切流：切流立即生效，新请求走新节点，
     在途写入仍由旧节点回执，且只存在于旧节点；
  3. 目标节点健康故障时切流被拦截（409），旧路由保持；
  4. 连续两次切流都不打断各自的在途写入（再次切流安全）；
  5. 回执超时：返回 504 + TIMEOUT_UNCERTAIN，不自动补投；
     事后向"原送达节点"查询可确认其实已落库并回执；
  6. 全量核对：两侧后端写入 ID 集合不相交，每条写入恰好出现一次；
  7. 同号同内容并发重试：恰好一次首次投递，其余全部抑制为只读核实；
  8. 切流后同号重试：仍只查首次绑定节点，新现役节点收不到任何写入；
  9. 同号异内容：409 冲突且原记录零改动、零投递；
  10. 原节点故障不可达：重试报不确定、绝不改投备用节点，恢复后可核实；
  11. 原节点查无记录：报不确定且不补投，代理侧原记录保持不变；
  12. 代理重启后重试：绑定/尝试历史持久化，重启 + 切流后语义不变；
  13. 重启后再次全量核对：不相交、每条恰好一次、投递总数 = 写入数。

用法:
  python3 -I scripts/selftest.py [--python /path/to/venv/python]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PASS, FAIL = 0, 0
RESULTS: list[tuple[bool, str]] = []


def check(cond: bool, name: str, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        RESULTS.append((True, f"PASS  {name}" + (f"  ({detail})" if detail else "")))
        print(f"  \033[32mPASS\033[0m  {name} {detail}")
    else:
        FAIL += 1
        RESULTS.append((False, f"FAIL  {name}  {detail}"))
        print(f"  \033[31mFAIL\033[0m  {name} {detail}")


def http(method: str, url: str, body: dict | None = None, timeout: float = 30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def wait_ready(url: str, timeout: float = 20) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            time.sleep(0.3)
    return False


def wait_status(proxy: str, write_id: str, expect: str, timeout: float = 15) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, s = http("GET", f"{proxy}/api/status")
        for w in s["writes"]:
            if w["id"] == write_id:
                if w["status"] == expect:
                    return w
        time.sleep(0.15)
    raise AssertionError(f"write {write_id} never reached status {expect}")


class SlowPost(threading.Thread):
    def __init__(self, proxy: str, delay: float, payload: str,
                 write_id: str | None = None, wait: bool | None = None):
        super().__init__(daemon=True)
        self.proxy, self.delay, self.payload = proxy, delay, payload
        self.write_id_arg = write_id
        self.wait = wait
        self.result: tuple[int, dict] | None = None
        self.write_id: str | None = None

    def run(self):
        body = {"payload": self.payload}
        if self.write_id_arg:
            body["id"] = self.write_id_arg
        url = f"{self.proxy}/api/writes?delay={self.delay}"
        if self.wait is not None:
            url += f"&wait={'true' if self.wait else 'false'}"
        self.result = http("POST", url, body, timeout=30)
        if self.result:
            self.write_id = self.result[1].get("id")


def node_record_ids(proxy: str, node: str) -> set[str]:
    _, rec = http("GET", f"{proxy}/api/nodes/{node}/records")
    return {r["id"] for r in rec["records"]}


def find_write(proxy: str, write_id: str) -> dict:
    _, s = http("GET", f"{proxy}/api/status")
    return next(w for w in s["writes"] if w["id"] == write_id)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--python", default=sys.executable)
    args = ap.parse_args()

    work = Path(tempfile.mkdtemp(prefix="cutover-selftest-"))
    logs = work / "logs"
    logs.mkdir()
    procs: list[subprocess.Popen] = []
    ports = {"proxy": 18000, "A": 18001, "B": 18002}
    proxy = f"http://127.0.0.1:{ports['proxy']}"

    def spawn(kind: str, app_dir: Path, env: dict, port: int) -> None:
        log = open(logs / f"{kind}.log", "wb")
        p = subprocess.Popen(
            [args.python, "-I", "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(port), "--app-dir", str(app_dir)],
            env={**os.environ, **env}, stdout=log, stderr=subprocess.STDOUT,
        )
        procs.append(p)

    try:
        print("== 启动两个后端 + 代理（读回执超时 2s，便于演练超时场景） ==")
        spawn("backend-a", ROOT / "backend",
              {"NODE_NAME": "A", "BACKEND_DB": str(work / "a.db")}, ports["A"])
        spawn("backend-b", ROOT / "backend",
              {"NODE_NAME": "B", "BACKEND_DB": str(work / "b.db")}, ports["B"])
        ok_a = wait_ready(f"http://127.0.0.1:{ports['A']}/healthz")
        ok_b = wait_ready(f"http://127.0.0.1:{ports['B']}/healthz")
        check(ok_a and ok_b, "两个后端启动并健康", f"A={ok_a} B={ok_b}")

        spawn("proxy", ROOT / "proxy", {
            "NODE_A_NAME": "A", "NODE_B_NAME": "B",
            "BACKEND_A_URL": f"http://127.0.0.1:{ports['A']}",
            "BACKEND_B_URL": f"http://127.0.0.1:{ports['B']}",
            "PROXY_DB": str(work / "proxy.db"),
            "WRITER_READ_TIMEOUT": "2", "CONNECT_TIMEOUT": "2", "PROBE_TIMEOUT": "2",
        }, ports["proxy"])
        check(wait_ready(proxy + "/"), "代理启动并提供控制台页面")

        _, st = http("GET", f"{proxy}/api/status")
        check(st["active"] == "A", "初始现役节点为 A", f"active={st['active']}")

        print("\n== 场景 1：普通写入只落现役节点 A ==")
        code, w0 = http("POST", f"{proxy}/api/writes?delay=0", {"payload": "base-write"})
        check(code == 201 and w0["status"] == "ACKED", "普通写入成功回执",
              f"id={w0.get('id','')[:8]} node={w0.get('node')}")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        check(any(r["id"] == w0["id"] for r in rec_a["records"]), "写入存在于 A 的 SQLite")
        check(not any(r["id"] == w0["id"] for r in rec_b["records"]), "B 上没有该写入（未重复投递）")

        print("\n== 场景 2：慢回执在途切流 A -> B，不中断、不重复 ==")
        slow1 = SlowPost(proxy, 1.2, "slow-during-cutover-1")
        slow1.start()
        # 等代理把 PENDING 行落库（请求已送达 A、回执未归）
        deadline = time.time() + 5
        pending_id = None
        while time.time() < deadline:
            _, s = http("GET", f"{proxy}/api/status")
            for row in s["writes"]:
                if row["status"] == "PENDING":
                    pending_id = row["id"]
                    inflight = s["drainage"]["A"]["inflight_now"]
                    check(inflight >= 1, "切流前 A 显示在途写入", f"inflight_now={inflight}")
                    break
            if pending_id:
                break
            time.sleep(0.1)
        check(bool(pending_id), "慢写入已进入 PENDING（已送达、待回执）")

        code, sw = http("POST", f"{proxy}/api/switch", {"target": "B"})
        check(code == 200 and sw["ok"] and sw["active"] == "B", "在途切流 A -> B 成功")
        _, s2 = http("GET", f"{proxy}/api/status")
        check(s2["drainage"]["A"]["draining"] is True, "旧节点 A 进入排空调（DRAINING）")

        slow1.join(timeout=10)
        check(not slow1.is_alive() and slow1.result and slow1.result[0] == 201,
              "在途写入未被切流打断，仍正常拿到回执")
        check(slow1.write_id == pending_id and slow1.result[1]["node"] == "A",
              "回执仍来自原送达节点 A", f"node={slow1.result[1].get('node') if slow1.result else '?'}")
        row = wait_status(proxy, pending_id, "ACKED")
        check(row["target_node"] == "A", "代理记录中该写入始终绑定 A")

        code, w_new = http("POST", f"{proxy}/api/writes?delay=0", {"payload": "after-switch"})
        check(code == 201 and w_new["node"] == "B", "切流后新请求立即走新节点 B",
              f"node={w_new.get('node')}")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        ids_a, ids_b = {r["id"] for r in rec_a["records"]}, {r["id"] for r in rec_b["records"]}
        check(pending_id in ids_a and pending_id not in ids_b, "慢写入只在 A，未被投到 B")
        check(w_new["id"] in ids_b and w_new["id"] not in ids_a, "新写入只在 B")

        print("\n== 场景 3：目标节点探测失败 -> 保持旧路由 ==")
        http("POST", f"{proxy}/api/nodes/A/fault", {"on": True})
        code, blocked = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 409 and blocked.get("blocked") and blocked.get("active") == "B",
              "A 故障时切向 A 被 409 拦截，现役仍为 B", blocked.get("reason", "")[:90])
        http("POST", f"{proxy}/api/nodes/A/fault", {"on": False})
        code, sw2 = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 200 and sw2["active"] == "A", "故障解除后切流成功，现役回到 A")

        print("\n== 场景 4：连续再次切流，各自在途写入都绑定原节点 ==")
        t1 = SlowPost(proxy, 1.2, "roundtrip-a")
        t1.start()
        time.sleep(0.35)
        code, _ = http("POST", f"{proxy}/api/switch", {"target": "B"})
        check(code == 200, "第二次切流 A -> B 成功且不打断在途写入")
        t1.join(timeout=10)
        check(t1.result and t1.result[1]["node"] == "A", "切走前送达 A 的写入仍由 A 回执")

        t2 = SlowPost(proxy, 1.2, "roundtrip-b")
        t2.start()
        time.sleep(0.35)
        code, _ = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 200, "第三次切流 B -> A 成功")
        t2.join(timeout=10)
        check(t2.result and t2.result[1]["node"] == "B",
              "切走前送达 B 的写入仍由 B 回执（再次切流不打断它）")

        print("\n== 场景 5：回执超时 -> 结果不确定，不补投，可事后向原节点核实 ==")
        active_before = http("GET", f"{proxy}/api/status")[1]["active"]
        t0 = time.time()
        code, tout = http("POST", f"{proxy}/api/writes?delay=5",
                          {"payload": "timeout-case"}, timeout=15)
        elapsed = time.time() - t0
        check(code == 504 and tout.get("uncertain") and
              tout["status"] == "TIMEOUT_UNCERTAIN",
              f"读超时返回 504 + TIMEOUT_UNCERTAIN（{elapsed:.1f}s 触发）")
        timeout_id = tout["id"]
        timeout_node = tout["node"]
        _, s3 = http("GET", f"{proxy}/api/status")
        row = next(w for w in s3["writes"] if w["id"] == timeout_id)
        check(row["status"] == "TIMEOUT_UNCERTAIN", "超时状态已落库，页面可见结果不确定")
        _, rec_other = http("GET", f"{proxy}/api/nodes/"
                            f"{'B' if timeout_node == 'A' else 'A'}/records")
        check(not any(r["id"] == timeout_id for r in rec_other["records"]),
              "超时后代理没有向另一节点补投")

        print("   等待后端慢回执结束后，向原送达节点查询核实……")
        time.sleep(4.2)
        code, ref = http("GET", f"{proxy}/api/writes/{timeout_id}/refresh")
        check(code == 200 and ref["write"]["status"] == "ACKED"
              and ref["write"]["target_node"] == timeout_node,
              "向原节点查询后确认：写入其实已落库并回执，状态就地更正，未换节点")

        print("\n== 场景 6：全量核对，两侧后端写入不相交、每条恰好一次 ==")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        ids_a, ids_b = {r["id"] for r in rec_a["records"]}, {r["id"] for r in rec_b["records"]}
        check(ids_a.isdisjoint(ids_b), "A/B 后端写入 ID 集合不相交（无任何重复投递）")
        _, s_all = http("GET", f"{proxy}/api/status")
        proxy_ids = {w["id"] for w in s_all["writes"]}
        check(proxy_ids == ids_a | ids_b,
              f"代理侧 {len(proxy_ids)} 条写入与后端记录一一对应",
              f"A={len(ids_a)} B={len(ids_b)}")

        # 直接打开后端 SQLite 核对持久化（后端各自保存写入的硬性要求）
        for node, db_path in (("A", work / "a.db"), ("B", work / "b.db")):
            con = sqlite3.connect(db_path)
            n = con.execute("SELECT COUNT(*) FROM writes").fetchone()[0]
            con.close()
            check(n == len(ids_a if node == "A" else ids_b),
                  f"后端 {node} 的 SQLite 持久化写入数 = {n}")

        print("\n== 场景 7：同号同内容并发重试——只有一次后端投递 ==")
        cid = "concurrent-" + uuid.uuid4().hex[:12]
        cpayload = "concurrent-same-payload"
        threads = [SlowPost(proxy, 1.5, cpayload, write_id=cid) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        codes = sorted(t.result[0] for t in threads)
        check(codes[0] == 200 and codes[-1] in (200, 201) and codes[0] != 500,
              "5 个并发同号同内容请求全部被正常受理（无 5xx）", f"codes={codes}")
        # 首次投递 201；其余重试只做只读查询，返回 200。
        n_created = sum(1 for t in threads if t.result[0] == 201)
        check(n_created == 1, f"恰好 1 个请求成为首次投递（201），其余 {5 - n_created} 个被抑制",
              f"201x{n_created}")
        row = find_write(proxy, cid)
        check(row["attempts"]["deliveries"] == 1 and row["attempts"]["retries"] >= 1
              and row["attempts"]["conflicts"] == 0,
              "attempts 留痕：投递 1 次、重试多次、冲突 0", str(row["attempts"]))
        recs_a, recs_b = node_record_ids(proxy, "A"), node_record_ids(proxy, "B")
        check(cid in recs_a ^ recs_b, "该 ID 只落在一个后端，没有二次投递")
        for node, ids in (("A", recs_a), ("B", recs_b)):
            if cid in ids:
                _, rec = http("GET", f"{proxy}/api/nodes/{node}/records")
                hits = [r for r in rec["records"] if r["id"] == cid]
                check(len(hits) == 1 and hits[0]["payload"] == cpayload,
                      f"后端 {node} 上该 ID 恰好一行且内容未被改写")

        print("\n== 场景 7b：慢回执在途期间并发同号重试——等待首次投递，仍只投递一次 ==")
        sid = "slowretry-" + uuid.uuid4().hex[:12]
        spayload = "slow-inflight-retry"
        # 读超时阈值是 2s，这里取 1.2s 保证首次投递能正常拿到 201（超时路径见场景 5）。
        slow_t = SlowPost(proxy, 1.2, spayload, write_id=sid)
        slow_t.start()
        deadline = time.time() + 5
        while time.time() < deadline:
            _, s = http("GET", f"{proxy}/api/status")
            if any(w["id"] == sid and w["in_flight"] for w in s["writes"]):
                break
            time.sleep(0.1)
        check(any(w["id"] == sid and w["in_flight"]
                  for w in http("GET", f"{proxy}/api/status")[1]["writes"]),
              "慢写入首次投递已登记为在途（in_flight=1）")
        # 首次投递尚未回执时，再并发打 3 个同号同内容重试（不等待后端慢回执）。
        retriers = [SlowPost(proxy, 0, spayload, write_id=sid, wait=False)
                    for _ in range(3)]
        for t in retriers:
            t.start()
        slow_t.join(timeout=30)
        for t in retriers:
            t.join(timeout=30)
        check(slow_t.result and slow_t.result[0] == 201,
              "首次投递正常 201 回执", str(slow_t.result[0] if slow_t.result else None))
        check(all(t.result and t.result[0] == 200 for t in retriers),
              "在途重试全部 200 返回（等待/查询，未二次投递）",
              str([t.result[0] if t.result else None for t in retriers]))
        srow = find_write(proxy, sid)
        check(srow["status"] == "ACKED" and srow["attempts"]["deliveries"] == 1
              and srow["attempts"]["retries"] == 3,
              "在途重试后仍 ACKED、仅 1 次投递、3 次重试留痕", str(srow["attempts"]))
        for node in ("A", "B"):
            _, rnode = http("GET", f"{proxy}/api/nodes/{node}/records")
            hits = [r for r in rnode["records"] if r["id"] == sid]
            if hits:
                check(len(hits) == 1, f"节点 {node} 上该慢写入仅一行（无重复 POST）")

        print("\n== 场景 8：切流后同号重试——仍只查首次绑定节点，不投到新现役节点 ==")
        rid = "postswitch-" + uuid.uuid4().hex[:12]
        rpayload = "retry-after-switch"
        code, r0 = http("POST", f"{proxy}/api/writes?delay=0",
                        {"id": rid, "payload": rpayload})
        bound = r0["node"]
        check(code == 201 and r0["status"] == "ACKED", "带显式 ID 的新写入成功",
              f"id={rid[:12]} node={bound}")
        other = "B" if bound == "A" else "A"
        http("POST", f"{proxy}/api/nodes/{other}/fault", {"on": False})
        code, sw = http("POST", f"{proxy}/api/switch", {"target": other})
        check(code == 200 and sw["active"] == other, f"切流到 {other} 以便验证重试不跟随新路由")
        before_other = node_record_ids(proxy, other)
        code, retry1 = http("POST", f"{proxy}/api/writes?wait=true",
                            {"id": rid, "payload": rpayload})
        check(code == 200 and retry1.get("re_delivered") is False
              and retry1["node"] == bound and retry1["status"] == "ACKED"
              and retry1.get("uncertain") is False,
              f"切流后重试只向原节点 {bound} 核实并返回当前结论，无二次投递")
        check(node_record_ids(proxy, other) == before_other,
              f"新现役节点 {other} 没有因重试收到任何写入")
        row = find_write(proxy, rid)
        check(row["target_node"] == bound and row["attempts"]["deliveries"] == 1
              and row["attempts"]["retries"] == 1,
              "原记录绑定节点不变，attempts 记 1 投递 1 重试", str(row["attempts"]))

        print("\n== 场景 9：同号异内容——409 冲突，原记录零改动、零投递 ==")
        before_row = find_write(proxy, rid)
        before_a, before_b = node_record_ids(proxy, "A"), node_record_ids(proxy, "B")
        code, conflict = http("POST", f"{proxy}/api/writes?delay=0",
                              {"id": rid, "payload": rpayload + "-TAMPERED"})
        check(code == 409 and conflict.get("conflict") is True
              and conflict.get("re_delivered") is False,
              "同号异内容返回 409 冲突且明确未投递")
        after_row = find_write(proxy, rid)
        check(after_row["payload"] == before_row["payload"]
              and after_row["status"] == before_row["status"]
              and after_row["target_node"] == before_row["target_node"]
              and after_row["created_at"] == before_row["created_at"],
              "冲突后原记录内容/状态/节点/创建时间全部未变")
        check(node_record_ids(proxy, "A") == before_a
              and node_record_ids(proxy, "B") == before_b,
              "冲突请求没有让任何后端多出记录")
        check(after_row["attempts"]["conflicts"] == 1
              and after_row["attempts"]["deliveries"] == 1,
              "attempts 记录 1 次冲突，投递数仍为 1", str(after_row["attempts"]))

        print("\n== 场景 10：原节点故障不可达——重试报不确定，绝不改投备用节点 ==")
        uid = "unreach-" + uuid.uuid4().hex[:12]
        upayload = "original-node-unreachable"
        code, u0 = http("POST", f"{proxy}/api/writes?delay=0",
                        {"id": uid, "payload": upayload})
        ubound = u0["node"]
        uother = "B" if ubound == "A" else "A"
        standby_before = node_record_ids(proxy, uother)
        http("POST", f"{proxy}/api/nodes/{ubound}/fault", {"on": True})
        code, rt = http("POST", f"{proxy}/api/writes?wait=false",
                        {"id": uid, "payload": upayload}, timeout=15)
        check(code == 200 and rt.get("uncertain") is True
              and rt["node"] == ubound and rt.get("re_delivered") is False,
              f"原节点 {ubound} 不可达时重试返回不确定且未改投", rt.get("note", "")[:80])
        check(node_record_ids(proxy, uother) == standby_before,
              f"备用节点 {uother} 未收到改投")
        urow = find_write(proxy, uid)
        check(urow["status"] == "ACKED" and urow["payload"] == upayload,
              "不确定重试不改动原记录状态与内容")
        http("POST", f"{proxy}/api/nodes/{ubound}/fault", {"on": False})
        code, rt2 = http("POST", f"{proxy}/api/writes?wait=true",
                         {"id": uid, "payload": upayload})
        check(code == 200 and rt2.get("uncertain") is False
              and rt2["status"] == "ACKED" and rt2["node"] == ubound,
              "原节点恢复后再次重试即可核实到 ACKED，全程只有一次投递")

        print("\n== 场景 11：代理有记录、原节点查无此 ID——不确定且不补投 ==")
        ghost_id = "ghost-" + uuid.uuid4().hex[:12]
        ghost_payload = "proxy-row-without-backend-row"
        code, g0 = http("POST", f"{proxy}/api/writes?delay=0",
                        {"id": ghost_id, "payload": ghost_payload})
        ghost_node = g0["node"]
        ghost_other = "B" if ghost_node == "A" else "A"
        ghost_db = work / ("a.db" if ghost_node == "A" else "b.db")
        con = sqlite3.connect(ghost_db)
        con.execute("DELETE FROM writes WHERE id = ?", (ghost_id,))
        con.commit()
        con.close()
        other_before = node_record_ids(proxy, ghost_other)
        code, gr = http("POST", f"{proxy}/api/writes?wait=false",
                        {"id": ghost_id, "payload": ghost_payload})
        check(code == 200 and gr.get("uncertain") is True
              and gr["node"] == ghost_node and gr.get("re_delivered") is False,
              "原节点查无记录时重试返回不确定，不补投、不改投", gr.get("note", "")[:80])
        check(node_record_ids(proxy, ghost_other) == other_before,
              "查无记录不会触发向备用节点的投递")
        grow = find_write(proxy, ghost_id)
        check(grow["target_node"] == ghost_node and grow["payload"] == ghost_payload,
              "代理侧原记录保持不变，等待后续核实")

        print("\n== 场景 12：代理重启后重试——绑定关系与语义持久化 ==")
        proxy_proc = procs[2]
        proxy_proc.terminate()
        try:
            proxy_proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proxy_proc.kill()
            proxy_proc.wait(timeout=5)
        procs.remove(proxy_proc)
        time.sleep(0.5)
        spawn("proxy-restarted", ROOT / "proxy", {
            "NODE_A_NAME": "A", "NODE_B_NAME": "B",
            "BACKEND_A_URL": f"http://127.0.0.1:{ports['A']}",
            "BACKEND_B_URL": f"http://127.0.0.1:{ports['B']}",
            "PROXY_DB": str(work / "proxy.db"),
            "WRITER_READ_TIMEOUT": "2", "CONNECT_TIMEOUT": "2", "PROBE_TIMEOUT": "2",
        }, ports["proxy"])
        check(wait_ready(proxy + "/"), "代理重启完成并复用原 SQLite 状态")
        _, s_boot = http("GET", f"{proxy}/api/status")
        boot_events = [e["kind"] for e in s_boot["events"]]
        check("BOOT" in boot_events, "重启事件已落日志")
        active_now = s_boot["active"]
        target_now = "B" if active_now == "A" else "A"
        code, _ = http("POST", f"{proxy}/api/switch", {"target": target_now})
        check(code == 200, f"重启后切流到 {target_now} 仍可用")
        pre_target = node_record_ids(proxy, target_now)
        code, post = http("POST", f"{proxy}/api/writes?wait=true",
                          {"id": rid, "payload": rpayload})
        check(code == 200 and post["node"] == bound and post["status"] == "ACKED"
              and post.get("re_delivered") is False and post.get("uncertain") is False,
              f"重启+切流后重试仍沿首次绑定节点 {bound} 核实，后端未收到第二次投递")
        check(node_record_ids(proxy, target_now) == pre_target,
              "重启后的重试没有向新现役节点投递")
        code, fresh = http("POST", f"{proxy}/api/writes?delay=0",
                           {"payload": "after-restart-fresh"})
        check(code == 201 and fresh["node"] == target_now,
              "重启后正常新写入继续可用并走现役节点", f"node={fresh.get('node')}")
        row = find_write(proxy, rid)
        check(row["attempts"]["deliveries"] == 1,
              "跨重启累计投递数仍为 1（attempts 持久化）", str(row["attempts"]))

        print("\n== 场景 13：重启后再次全量核对——不相交、每条恰好一次 ==")
        ids_a, ids_b = node_record_ids(proxy, "A"), node_record_ids(proxy, "B")
        check(ids_a.isdisjoint(ids_b), "重启后 A/B 后端写入 ID 集合仍不相交")
        _, s_all = http("GET", f"{proxy}/api/status")
        proxy_ids = {w["id"] for w in s_all["writes"]}
        # ghost 行在场景 11 被刻意从原节点 SQLite 删除，代理侧仍保留、等待核实。
        check(proxy_ids - {ghost_id} == ids_a | ids_b,
              f"代理侧 {len(proxy_ids)} 条写入（除刻意删除的 ghost）与后端一一对应",
              f"A={len(ids_a)} B={len(ids_b)}")
        total_deliveries = sum(w["attempts"]["deliveries"] for w in s_all["writes"])
        check(total_deliveries == len(proxy_ids),
              f"全量 attempts 投递总数 {total_deliveries} = 写入数 {len(proxy_ids)}（无任何重复投递）")
        for node, db_path in (("A", work / "a.db"), ("B", work / "b.db")):
            con = sqlite3.connect(db_path)
            n = con.execute("SELECT COUNT(*) FROM writes").fetchone()[0]
            con.close()
            check(n == len(ids_a if node == "A" else ids_b),
                  f"重启后后端 {node} SQLite 写入数 = {n}")

    finally:
        print("\n== 清理进程 ==")
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=8)
            except subprocess.TimeoutExpired:
                p.kill()
        if FAIL:
            print(f"\n日志保留在: {logs}")
        else:
            shutil.rmtree(work, ignore_errors=True)

    print(f"\n================ 自测结果: {PASS} 通过 / {FAIL} 失败 ================")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
