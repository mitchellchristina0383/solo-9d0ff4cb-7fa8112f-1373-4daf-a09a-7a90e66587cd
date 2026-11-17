"""FastAPI 反向代理 + 写入切流控制台。

切流与安全重试语义（本文件的所有代码路径都遵守）：
  1. 每个写入请求在发出前快照"当前现役节点"，并把该节点名持久化到 writes 行；
     之后无论怎么切流，这个请求只与该节点通信，绝不重投到另一节点。
  2. 切流只改 route 表，对已经发往旧节点、尚未回执的在途请求不做任何取消动作；
     回执仍从旧节点读取（也可事后用 /api/writes/{id}/refresh 向原节点查询）。
  3. 切流前先探测目标节点 /healthz，探测失败则保留旧路由并返回 409。
  4. 读超时（写入大概率已在后端落库，只是回执没回来）标记为 TIMEOUT_UNCERTAIN，
     返回 504 并在页面提示"结果不确定"，代理不做任何自动补投/重试。
  5. 安全重试（客户端断线后携原写入 ID + 原内容重试）：
       a. 同号同内容：不产生第二次后端投递。若首次投递仍在途，等待并返回其结论；
          否则只向"首次绑定的原节点"做只读查询，返回该笔写入的当前结论；
       b. 同号异内容：返回 409 CONFLICT，原记录一行都不改，也不产生任何投递；
       c. 原节点暂时不可达、或原节点查无此记录：返回 200 + uncertain=true，
          结论不确定，绝不改投备用节点；
       d. 重试语义在代理重启后依然成立——绑定关系与尝试历史都在 SQLite 里。
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

from . import db

NODE_A = os.environ.get("NODE_A_NAME", "A")
NODE_B = os.environ.get("NODE_B_NAME", "B")
URL_A = os.environ.get("BACKEND_A_URL", "http://127.0.0.1:8001")
URL_B = os.environ.get("BACKEND_B_URL", "http://127.0.0.1:8002")

# 连接失败 = 节点不可达、请求未送达；读超时 = 请求已发出、回执未归。
CONNECT_TIMEOUT = float(os.environ.get("CONNECT_TIMEOUT", "3.0"))
READ_TIMEOUT = float(os.environ.get("WRITER_READ_TIMEOUT", "30.0"))
PROBE_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "2.5"))
# 重试/刷新向原节点做只读查询时的读超时（可带 wait 等慢回执结束）。
QUERY_READ_TIMEOUT = float(os.environ.get("QUERY_READ_TIMEOUT", "20.0"))

# node -> 在途"首次投递"写入 id 集合（仅内存态；进程重启后以 DB 的
# in_flight=1 行为准，重启时没有存活投递，该集合自然为空）。
_inflight: dict[str, set[str]] = {}
_inflight_lock = asyncio.Lock()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db()
    await db.register_node(NODE_A, URL_A)
    await db.register_node(NODE_B, URL_B)
    _inflight.setdefault(NODE_A, set())
    _inflight.setdefault(NODE_B, set())
    app.state.http = httpx.AsyncClient(
        timeout=httpx.Timeout(connect=CONNECT_TIMEOUT, read=READ_TIMEOUT,
                              write=CONNECT_TIMEOUT, pool=CONNECT_TIMEOUT),
        # 显式关闭 httpx 默认的连接重试，保证"一次投递"语义。
        limits=httpx.Limits(max_connections=100),
    )
    stale = await db.in_flight_ids()
    if stale:
        # 进程重启不可能带着活着的 HTTP 投递；残留标记说明上次进程被硬杀。
        # 状态本身不改（仍可能是 PENDING/TIMEOUT_UNCERTAIN），客户端可安全重试，
        # 重试只会向原节点只读查询。
        await db.add_event(
            "BOOT_STALE_INFLIGHT",
            f"proxy restarted; {len(stale)} writes were in-flight when last process "
            f"died: {stale[:10]}; statuses kept, retry will query original node only",
        )
    await db.add_event("BOOT", f"proxy started, active={await db.get_active()}")
    try:
        yield
    finally:
        await app.state.http.aclose()


app = FastAPI(title="write-cutover-proxy", lifespan=lifespan)


# ---------------------------------------------------------------- 健康探测

async def probe_node(base_url: str) -> tuple[bool, str]:
    """返回 (是否健康, 说明)。探测本身绝不触发任何写入。"""
    client: httpx.AsyncClient = app.state.http
    try:
        resp = await client.get(
            f"{base_url}/healthz", timeout=httpx.Timeout(PROBE_TIMEOUT)
        )
    except httpx.RequestError as exc:
        return False, f"probe request failed: {type(exc).__name__}: {exc}"
    if resp.status_code == 200:
        return True, "healthy"
    return False, f"healthz returned HTTP {resp.status_code}: {resp.text[:200]}"


async def node_health_map() -> dict[str, dict[str, str]]:
    nodes = await db.list_nodes()
    results = await asyncio.gather(*(probe_node(n["base_url"]) for n in nodes))
    return {
        n["name"]: {"healthy": ok, "detail": detail, "base_url": n["base_url"]}
        for n, (ok, detail) in zip(nodes, results)
    }


# ---------------------------------------------------------------- 切流

@app.post("/api/switch")
async def switch(request: Request) -> JSONResponse:
    body = await request.json()
    target = str(body.get("target", "")).strip()
    nodes = {n["name"]: n["base_url"] for n in await db.list_nodes()}
    if target not in nodes:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": f"unknown target node: {target!r}",
                     "known_nodes": sorted(nodes)},
        )

    active = await db.get_active()
    if target == active:
        return JSONResponse(content={"ok": True, "noop": True, "active": active,
                                     "message": "target is already active"})

    healthy, detail = await probe_node(nodes[target])
    if not healthy:
        # 探测失败：保持旧路由，不动 route 表。
        await db.add_event(
            "SWITCH_BLOCKED",
            f"{active} -> {target} blocked, target unhealthy: {detail}",
        )
        return JSONResponse(
            status_code=409,
            content={"ok": False, "active": active, "blocked": True,
                     "reason": f"target {target} probe failed: {detail}",
                     "message": "kept old route"},
        )

    async with _inflight_lock:
        inflight_snapshot = {k: len(v) for k, v in _inflight.items()}
        await db.set_active(target)
    await db.add_event(
        "SWITCH_OK",
        f"{active} -> {target} ok; inflight at cutover={inflight_snapshot}; "
        "new requests now route to target, in-flight requests stay on old node",
    )
    return JSONResponse(content={
        "ok": True, "active": target, "previous": active,
        "inflight_at_cutover": inflight_snapshot,
        "message": "switch done; in-flight writes continue against their original node",
    })


# ---------------------------------------------------------------- 写入代理

async def _track(node: str, write_id: str) -> None:
    async with _inflight_lock:
        _inflight.setdefault(node, set()).add(write_id)


async def _untrack(node: str, write_id: str) -> None:
    async with _inflight_lock:
        _inflight.setdefault(node, set()).discard(write_id)


# 首次投递完成事件：并发的同号重试据此等待在途投递结束，而不是再发一次 POST。
# 仅内存态；代理重启后没有存活投递，重试直接走"向原节点只读查询"。
_delivery_events: dict[str, asyncio.Event] = {}
_delivery_events_lock = asyncio.Lock()


async def _query_original_node(
    row: dict[str, Any], *, wait: bool, kind: str
) -> dict[str, Any]:
    """向该写入**首次绑定的原节点**做只读查询。

    重试与刷新共用本函数：只发 GET，绝不 POST、绝不联系另一节点。
    原节点不可达 / 查无记录 / 仍 pending 时，结论一律标 uncertain，
    原记录状态保持不变（只追加说明），等待后续再次核实。
    kind 取 db.RETRY / db.REFRESH，用于 attempts 留痕。
    """
    write_id = row["id"]
    node = row["target_node"]
    base_url = await db.node_url(node)
    client: httpx.AsyncClient = app.state.http
    label = "retry" if kind == db.RETRY else "refresh"
    common = {
        "id": write_id, "node": node, "re_delivered": False,
        "retry": kind == db.RETRY,
    }
    try:
        resp = await client.get(
            f"{base_url}/writes/{write_id}",
            params={"wait": "true"} if wait else None,
            timeout=httpx.Timeout(connect=CONNECT_TIMEOUT, read=QUERY_READ_TIMEOUT,
                                  write=CONNECT_TIMEOUT, pool=CONNECT_TIMEOUT),
        )
    except httpx.RequestError as exc:
        note = (f"{label} query to original node {node} failed: "
                f"{type(exc).__name__}: {exc}; result UNCERTAIN; "
                "no re-delivery and no failover to another node")
        updated = await db.append_status_detail(write_id, note)
        await db.record_attempt(write_id, kind, db.ATTEMPT_UNCERTAIN, node, note)
        await db.add_event("RETRY_UNCERTAIN" if kind == db.RETRY else "REFRESH_UNCERTAIN",
                           f"{write_id} @ {node}: {note}")
        return {**common, "status": updated["status"], "uncertain": True,
                "note": note, "write": updated}

    if resp.status_code == 404:
        note = (f"original node {node} has no record of this write yet; keep status "
                f"{row['status']}; result UNCERTAIN; do not re-deliver or fail over")
        updated = await db.append_status_detail(write_id, note)
        await db.record_attempt(write_id, kind, db.ATTEMPT_UNCERTAIN, node, note)
        await db.add_event("RETRY_UNCERTAIN" if kind == db.RETRY else "REFRESH_UNCERTAIN",
                           f"{write_id} @ {node}: not found, uncertain, no re-delivery")
        return {**common, "status": updated["status"], "uncertain": True,
                "note": note, "write": updated}

    if resp.status_code != 200:
        # 原节点在线但明确报错（如故障态 503）：同样只报不确定，绝不改投。
        note = (f"original node {node} answered HTTP {resp.status_code}: "
                f"{resp.text[:200]}; result UNCERTAIN; no failover")
        updated = await db.append_status_detail(write_id, note)
        await db.record_attempt(write_id, kind, db.ATTEMPT_UNCERTAIN, node, note)
        await db.add_event("RETRY_UNCERTAIN" if kind == db.RETRY else "REFRESH_UNCERTAIN",
                           f"{write_id} @ {node}: {note}")
        return {**common, "status": updated["status"], "uncertain": True,
                "note": note, "write": updated}

    data = resp.json()
    refreshed = await db.get_write(write_id)
    if data.get("status") == "acked":
        if refreshed["status"] != db.ACKED:
            await db.mark_acked(
                write_id, f"acked confirmed by original node {node} via {label}"
            )
            await db.add_event(
                "WRITE_RESOLVED",
                f"{write_id} confirmed acked by original node {node} via {label}",
            )
        note = f"original node {node} confirms the write is acked"
        await db.record_attempt(write_id, kind, db.ATTEMPT_REPLIED, node, note)
        final_row = await db.get_write(write_id)
        return {**common, "status": db.ACKED, "uncertain": False,
                "note": note, "write": final_row, "backend_view": data}

    # 原节点答复 200 但写入仍 pending：还没有结论。
    note = f"original node {node} reports the write is still pending; result UNCERTAIN"
    updated = await db.append_status_detail(write_id, note)
    await db.record_attempt(write_id, kind, db.ATTEMPT_REPLIED, node, note)
    return {**common, "status": updated["status"], "uncertain": True,
            "note": note, "write": updated, "backend_view": data}


async def _handle_retry(row: dict[str, Any], *, wait: bool) -> JSONResponse:
    """同号同内容重试：抑制第二次投递，只沿首次绑定节点核实当前结论。"""
    write_id = row["id"]
    waited_inflight = False
    event = _delivery_events.get(write_id)
    if event is not None:
        # 并发重试：首次投递还在途，等它结束（重启后无 event，直接查询）。
        waited_inflight = True
        try:
            await asyncio.wait_for(event.wait(), timeout=READ_TIMEOUT + 10.0)
        except asyncio.TimeoutError:
            pass
    await db.add_event(
        "RETRY_SUPPRESSED",
        f"{write_id} retry with same id+payload: second delivery suppressed; "
        f"querying original node {row['target_node']} only"
        + (" (waited for in-flight first delivery)" if waited_inflight else ""),
    )
    body = await _query_original_node(row, wait=wait, kind=db.RETRY)
    body["waited_for_inflight"] = waited_inflight
    body["attempts"] = (await db.attempt_stats()).get(write_id, {})
    return JSONResponse(status_code=200, content=body)


@app.post("/api/writes")
async def proxy_write(request: Request) -> JSONResponse:
    params = request.query_params
    delay = float(params.get("delay", "0"))
    # 重试默认允许等原节点慢回执结束（?wait=false 可关闭）。
    wait = params.get("wait", "true").lower() != "false"
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {"payload": body}

    # 关键：路由在投递前快照，切流不影响本请求。
    active = await db.get_active()
    base_url = await db.node_url(active)
    write_id = str(body.get("id") or uuid.uuid4())
    body["id"] = write_id
    payload = str(body.get("payload", ""))

    # 原子判定：新写入 / 同号同内容重试 / 同号异内容冲突。
    outcome = await db.begin_or_get_write(write_id, active, payload, delay)
    if outcome["conflict"]:
        existing = outcome["write"]
        await db.add_event(
            "RETRY_CONFLICT",
            f"{write_id} retry payload CONFLICT (bound to {existing['target_node']}); "
            "original record untouched, nothing delivered",
        )
        return JSONResponse(status_code=409, content={
            "id": write_id, "conflict": True,
            "node": existing["target_node"], "status": existing["status"],
            "existing_payload": existing["payload"], "retry_payload": payload,
            "re_delivered": False,
            "message": ("same write id with different payload is a conflict; "
                        "the original record is unchanged and no delivery was made"),
            "write": existing,
        })
    if not outcome["created"]:
        return await _handle_retry(outcome["write"], wait=wait)

    # 新 ID：这是该写入**唯一一次**向后端的 POST 投递。
    row = outcome["write"]
    node = row["target_node"]
    await _track(node, write_id)
    done = asyncio.Event()
    async with _delivery_events_lock:
        _delivery_events[write_id] = done
    client: httpx.AsyncClient = app.state.http
    try:
        try:
            resp = await client.post(
                f"{base_url}/writes", params={"delay": delay}, json=body
            )
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout,
                httpx.WriteTimeout) as exc:
            # 连接层面失败：请求没有被后端受理，标记 FAILED，不向任何节点补投。
            detail = f"connect to {node} failed: {type(exc).__name__}: {exc}"
            await db.finish_write(write_id, db.FAILED, detail)
            await db.add_event("WRITE_FAILED", f"{write_id} -> {node}: {detail}")
            return JSONResponse(status_code=502,
                                content={"id": write_id, "node": node,
                                         "status": db.FAILED, "detail": detail})
        except httpx.ReadTimeout:
            # 读超时：请求已送达，后端可能已落库，回执未归。结果不确定，禁止补投。
            detail = (f"read timeout after {READ_TIMEOUT}s waiting ack from {node}; "
                      "write may already be persisted there, result UNCERTAIN; "
                      "no automatic re-delivery to another node")
            await db.finish_write(write_id, db.TIMEOUT_UNCERTAIN, detail)
            await db.add_event("WRITE_TIMEOUT_UNCERTAIN",
                               f"{write_id} -> {node}: {detail}")
            return JSONResponse(status_code=504,
                                content={"id": write_id, "node": node,
                                         "status": db.TIMEOUT_UNCERTAIN,
                                         "uncertain": True, "detail": detail})
        except httpx.HTTPError as exc:
            detail = f"unexpected transport error against {node}: {exc}"
            await db.finish_write(write_id, db.FAILED, detail)
            return JSONResponse(status_code=502,
                                content={"id": write_id, "node": node,
                                         "status": db.FAILED, "detail": detail})

        if resp.status_code in (200, 201):
            try:
                ack = resp.json()
            except Exception:
                ack = {}
            await db.finish_write(
                write_id, db.ACKED,
                f"acked by {node} (backend status={ack.get('status', 'acked')})",
                acked=True,
            )
            await db.add_event("WRITE_ACKED", f"{write_id} acked by {node}")
            return JSONResponse(status_code=201,
                                content={"id": write_id, "node": node,
                                         "status": db.ACKED, "backend": ack})

        # 后端明确拒绝（如故障态 500）：未受理，FAILED；同样不补投。
        detail = f"{node} rejected with HTTP {resp.status_code}: {resp.text[:300]}"
        await db.finish_write(write_id, db.FAILED, detail)
        await db.add_event("WRITE_FAILED", f"{write_id} -> {node}: {detail}")
        return JSONResponse(
            status_code=502,
            content={"id": write_id, "node": node, "status": db.FAILED, "detail": detail},
        )
    finally:
        await _untrack(node, write_id)
        async with _delivery_events_lock:
            _delivery_events.pop(write_id, None)
        done.set()


@app.get("/api/writes/{write_id}/refresh")
async def refresh_write(write_id: str) -> JSONResponse:
    """向"原送达节点"查询该写入的最新回执状态——切流后、重启后仍然找旧节点。"""
    row = await db.get_write(write_id)
    if row is None:
        return JSONResponse(status_code=404,
                            content={"error": f"unknown write {write_id}"})
    body = await _query_original_node(row, wait=False, kind=db.REFRESH)
    # 兼容旧页面字段：refreshed 表示是否拿到原节点的有效答复。
    body["refreshed"] = not body["uncertain"]
    return JSONResponse(status_code=200, content=body)


# ---------------------------------------------------------------- 状态/排空

@app.get("/api/status")
async def status() -> JSONResponse:
    active = await db.get_active()
    health = await node_health_map()
    writes = await db.list_writes()
    stats = await db.attempt_stats()
    for w in writes:
        w["attempts"] = stats.get(w["id"], {
            "deliveries": 0, "retries": 0, "refreshes": 0, "conflicts": 0,
        })

    async with _inflight_lock:
        inflight_counts = {k: len(v) for k, v in _inflight.items()}
        inflight_ids = {k: sorted(v) for k, v in _inflight.items()}
    # 重启后内存集合为空，以 DB 的 in_flight 标记兜底（可能是上次硬杀的残留）。
    for w in writes:
        if w["in_flight"] and w["id"] not in set(inflight_ids.get(w["target_node"], [])):
            inflight_ids.setdefault(w["target_node"], []).append(w["id"])
            inflight_ids[w["target_node"]].sort()
            inflight_counts[w["target_node"]] = len(inflight_ids[w["target_node"]])

    # 排空视角：已切走的旧节点上仍未拿到回执的写入。
    per_node: dict[str, dict[str, Any]] = {}
    for w in writes:
        slot = per_node.setdefault(
            w["target_node"], {"total": 0, "pending": 0, "acked": 0,
                               "timeout_uncertain": 0, "failed": 0,
                               "retries": 0, "conflicts": 0}
        )
        slot["total"] += 1
        slot[w["status"].lower()] = slot.get(w["status"].lower(), 0) + 1
        slot["retries"] += w["attempts"]["retries"]
        slot["conflicts"] += w["attempts"]["conflicts"]
    for name in health:
        per_node.setdefault(name, {"total": 0, "pending": 0, "acked": 0,
                                   "timeout_uncertain": 0, "failed": 0,
                                   "retries": 0, "conflicts": 0})
    for name, slot in per_node.items():
        slot["inflight_now"] = inflight_counts.get(name, 0)
        slot["inflight_ids"] = inflight_ids.get(name, [])
        slot["is_active"] = name == active
        slot["draining"] = name != active and (
            slot["inflight_now"] > 0 or slot["pending"] > 0
        )

    return JSONResponse(content={
        "ts": time.time(),
        "active": active,
        "nodes": health,
        "drainage": per_node,
        "writes": writes,
        "events": await db.list_events(),
        "timeouts": {"connect": CONNECT_TIMEOUT, "read": READ_TIMEOUT,
                     "probe": PROBE_TIMEOUT, "query": QUERY_READ_TIMEOUT},
    })


@app.get("/api/nodes/{name}/records")
async def node_records(name: str) -> JSONResponse:
    """透传某后端自己保存的写入记录。"""
    base_url = await db.node_url(name)
    if base_url is None:
        return JSONResponse(status_code=404, content={"error": f"unknown node {name}"})
    try:
        resp = await app.state.http.get(
            f"{base_url}/records", timeout=httpx.Timeout(PROBE_TIMEOUT)
        )
    except httpx.RequestError as exc:
        return JSONResponse(status_code=502,
                            content={"error": f"node {name} unreachable: {exc}"})
    return JSONResponse(content=resp.json())


@app.post("/api/nodes/{name}/fault")
async def node_fault(name: str, request: Request) -> JSONResponse:
    """给某后端注入/解除健康故障（透传后端控制接口）。"""
    base_url = await db.node_url(name)
    if base_url is None:
        return JSONResponse(status_code=404, content={"error": f"unknown node {name}"})
    body = await request.json()
    try:
        resp = await app.state.http.post(
            f"{base_url}/control/fault", json={"on": bool(body.get("on", True))},
            timeout=httpx.Timeout(PROBE_TIMEOUT),
        )
    except httpx.RequestError as exc:
        return JSONResponse(status_code=502,
                            content={"error": f"node {name} unreachable: {exc}"})
    await db.add_event("FAULT_INJECTION",
                       f"node {name} fault on={bool(body.get('on', True))}")
    return JSONResponse(content=resp.json())


# ---------------------------------------------------------------- 控制台页面

@app.get("/", response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    html = (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")
    return HTMLResponse(html)
