"""代理侧 SQLite 持久层：节点配置、当前路由、写入记录、尝试历史、切流事件。

写入记录一旦落库即代表"已送达某节点"，回执状态只能就地向原节点查询更新，
任何状态机代码都不会把同一条写入再投到另一个节点。同号重试的"存在即判定"
在本模块的单写锁内原子完成，attempts 表留痕每一次首次投递/重试/刷新/冲突。
"""
from __future__ import annotations

import os
import time
from typing import Any

import aiosqlite

DB_PATH = os.environ.get("PROXY_DB", "/data/proxy.db")

# 写入状态机
PENDING = "PENDING"        # 已送达/投递中，尚未拿到回执
ACKED = "ACKED"            # 已拿到原节点成功回执
TIMEOUT_UNCERTAIN = "TIMEOUT_UNCERTAIN"  # 写入已落库但回执超时，结果不确定
FAILED = "FAILED"          # 节点明确拒绝（连接前失败/5xx），未送达

# 尝试类型（attempts.kind）：一次写入至多有一条 INITIAL_DELIVERY，其余都是只读重试。
INITIAL_DELIVERY = "INITIAL_DELIVERY"
RETRY = "RETRY"
REFRESH = "REFRESH"
RETRY_CONFLICT = "RETRY_CONFLICT"
# 尝试结果（attempts.result）
ATTEMPT_IN_FLIGHT = "IN_FLIGHT"    # 首次投递已发出，回执未归
ATTEMPT_REPLIED = "REPLIED"        # 与原节点通信拿到了答复（含 404/503 等）
ATTEMPT_UNCERTAIN = "UNCERTAIN"    # 原节点不可达/查无记录，结论不确定
ATTEMPT_BLOCKED = "CONFLICT"       # 同号异内容，被拦截，未投递

# 单连接 + 单写锁，SQLite 写入串行化，避免 lock 竞争。
_conn: aiosqlite.Connection | None = None
_write_lock: Any = None


async def init_db() -> None:
    global _conn, _write_lock
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    _conn = aiosqlite.connect(DB_PATH)
    await _conn
    _write_lock = asyncio_lock()
    _conn.row_factory = aiosqlite.Row
    await _conn.execute("PRAGMA journal_mode=WAL")
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS nodes (
            name        TEXT PRIMARY KEY,
            base_url    TEXT NOT NULL,
            created_at  REAL NOT NULL
        )
        """
    )
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS route (
            id          INTEGER PRIMARY KEY CHECK (id = 1),
            active      TEXT NOT NULL,
            updated_at  REAL NOT NULL
        )
        """
    )
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS writes (
            id           TEXT PRIMARY KEY,
            target_node  TEXT NOT NULL,
            payload      TEXT NOT NULL,
            delay_hint   REAL NOT NULL DEFAULT 0,
            status       TEXT NOT NULL,
            status_detail TEXT NOT NULL DEFAULT '',
            in_flight    INTEGER NOT NULL DEFAULT 0,
            created_at   REAL NOT NULL,
            updated_at   REAL NOT NULL,
            acked_at     REAL
        )
        """
    )
    # 旧库升级：in_flight 是后加列；重启后没有任何在途投递，旧行一律置 0。
    cur = await _conn.execute("PRAGMA table_info(writes)")
    cols = {r[1] for r in await cur.fetchall()}
    if "in_flight" not in cols:
        await _conn.execute("ALTER TABLE writes ADD COLUMN in_flight INTEGER NOT NULL DEFAULT 0")
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS attempts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            write_id    TEXT NOT NULL,
            kind        TEXT NOT NULL,
            result      TEXT NOT NULL,
            node        TEXT NOT NULL,
            detail      TEXT NOT NULL DEFAULT '',
            ts          REAL NOT NULL
        )
        """
    )
    await _conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_attempts_write ON attempts(write_id, id)"
    )
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          REAL NOT NULL,
            kind        TEXT NOT NULL,
            message     TEXT NOT NULL
        )
        """
    )
    await _conn.commit()


def asyncio_lock() -> Any:
    import asyncio

    return asyncio.Lock()


async def register_node(name: str, base_url: str) -> None:
    async with _write_lock:
        await _conn.execute(
            "INSERT OR IGNORE INTO nodes (name, base_url, created_at) VALUES (?, ?, ?)",
            (name, base_url, time.time()),
        )
        # 已存在则更新地址（compose 重建容器场景）。
        await _conn.execute(
            "UPDATE nodes SET base_url = ? WHERE name = ?", (base_url, name)
        )
        cur = await _conn.execute("SELECT COUNT(*) AS c FROM route")
        row = await cur.fetchone()
        if row["c"] == 0:
            await _conn.execute(
                "INSERT INTO route (id, active, updated_at) VALUES (1, ?, ?)",
                (name, time.time()),
            )
        await _conn.commit()


async def get_active() -> str:
    cur = await _conn.execute("SELECT active FROM route WHERE id = 1")
    row = await cur.fetchone()
    return row["active"]


async def set_active(name: str) -> None:
    async with _write_lock:
        await _conn.execute(
            "UPDATE route SET active = ?, updated_at = ? WHERE id = 1",
            (name, time.time()),
        )
        await _conn.commit()


async def list_nodes() -> list[dict[str, Any]]:
    cur = await _conn.execute("SELECT name, base_url FROM nodes ORDER BY name")
    rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def node_url(name: str) -> str | None:
    cur = await _conn.execute("SELECT base_url FROM nodes WHERE name = ?", (name,))
    row = await cur.fetchone()
    return row["base_url"] if row else None


async def begin_or_get_write(
    write_id: str, target_node: str, payload: str, delay_hint: float
) -> dict[str, Any]:
    """原子登记一次写入请求，返回 (write_row, created, conflict) 三态结果。

    在单写锁 + 单连接内完成"存在即判定"，因此同号并发重试只有一个请求能
    创建记录并向后端发起首次投递；后续同号请求永远走重试分支，后端不会
    收到第二次投递。

      * created=True          ：新 ID，登记 PENDING/in_flight=1，记录首次投递；
      * created=False, 同内容 ：返回既有行，不做任何改动（含不改 in_flight）；
      * conflict=True, 异内容 ：既有行完全不动，仅记一条 RETRY_CONFLICT 尝试。
    """
    async with _write_lock:
        cur = await _conn.execute("SELECT * FROM writes WHERE id = ?", (write_id,))
        existing = await cur.fetchone()
        now = time.time()
        if existing is not None:
            row = dict(existing)
            if row["payload"] != payload:
                # 同号异内容：冲突。原记录一行一字段都不改。
                await _conn.execute(
                    "INSERT INTO attempts (write_id, kind, result, node, detail, ts) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (write_id, RETRY_CONFLICT, ATTEMPT_BLOCKED, row["target_node"],
                     f"payload mismatch: existing={row['payload'][:200]!r} "
                     f"retry={payload[:200]!r}; original record untouched", now),
                )
                await _conn.commit()
                return {"write": row, "created": False, "conflict": True}
            return {"write": row, "created": False, "conflict": False}

        await _conn.execute(
            "INSERT INTO writes (id, target_node, payload, delay_hint, status, "
            "status_detail, in_flight, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)",
            (write_id, target_node, payload, delay_hint, PENDING,
             "delivered, awaiting ack", now, now),
        )
        await _conn.execute(
            "INSERT INTO attempts (write_id, kind, result, node, detail, ts) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (write_id, INITIAL_DELIVERY, ATTEMPT_IN_FLIGHT, target_node,
             f"first and only delivery, routed to {target_node}", now),
        )
        await _conn.commit()
        cur = await _conn.execute("SELECT * FROM writes WHERE id = ?", (write_id,))
        return {"write": dict(await cur.fetchone()), "created": True, "conflict": False}


async def finish_write(write_id: str, status: str, detail: str, acked: bool = False) -> None:
    """首次投递结束：更新终态并清除在途标记。只作用于首次投递，重试路径永不调用。"""
    async with _write_lock:
        if acked:
            await _conn.execute(
                "UPDATE writes SET status = ?, status_detail = ?, updated_at = ?, "
                "acked_at = ?, in_flight = 0 WHERE id = ?",
                (status, detail, time.time(), time.time(), write_id),
            )
        else:
            await _conn.execute(
                "UPDATE writes SET status = ?, status_detail = ?, updated_at = ?, "
                "in_flight = 0 WHERE id = ?",
                (status, detail, time.time(), write_id),
            )
        await _conn.commit()


async def append_status_detail(write_id: str, extra: str) -> dict[str, Any]:
    """只追加说明文字、不改状态（重试查不到/不可达时使用）。"""
    async with _write_lock:
        await _conn.execute(
            "UPDATE writes SET status_detail = status_detail || ? WHERE id = ?",
            (f" | {extra}", write_id),
        )
        await _conn.commit()
        cur = await _conn.execute("SELECT * FROM writes WHERE id = ?", (write_id,))
        return dict(await cur.fetchone())


async def mark_acked(write_id: str, detail: str) -> None:
    """向原节点核实后就地确认 ACKED（刷新与重试路径共用）。"""
    async with _write_lock:
        await _conn.execute(
            "UPDATE writes SET status = ?, status_detail = ?, updated_at = ?, "
            "acked_at = COALESCE(acked_at, ?) WHERE id = ?",
            (ACKED, detail, time.time(), time.time(), write_id),
        )
        await _conn.commit()


async def record_attempt(
    write_id: str, kind: str, result: str, node: str, detail: str
) -> None:
    async with _write_lock:
        await _conn.execute(
            "INSERT INTO attempts (write_id, kind, result, node, detail, ts) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (write_id, kind, result, node, detail, time.time()),
        )
        await _conn.commit()


async def attempt_stats() -> dict[str, dict[str, int]]:
    """每条写入的投递/重试/冲突计数，供控制台与自测核对。"""
    cur = await _conn.execute(
        "SELECT write_id, kind, COUNT(*) AS c FROM attempts GROUP BY write_id, kind"
    )
    stats: dict[str, dict[str, int]] = {}
    for r in await cur.fetchall():
        slot = stats.setdefault(r["write_id"], {
            "deliveries": 0, "retries": 0, "refreshes": 0, "conflicts": 0,
        })
        if r["kind"] == INITIAL_DELIVERY:
            slot["deliveries"] = r["c"]
        elif r["kind"] == RETRY:
            slot["retries"] = r["c"]
        elif r["kind"] == REFRESH:
            slot["refreshes"] = r["c"]
        elif r["kind"] == RETRY_CONFLICT:
            slot["conflicts"] = r["c"]
    return stats


async def in_flight_ids() -> list[str]:
    cur = await _conn.execute("SELECT id FROM writes WHERE in_flight = 1 ORDER BY id")
    return [r["id"] for r in await cur.fetchall()]


async def get_write(write_id: str) -> dict[str, Any] | None:
    cur = await _conn.execute("SELECT * FROM writes WHERE id = ?", (write_id,))
    row = await cur.fetchone()
    return dict(row) if row else None


async def list_writes() -> list[dict[str, Any]]:
    cur = await _conn.execute("SELECT * FROM writes ORDER BY created_at DESC LIMIT 200")
    rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def add_event(kind: str, message: str) -> None:
    async with _write_lock:
        await _conn.execute(
            "INSERT INTO events (ts, kind, message) VALUES (?, ?, ?)",
            (time.time(), kind, message),
        )
        await _conn.commit()


async def list_events(limit: int = 30) -> list[dict[str, Any]]:
    cur = await _conn.execute(
        "SELECT ts, kind, message FROM events ORDER BY id DESC LIMIT ?", (limit,)
    )
    rows = await cur.fetchall()
    return [dict(r) for r in rows]
