"""再生水批次追踪核心服务。

谱系模型：进水批次 -> 处理单元流转 -> 采样检测 -> 拆分/混配 -> 放行 -> 装车 -> 客户接收。
不变量：
  * 拆分/合并体积守恒（内部以升为单位整数记账，守恒校验精确成立）；
  * 旁路期间经过的水不自动继承合格状态，降级为 suspect，须重新检测合格；
  * 检测结果迟到、撤回或判废会冻结受影响放行，并生成通知与待复核任务；
  * 已交付记录不可改，只能追加更正；
  * 装车在事务内条件扣减，并发不超卖；回调按键幂等，不重复扣量；
  * 全部状态落 SQLite，重启后冻结/通知/待复核任务可继续处理。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

DEFAULT_TZ = ZoneInfo("Asia/Shanghai")
LITERS_PER_M3 = 1000
DEFAULT_RESULT_DEADLINE_HOURS = 24

QUALITY_PENDING = "pending"
QUALITY_QUALIFIED = "qualified"
QUALITY_UNQUALIFIED = "unqualified"
QUALITY_SUSPECT = "suspect"

RELEASE_ACTIVE = "active"
RELEASE_FROZEN = "frozen"
RELEASE_COMPLETED = "completed"
RELEASE_VOIDED = "voided"


class DomainError(Exception):
    """业务规则冲突基类，status 供 HTTP 层映射。"""

    status = 400
    code = "domain_error"

    def __init__(self, message, **detail):
        super().__init__(message)
        self.message = message
        self.detail = detail


class NotFound(DomainError):
    status = 404
    code = "not_found"


class Conflict(DomainError):
    status = 409
    code = "conflict"


class Forbidden(DomainError):
    status = 403
    code = "forbidden"


def load_config(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _m3_to_l(value, field="volume_m3"):
    try:
        liters = int(round(float(value) * LITERS_PER_M3))
    except (TypeError, ValueError):
        raise DomainError(f"{field} 必须是数字（立方米）")
    return liters


def _l_to_m3(liters):
    return liters / LITERS_PER_M3


def _parse_ts(value, field):
    if not isinstance(value, str) or not value.strip():
        raise DomainError(f"{field} 必须是带时区的 ISO 8601 字符串")
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        raise DomainError(f"{field} 不是合法的 ISO 8601 时间: {value}")
    if dt.tzinfo is None:
        raise DomainError(f"{field} 必须携带时区: {value}")
    return dt


def _to_utc_iso(dt):
    return dt.astimezone(timezone.utc).isoformat()


SCHEMA = """
CREATE TABLE IF NOT EXISTS counters(
  name TEXT PRIMARY KEY,
  value INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS units(
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  params TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS tanks(
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  capacity_l INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS vehicles(
  plate TEXT PRIMARY KEY,
  capacity_l INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS sampling_rules(
  id TEXT PRIMARY KEY,
  point TEXT NOT NULL,
  type TEXT NOT NULL,
  frequency TEXT NOT NULL,
  analytes TEXT NOT NULL DEFAULT '[]',
  deadline_hours REAL NOT NULL,
  required_for_release INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS batches(
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,               -- intake | split | merge
  volume_l INTEGER NOT NULL,        -- 创建时的初始体积
  remaining_l INTEGER NOT NULL,     -- 当前剩余（被拆分/合并/装车扣减）
  quality TEXT NOT NULL,            -- pending | qualified | unqualified | suspect
  bypassed INTEGER NOT NULL DEFAULT 0,
  location TEXT,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batch_edges(
  parent TEXT NOT NULL,
  child TEXT NOT NULL,
  volume_l INTEGER NOT NULL,
  kind TEXT NOT NULL,               -- split | merge
  PRIMARY KEY(parent, child, kind)
);
CREATE TABLE IF NOT EXISTS bypass_windows(
  id TEXT PRIMARY KEY,
  unit_id TEXT NOT NULL,
  start_ts TEXT NOT NULL,
  end_ts TEXT NOT NULL,
  reason TEXT
);
CREATE TABLE IF NOT EXISTS samples(
  id TEXT PRIMARY KEY,
  batch_id TEXT NOT NULL,
  rule_id TEXT,
  type TEXT NOT NULL,
  collected_at TEXT NOT NULL,
  deadline_at TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending'   -- pending | recorded | retracted
);
CREATE TABLE IF NOT EXISTS results(
  sample_id TEXT PRIMARY KEY,
  verdict TEXT NOT NULL,            -- pass | fail
  analytes TEXT NOT NULL DEFAULT '{}',
  recorded_at TEXT NOT NULL,
  late INTEGER NOT NULL DEFAULT 0,
  retracted_at TEXT,
  retract_reason TEXT
);
CREATE TABLE IF NOT EXISTS releases(
  id TEXT PRIMARY KEY,
  batch_id TEXT NOT NULL,
  customer_id TEXT NOT NULL,
  authorized_l INTEGER NOT NULL,
  loaded_l INTEGER NOT NULL DEFAULT 0,
  delivered_l INTEGER NOT NULL DEFAULT 0,
  purpose TEXT,
  basis_sample TEXT,
  status TEXT NOT NULL DEFAULT 'active',   -- active | frozen | completed | voided
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS loads(
  id TEXT PRIMARY KEY,
  release_id TEXT NOT NULL,
  vehicle TEXT NOT NULL,
  volume_l INTEGER NOT NULL,
  occurred_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS deliveries(
  id TEXT PRIMARY KEY,
  release_id TEXT NOT NULL,
  customer_id TEXT NOT NULL,
  volume_l INTEGER NOT NULL,
  delivered_at TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS delivery_corrections(
  id TEXT PRIMARY KEY,
  delivery_id TEXT NOT NULL,
  delta_l INTEGER NOT NULL,
  reason TEXT,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS freezes(
  id TEXT PRIMARY KEY,
  release_id TEXT NOT NULL,
  sample_id TEXT,
  reason TEXT NOT NULL,             -- late_result | failed_result | retracted_result
  status TEXT NOT NULL DEFAULT 'open',   -- open | resolved
  created_at TEXT NOT NULL,
  resolved_at TEXT,
  resolution TEXT                   -- release | void
);
CREATE TABLE IF NOT EXISTS notifications(
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL,
  dispatched INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  dispatched_at TEXT
);
CREATE TABLE IF NOT EXISTS tasks(
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,               -- review_freeze
  ref_id TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'open',   -- open | resolved
  created_at TEXT NOT NULL,
  resolved_at TEXT
);
CREATE TABLE IF NOT EXISTS idempotency_keys(
  key TEXT PRIMARY KEY,
  scope TEXT NOT NULL,
  response TEXT,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events(
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  payload TEXT NOT NULL,
  occurred_at TEXT,
  received_at TEXT NOT NULL
);
"""


class TrackingService:
    """批次谱系追踪服务。线程安全；所有写操作在单事务内完成。"""

    def __init__(self, db_path, config=None, clock=None):
        self._lock = threading.RLock()
        self._clock = clock or (lambda: datetime.now(DEFAULT_TZ))
        self._config = config or {}
        db_path = str(db_path)
        if db_path != ":memory:":
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys=ON")
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()
        if config:
            self._seed(config)

    @classmethod
    def from_config_file(cls, db_path, config_path, clock=None):
        return cls(db_path, config=load_config(config_path), clock=clock)

    def close(self):
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------
    # 基础设施
    # ------------------------------------------------------------------
    def _now(self):
        return _to_utc_iso(self._clock())

    @contextmanager
    def _transact(self):
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                yield cur
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
            finally:
                cur.close()

    def _run_idempotent(self, key, scope, work):
        """在同一事务内登记幂等键并执行 work；重复键直接返回首次结果。"""
        if not key:
            raise DomainError("缺少幂等键")
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                cur.execute(
                    "INSERT INTO idempotency_keys(key, scope, response, created_at)"
                    " VALUES (?,?,NULL,?)",
                    (f"{scope}:{key}", scope, self._now()),
                )
            except sqlite3.IntegrityError:
                self._conn.rollback()
                row = self._conn.execute(
                    "SELECT response FROM idempotency_keys WHERE key=?",
                    (f"{scope}:{key}",),
                ).fetchone()
                if row is None or row["response"] is None:
                    raise Conflict(f"幂等键 {key} 正在处理中，请稍后重试")
                result = json.loads(row["response"])
                result["replayed"] = True
                return result
            try:
                result = dict(work(cur))
                result["replayed"] = False
                cur.execute(
                    "UPDATE idempotency_keys SET response=? WHERE key=?",
                    (json.dumps(result, ensure_ascii=False), f"{scope}:{key}"),
                )
                self._conn.commit()
                return result
            except Exception:
                self._conn.rollback()
                raise
            finally:
                cur.close()

    def _next_id(self, cur, prefix):
        cur.execute(
            "INSERT INTO counters(name, value) VALUES (?, 0)"
            " ON CONFLICT(name) DO NOTHING",
            (prefix,),
        )
        cur.execute("UPDATE counters SET value = value + 1 WHERE name=?", (prefix,))
        value = cur.execute(
            "SELECT value FROM counters WHERE name=?", (prefix,)
        ).fetchone()["value"]
        return f"{prefix}-{value:04d}"

    def _record_event(self, cur, type_, payload, occurred_at=None):
        cur.execute(
            "INSERT INTO events(id, type, payload, occurred_at, received_at)"
            " VALUES (?,?,?,?,?)",
            (
                self._next_id(cur, "event"),
                type_,
                json.dumps(payload, ensure_ascii=False),
                occurred_at,
                self._now(),
            ),
        )

    def _seed(self, config):
        def work(cur):
            for unit in config.get("process", {}).get("units", []):
                cur.execute(
                    "INSERT OR IGNORE INTO units(id, name, params) VALUES (?,?,?)",
                    (unit["id"], unit["name"], json.dumps(unit.get("params", {}), ensure_ascii=False)),
                )
            for tank in config.get("tanks", []):
                cur.execute(
                    "INSERT OR IGNORE INTO tanks(id, name, capacity_l) VALUES (?,?,?)",
                    (tank["id"], tank["name"], _m3_to_l(tank["capacity_m3"], "capacity_m3")),
                )
            for vehicle in config.get("vehicles", []):
                cur.execute(
                    "INSERT OR IGNORE INTO vehicles(plate, capacity_l) VALUES (?,?)",
                    (vehicle["plate"], _m3_to_l(vehicle["capacity_m3"], "capacity_m3")),
                )
            for rule in config.get("sampling_rules", []):
                cur.execute(
                    "INSERT OR IGNORE INTO sampling_rules"
                    "(id, point, type, frequency, analytes, deadline_hours, required_for_release)"
                    " VALUES (?,?,?,?,?,?,?)",
                    (
                        rule["id"],
                        rule["point"],
                        rule["type"],
                        rule["frequency"],
                        json.dumps(rule.get("analytes", []), ensure_ascii=False),
                        float(rule.get("result_deadline_hours", DEFAULT_RESULT_DEADLINE_HOURS)),
                        1 if rule.get("required_for_release") else 0,
                    ),
                )

        with self._transact() as cur:
            work(cur)

    # ------------------------------------------------------------------
    # 行读取与序列化
    # ------------------------------------------------------------------
    @staticmethod
    def _one(cur, sql, params, what):
        row = cur.execute(sql, params).fetchone()
        if row is None:
            raise NotFound(f"{what} 不存在")
        return row

    def _batch_row(self, cur, batch_id):
        return self._one(cur, "SELECT * FROM batches WHERE id=?", (batch_id,), f"批次 {batch_id}")

    def _release_row(self, cur, release_id):
        return self._one(cur, "SELECT * FROM releases WHERE id=?", (release_id,), f"放行单 {release_id}")

    def _sample_row(self, cur, sample_id):
        return self._one(cur, "SELECT * FROM samples WHERE id=?", (sample_id,), f"样本 {sample_id}")

    def _delivery_row(self, cur, delivery_id):
        return self._one(cur, "SELECT * FROM deliveries WHERE id=?", (delivery_id,), f"交付记录 {delivery_id}")

    def _freeze_row(self, cur, freeze_id):
        return self._one(cur, "SELECT * FROM freezes WHERE id=?", (freeze_id,), f"冻结 {freeze_id}")

    @staticmethod
    def _batch_dict(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "volume_m3": _l_to_m3(row["volume_l"]),
            "remaining_m3": _l_to_m3(row["remaining_l"]),
            "quality": row["quality"],
            "bypassed": bool(row["bypassed"]),
            "location": row["location"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _release_dict(row):
        return {
            "id": row["id"],
            "batch_id": row["batch_id"],
            "customer_id": row["customer_id"],
            "authorized_m3": _l_to_m3(row["authorized_l"]),
            "loaded_m3": _l_to_m3(row["loaded_l"]),
            "delivered_m3": _l_to_m3(row["delivered_l"]),
            "purpose": row["purpose"],
            "basis_sample": row["basis_sample"],
            "status": row["status"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _delivery_dict(row):
        return {
            "id": row["id"],
            "release_id": row["release_id"],
            "customer_id": row["customer_id"],
            "volume_m3": _l_to_m3(row["volume_l"]),
            "delivered_at": row["delivered_at"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _freeze_dict(row):
        return {
            "id": row["id"],
            "release_id": row["release_id"],
            "sample_id": row["sample_id"],
            "reason": row["reason"],
            "status": row["status"],
            "created_at": row["created_at"],
            "resolved_at": row["resolved_at"],
            "resolution": row["resolution"],
        }

    @staticmethod
    def _sample_dict(row, result=None):
        data = {
            "id": row["id"],
            "batch_id": row["batch_id"],
            "rule_id": row["rule_id"],
            "type": row["type"],
            "collected_at": row["collected_at"],
            "deadline_at": row["deadline_at"],
            "status": row["status"],
        }
        if result is not None:
            data["result"] = {
                "verdict": result["verdict"],
                "analytes": json.loads(result["analytes"]),
                "recorded_at": result["recorded_at"],
                "late": bool(result["late"]),
                "retracted_at": result["retracted_at"],
                "retract_reason": result["retract_reason"],
            }
        return data

    # ------------------------------------------------------------------
    # 谱系遍历
    # ------------------------------------------------------------------
    @staticmethod
    def _walk(cur, start_id, direction):
        """沿 batch_edges 做递归遍历，返回含起点在内的闭包 id 列表。"""
        if direction == "down":
            sql = (
                "WITH RECURSIVE d(id) AS ("
                " SELECT ? UNION"
                " SELECT e.child FROM batch_edges e JOIN d ON e.parent = d.id"
                ") SELECT id FROM d"
            )
        else:
            sql = (
                "WITH RECURSIVE d(id) AS ("
                " SELECT ? UNION"
                " SELECT e.parent FROM batch_edges e JOIN d ON e.child = d.id"
                ") SELECT id FROM d"
            )
        return [r["id"] for r in cur.execute(sql, (start_id,)).fetchall()]

    # ------------------------------------------------------------------
    # 进水与处理单元流转
    # ------------------------------------------------------------------
    def register_intake(self, intake_id, volume_m3, source_time, metadata=None):
        """登记进水，生成根批次。"""
        volume_l = _m3_to_l(volume_m3)
        if volume_l <= 0:
            raise DomainError("进水量必须为正")
        source_iso = _to_utc_iso(_parse_ts(source_time, "source_time"))

        def work(cur):
            if cur.execute("SELECT 1 FROM batches WHERE id=?", (intake_id,)).fetchone():
                raise Conflict(f"批次 {intake_id} 已存在")
            cur.execute(
                "INSERT INTO batches(id, kind, volume_l, remaining_l, quality, bypassed,"
                " location, created_at) VALUES (?,?,?,?,?,?,?,?)",
                (intake_id, "intake", volume_l, volume_l, QUALITY_PENDING, 0, None, self._now()),
            )
            self._record_event(
                cur,
                "intake_registered",
                {"batch_id": intake_id, "volume_m3": volume_m3, "metadata": metadata or {}},
                occurred_at=source_iso,
            )
            return self._batch_dict(self._batch_row(cur, intake_id))

        with self._transact() as cur:
            return work(cur)

    def open_bypass(self, unit_id, start, end, reason=None, window_id=None):
        """登记处理单元临时旁路窗口。"""
        start_dt = _parse_ts(start, "start")
        end_dt = _parse_ts(end, "end")
        if end_dt <= start_dt:
            raise DomainError("旁路结束时间必须晚于开始时间")

        def work(cur):
            self._one(cur, "SELECT id FROM units WHERE id=?", (unit_id,), f"处理单元 {unit_id}")
            wid = window_id or self._next_id(cur, "bypass")
            cur.execute(
                "INSERT INTO bypass_windows(id, unit_id, start_ts, end_ts, reason)"
                " VALUES (?,?,?,?,?)",
                (wid, unit_id, _to_utc_iso(start_dt), _to_utc_iso(end_dt), reason),
            )
            self._record_event(
                cur,
                "bypass_opened",
                {"window_id": wid, "unit_id": unit_id, "reason": reason},
                occurred_at=_to_utc_iso(start_dt),
            )
            return {
                "id": wid,
                "unit_id": unit_id,
                "start": _to_utc_iso(start_dt),
                "end": _to_utc_iso(end_dt),
                "reason": reason,
            }

        with self._transact() as cur:
            return work(cur)

    def transfer(self, batch_id, unit_id, occurred_at):
        """批次流经处理单元；若该时刻单元处于旁路窗口，批次降级为 suspect。"""
        occurred_iso = _to_utc_iso(_parse_ts(occurred_at, "occurred_at"))

        def work(cur):
            self._one(cur, "SELECT id FROM units WHERE id=?", (unit_id,), f"处理单元 {unit_id}")
            batch = self._batch_row(cur, batch_id)
            in_bypass = cur.execute(
                "SELECT 1 FROM bypass_windows WHERE unit_id=? AND start_ts<=? AND end_ts>=?",
                (unit_id, occurred_iso, occurred_iso),
            ).fetchone()
            quality = batch["quality"]
            bypassed = batch["bypassed"]
            if in_bypass:
                # 旁路期间的水不自动继承合格状态
                quality = QUALITY_SUSPECT
                bypassed = 1
            cur.execute(
                "UPDATE batches SET location=?, quality=?, bypassed=? WHERE id=?",
                (unit_id, quality, bypassed, batch_id),
            )
            self._record_event(
                cur,
                "unit_transfer",
                {"batch_id": batch_id, "unit_id": unit_id, "bypassed": bool(in_bypass)},
                occurred_at=occurred_iso,
            )
            return self._batch_dict(self._batch_row(cur, batch_id))

        with self._transact() as cur:
            return work(cur)

    # ------------------------------------------------------------------
    # 拆分 / 合并（体积守恒）
    # ------------------------------------------------------------------
    def split_batch(self, batch_id, parts_m3, occurred_at=None):
        """把批次拆成若干子批次；子批次体积之和从父批次扣减，总量守恒。"""
        if not isinstance(parts_m3, (list, tuple)) or len(parts_m3) < 2:
            raise DomainError("拆分至少需要两个目标体积")
        parts_l = []
        for i, part in enumerate(parts_m3):
            liters = _m3_to_l(part, f"parts_m3[{i}]")
            if liters <= 0:
                raise DomainError("拆分体积必须为正")
            parts_l.append(liters)
        total = sum(parts_l)
        occurred_iso = _to_utc_iso(_parse_ts(occurred_at, "occurred_at")) if occurred_at else None

        def work(cur):
            batch = self._batch_row(cur, batch_id)
            cur.execute(
                "UPDATE batches SET remaining_l = remaining_l - ?"
                " WHERE id=? AND remaining_l >= ?",
                (total, batch_id, total),
            )
            if cur.rowcount != 1:
                raise Conflict(
                    f"批次 {batch_id} 剩余不足，无法拆出 {total} L",
                    remaining_m3=_l_to_m3(batch["remaining_l"]),
                )
            children = []
            for liters in parts_l:
                child_id = self._next_id(cur, "batch")
                cur.execute(
                    "INSERT INTO batches(id, kind, volume_l, remaining_l, quality, bypassed,"
                    " location, created_at) VALUES (?,?,?,?,?,?,?,?)",
                    (
                        child_id,
                        "split",
                        liters,
                        liters,
                        batch["quality"],
                        batch["bypassed"],
                        batch["location"],
                        self._now(),
                    ),
                )
                cur.execute(
                    "INSERT INTO batch_edges(parent, child, volume_l, kind) VALUES (?,?,?,?)",
                    (batch_id, child_id, liters, "split"),
                )
                children.append(child_id)
            self._record_event(
                cur,
                "batch_split",
                {"parent": batch_id, "children": children, "parts_m3": [_l_to_m3(v) for v in parts_l]},
                occurred_at=occurred_iso,
            )
            return {
                "parent": self._batch_dict(self._batch_row(cur, batch_id)),
                "children": [self._batch_dict(self._batch_row(cur, cid)) for cid in children],
            }

        with self._transact() as cur:
            return work(cur)

    def merge_batches(self, batch_ids, volumes_m3=None, occurred_at=None):
        """把多个批次混配成一个新批次；新批次体积等于各来源贡献之和。"""
        if not isinstance(batch_ids, (list, tuple)) or len(batch_ids) < 2:
            raise DomainError("混配至少需要两个来源批次")
        if len(set(batch_ids)) != len(batch_ids):
            raise DomainError("混配来源批次不能重复")
        if volumes_m3 is not None and len(volumes_m3) != len(batch_ids):
            raise DomainError("volumes_m3 与 batch_ids 数量不一致")
        occurred_iso = _to_utc_iso(_parse_ts(occurred_at, "occurred_at")) if occurred_at else None

        def work(cur):
            parents = [self._batch_row(cur, bid) for bid in batch_ids]
            takes = []
            for i, parent in enumerate(parents):
                if volumes_m3 is None:
                    liters = parent["remaining_l"]
                else:
                    liters = _m3_to_l(volumes_m3[i], f"volumes_m3[{i}]")
                if liters <= 0:
                    raise DomainError(f"来源批次 {parent['id']} 贡献体积必须为正")
                takes.append(liters)
            qualities = {p["quality"] for p in parents}
            if QUALITY_UNQUALIFIED in qualities:
                quality = QUALITY_UNQUALIFIED
            elif QUALITY_SUSPECT in qualities:
                quality = QUALITY_SUSPECT
            elif qualities == {QUALITY_QUALIFIED}:
                quality = QUALITY_QUALIFIED
            else:
                quality = QUALITY_PENDING
            bypassed = 1 if any(p["bypassed"] for p in parents) else 0
            total = sum(takes)
            child_id = self._next_id(cur, "batch")
            for parent, liters in zip(parents, takes):
                cur.execute(
                    "UPDATE batches SET remaining_l = remaining_l - ?"
                    " WHERE id=? AND remaining_l >= ?",
                    (liters, parent["id"], liters),
                )
                if cur.rowcount != 1:
                    raise Conflict(
                        f"批次 {parent['id']} 剩余不足，无法取出 {liters} L",
                        remaining_m3=_l_to_m3(parent["remaining_l"]),
                    )
                cur.execute(
                    "INSERT INTO batch_edges(parent, child, volume_l, kind) VALUES (?,?,?,?)",
                    (parent["id"], child_id, liters, "merge"),
                )
            cur.execute(
                "INSERT INTO batches(id, kind, volume_l, remaining_l, quality, bypassed,"
                " location, created_at) VALUES (?,?,?,?,?,?,?,?)",
                (child_id, "merge", total, total, quality, bypassed, None, self._now()),
            )
            self._record_event(
                cur,
                "batch_merged",
                {
                    "child": child_id,
                    "sources": [
                        {"batch_id": p["id"], "volume_m3": _l_to_m3(v)}
                        for p, v in zip(parents, takes)
                    ],
                },
                occurred_at=occurred_iso,
            )
            return self._batch_dict(self._batch_row(cur, child_id))

        with self._transact() as cur:
            return work(cur)

    def store_in_tank(self, batch_id, tank_id):
        """批次入罐；罐区容量校验。"""
        def work(cur):
            tank = self._one(cur, "SELECT * FROM tanks WHERE id=?", (tank_id,), f"罐 {tank_id}")
            batch = self._batch_row(cur, batch_id)
            used = cur.execute(
                "SELECT COALESCE(SUM(remaining_l),0) AS used FROM batches WHERE location=?",
                (tank_id,),
            ).fetchone()["used"]
            if used + batch["remaining_l"] > tank["capacity_l"]:
                raise Conflict(
                    f"罐 {tank_id} 容量不足",
                    capacity_m3=_l_to_m3(tank["capacity_l"]),
                    used_m3=_l_to_m3(used),
                    required_m3=_l_to_m3(batch["remaining_l"]),
                )
            cur.execute("UPDATE batches SET location=? WHERE id=?", (tank_id, batch_id))
            self._record_event(
                cur, "batch_stored", {"batch_id": batch_id, "tank_id": tank_id}
            )
            return self._batch_dict(self._batch_row(cur, batch_id))

        with self._transact() as cur:
            return work(cur)

    # ------------------------------------------------------------------
    # 采样与检测
    # ------------------------------------------------------------------
    def collect_sample(self, sample_id, batch_id, rule_id, collected_at):
        """按采样规则在批次上采样，生成检测样本（含结果期限）。"""
        collected_dt = _parse_ts(collected_at, "collected_at")

        def work(cur):
            self._batch_row(cur, batch_id)
            rule = None
            if rule_id is not None:
                rule = self._one(
                    cur,
                    "SELECT * FROM sampling_rules WHERE id=?",
                    (rule_id,),
                    f"采样规则 {rule_id}",
                )
            if cur.execute("SELECT 1 FROM samples WHERE id=?", (sample_id,)).fetchone():
                raise Conflict(f"样本 {sample_id} 已存在")
            deadline_hours = (
                rule["deadline_hours"] if rule else DEFAULT_RESULT_DEADLINE_HOURS
            )
            deadline = collected_dt + timedelta(hours=deadline_hours)
            cur.execute(
                "INSERT INTO samples(id, batch_id, rule_id, type, collected_at, deadline_at,"
                " status) VALUES (?,?,?,?,?,?, 'pending')",
                (
                    sample_id,
                    batch_id,
                    rule_id,
                    rule["type"] if rule else "adhoc",
                    _to_utc_iso(collected_dt),
                    _to_utc_iso(deadline),
                ),
            )
            self._record_event(
                cur,
                "sample_collected",
                {"sample_id": sample_id, "batch_id": batch_id, "rule_id": rule_id},
                occurred_at=_to_utc_iso(collected_dt),
            )
            return self._sample_dict(self._sample_row(cur, sample_id))

        with self._transact() as cur:
            return work(cur)

    def _apply_quality(self, cur, batch_id):
        """按当前有效（未撤回）结果重算批次质量状态。"""
        rows = cur.execute(
            "SELECT r.verdict FROM results r JOIN samples s ON s.id = r.sample_id"
            " WHERE s.batch_id=? AND s.status='recorded'",
            (batch_id,),
        ).fetchall()
        verdicts = {r["verdict"] for r in rows}
        batch = self._batch_row(cur, batch_id)
        if "fail" in verdicts:
            quality = QUALITY_UNQUALIFIED
        elif "pass" in verdicts:
            quality = QUALITY_QUALIFIED
        elif batch["bypassed"]:
            quality = QUALITY_SUSPECT
        else:
            quality = QUALITY_PENDING
        cur.execute("UPDATE batches SET quality=? WHERE id=?", (quality, batch_id))
        return quality

    def _propagate_suspect(self, cur, batch_id):
        """检测证据失效时，下游已合格批次一律降级为 suspect，等待重新检测。"""
        descendants = self._walk(cur, batch_id, "down")
        descendants.remove(batch_id)
        if not descendants:
            return []
        marks = ",".join("?" * len(descendants))
        cur.execute(
            f"UPDATE batches SET quality=? WHERE id IN ({marks}) AND quality=?",
            (QUALITY_SUSPECT, *descendants, QUALITY_QUALIFIED),
        )
        return descendants

    def _notify(self, cur, kind, payload):
        nid = self._next_id(cur, "notification")
        cur.execute(
            "INSERT INTO notifications(id, kind, payload, dispatched, created_at)"
            " VALUES (?,?,?,0,?)",
            (nid, kind, json.dumps(payload, ensure_ascii=False), self._now()),
        )
        return nid

    def _add_task(self, cur, kind, ref_id):
        tid = self._next_id(cur, "task")
        cur.execute(
            "INSERT INTO tasks(id, kind, ref_id, status, created_at) VALUES (?,?,?, 'open', ?)",
            (tid, kind, ref_id, self._now()),
        )
        return tid

    def _freeze_affected(self, cur, sample_id, reason):
        """冻结样本影响范围内所有未结清放行，并生成通知与待复核任务。"""
        sample = self._sample_row(cur, sample_id)
        batch_ids = self._walk(cur, sample["batch_id"], "down")
        marks = ",".join("?" * len(batch_ids))
        releases = cur.execute(
            f"SELECT * FROM releases WHERE batch_id IN ({marks})"
            " AND status IN ('active','completed')",
            batch_ids,
        ).fetchall()
        frozen = []
        for release in releases:
            dup = cur.execute(
                "SELECT 1 FROM freezes WHERE release_id=? AND sample_id=? AND reason=?"
                " AND status='open'",
                (release["id"], sample_id, reason),
            ).fetchone()
            if dup:
                continue
            freeze_id = self._next_id(cur, "freeze")
            cur.execute(
                "INSERT INTO freezes(id, release_id, sample_id, reason, status, created_at)"
                " VALUES (?,?,?,?, 'open', ?)",
                (freeze_id, release["id"], sample_id, reason, self._now()),
            )
            cur.execute(
                "UPDATE releases SET status=? WHERE id=?", (RELEASE_FROZEN, release["id"])
            )
            self._notify(
                cur,
                "RELEASE_FROZEN",
                {
                    "freeze_id": freeze_id,
                    "release_id": release["id"],
                    "customer_id": release["customer_id"],
                    "sample_id": sample_id,
                    "reason": reason,
                },
            )
            self._add_task(cur, "review_freeze", freeze_id)
            frozen.append(freeze_id)
        return frozen

    def record_result(self, sample_id, verdict, analytes=None, recorded_at=None):
        """登记检测结果。迟到或判废会冻结受影响放行。"""
        if verdict not in ("pass", "fail"):
            raise DomainError("verdict 必须是 pass 或 fail")
        recorded_dt = self._clock() if recorded_at is None else _parse_ts(recorded_at, "recorded_at")
        recorded_iso = _to_utc_iso(recorded_dt)

        def work(cur):
            sample = self._sample_row(cur, sample_id)
            if sample["status"] != "pending":
                raise Conflict(f"样本 {sample_id} 状态为 {sample['status']}，不能登记结果")
            late = recorded_iso > sample["deadline_at"]
            cur.execute(
                "INSERT INTO results(sample_id, verdict, analytes, recorded_at, late)"
                " VALUES (?,?,?,?,?)",
                (
                    sample_id,
                    verdict,
                    json.dumps(analytes or {}, ensure_ascii=False),
                    recorded_iso,
                    1 if late else 0,
                ),
            )
            cur.execute("UPDATE samples SET status='recorded' WHERE id=?", (sample_id,))
            quality = self._apply_quality(cur, sample["batch_id"])
            freezes = []
            if verdict == "fail" or late:
                # 判废或迟到：谱系下游批次降级，受影响放行冻结
                self._propagate_suspect(cur, sample["batch_id"])
                reason = "failed_result" if verdict == "fail" else "late_result"
                freezes = self._freeze_affected(cur, sample_id, reason)
            self._record_event(
                cur,
                "result_recorded",
                {
                    "sample_id": sample_id,
                    "verdict": verdict,
                    "late": late,
                    "freezes": freezes,
                },
                occurred_at=recorded_iso,
            )
            result = cur.execute(
                "SELECT * FROM results WHERE sample_id=?", (sample_id,)
            ).fetchone()
            return {
                "sample": self._sample_dict(self._sample_row(cur, sample_id), result),
                "batch_quality": quality,
                "freezes": freezes,
            }

        with self._transact() as cur:
            return work(cur)

    def retract_result(self, sample_id, reason, retracted_at=None):
        """撤回检测结果：批次失去合格依据，受影响放行冻结。"""
        retracted_iso = _to_utc_iso(
            self._clock() if retracted_at is None else _parse_ts(retracted_at, "retracted_at")
        )

        def work(cur):
            sample = self._sample_row(cur, sample_id)
            if sample["status"] != "recorded":
                raise Conflict(f"样本 {sample_id} 状态为 {sample['status']}，不能撤回")
            cur.execute("UPDATE samples SET status='retracted' WHERE id=?", (sample_id,))
            cur.execute(
                "UPDATE results SET retracted_at=?, retract_reason=? WHERE sample_id=?",
                (retracted_iso, reason, sample_id),
            )
            quality = self._apply_quality(cur, sample["batch_id"])
            self._propagate_suspect(cur, sample["batch_id"])
            freezes = self._freeze_affected(cur, sample_id, "retracted_result")
            self._record_event(
                cur,
                "result_retracted",
                {"sample_id": sample_id, "reason": reason, "freezes": freezes},
                occurred_at=retracted_iso,
            )
            result = cur.execute(
                "SELECT * FROM results WHERE sample_id=?", (sample_id,)
            ).fetchone()
            return {
                "sample": self._sample_dict(self._sample_row(cur, sample_id), result),
                "batch_quality": quality,
                "freezes": freezes,
            }

        with self._transact() as cur:
            return work(cur)

    # ------------------------------------------------------------------
    # 放行 / 装车 / 交付
    # ------------------------------------------------------------------
    def create_release(self, release_id, batch_id, customer_id, volume_m3, purpose=None):
        """开立放行单。仅合格批次可放行；放行单是额度，装车时才扣库存。"""
        volume_l = _m3_to_l(volume_m3)
        if volume_l <= 0:
            raise DomainError("放行量必须为正")
        if not customer_id:
            raise DomainError("customer_id 不能为空")

        def work(cur):
            batch = self._batch_row(cur, batch_id)
            if cur.execute("SELECT 1 FROM releases WHERE id=?", (release_id,)).fetchone():
                raise Conflict(f"放行单 {release_id} 已存在")
            if batch["quality"] != QUALITY_QUALIFIED:
                raise Conflict(
                    f"批次 {batch_id} 质量状态为 {batch['quality']}，不允许放行",
                    quality=batch["quality"],
                )
            if volume_l > batch["remaining_l"]:
                raise Conflict(
                    f"放行量超过批次剩余",
                    remaining_m3=_l_to_m3(batch["remaining_l"]),
                )
            basis = cur.execute(
                "SELECT s.id FROM samples s JOIN results r ON r.sample_id = s.id"
                " WHERE s.batch_id=? AND s.status='recorded' AND r.verdict='pass'"
                " ORDER BY r.recorded_at DESC LIMIT 1",
                (batch_id,),
            ).fetchone()
            cur.execute(
                "INSERT INTO releases(id, batch_id, customer_id, authorized_l, purpose,"
                " basis_sample, status, created_at) VALUES (?,?,?,?,?,?,?,?)",
                (
                    release_id,
                    batch_id,
                    customer_id,
                    volume_l,
                    purpose,
                    basis["id"] if basis else None,
                    RELEASE_ACTIVE,
                    self._now(),
                ),
            )
            self._record_event(
                cur,
                "release_created",
                {
                    "release_id": release_id,
                    "batch_id": batch_id,
                    "customer_id": customer_id,
                    "volume_m3": volume_m3,
                },
            )
            return self._release_dict(self._release_row(cur, release_id))

        with self._transact() as cur:
            return work(cur)

    def load_vehicle(self, release_id, load_id, vehicle_id, volume_m3, occurred_at=None):
        """装车扣库存。事务内条件扣减，并发不超出可用库存；load_id 幂等。"""
        volume_l = _m3_to_l(volume_m3)
        if volume_l <= 0:
            raise DomainError("装车量必须为正")
        occurred_iso = _to_utc_iso(
            self._clock() if occurred_at is None else _parse_ts(occurred_at, "occurred_at")
        )

        def work(cur):
            release = self._release_row(cur, release_id)
            if release["status"] != RELEASE_ACTIVE:
                raise Conflict(
                    f"放行单 {release_id} 状态为 {release['status']}，禁止装车",
                    status=release["status"],
                )
            vehicle = self._one(
                cur, "SELECT * FROM vehicles WHERE plate=?", (vehicle_id,), f"车辆 {vehicle_id}"
            )
            if volume_l > vehicle["capacity_l"]:
                raise Conflict(
                    f"装车量超过车辆 {vehicle_id} 核载",
                    capacity_m3=_l_to_m3(vehicle["capacity_l"]),
                )
            if release["loaded_l"] + volume_l > release["authorized_l"]:
                raise Conflict(
                    f"装车量超出放行单可用额度",
                    authorized_m3=_l_to_m3(release["authorized_l"]),
                    loaded_m3=_l_to_m3(release["loaded_l"]),
                )
            cur.execute(
                "UPDATE batches SET remaining_l = remaining_l - ?"
                " WHERE id=? AND remaining_l >= ?",
                (volume_l, release["batch_id"], volume_l),
            )
            if cur.rowcount != 1:
                raise Conflict(f"批次 {release['batch_id']} 可用库存不足")
            cur.execute(
                "UPDATE releases SET loaded_l = loaded_l + ?"
                " WHERE id=? AND loaded_l + ? <= authorized_l",
                (volume_l, release_id, volume_l),
            )
            if cur.rowcount != 1:
                raise Conflict("并发装车超出放行额度")
            cur.execute(
                "INSERT INTO loads(id, release_id, vehicle, volume_l, occurred_at)"
                " VALUES (?,?,?,?,?)",
                (load_id, release_id, vehicle_id, volume_l, occurred_iso),
            )
            self._record_event(
                cur,
                "vehicle_loaded",
                {
                    "load_id": load_id,
                    "release_id": release_id,
                    "vehicle": vehicle_id,
                    "volume_m3": _l_to_m3(volume_l),
                },
                occurred_at=occurred_iso,
            )
            return {
                "load": {
                    "id": load_id,
                    "release_id": release_id,
                    "vehicle": vehicle_id,
                    "volume_m3": _l_to_m3(volume_l),
                    "occurred_at": occurred_iso,
                },
                "release": self._release_dict(self._release_row(cur, release_id)),
            }

        return self._run_idempotent(load_id, "load", work)

    def confirm_delivery(self, release_id, callback_id, volume_m3, delivered_at=None):
        """交付回调。同一放行单重复回调按 callback_id 幂等，不重复扣量。"""
        volume_l = _m3_to_l(volume_m3)
        if volume_l <= 0:
            raise DomainError("交付量必须为正")
        delivered_iso = _to_utc_iso(
            self._clock() if delivered_at is None else _parse_ts(delivered_at, "delivered_at")
        )

        def work(cur):
            release = self._release_row(cur, release_id)
            if release["status"] in (RELEASE_FROZEN, RELEASE_VOIDED):
                raise Conflict(
                    f"放行单 {release_id} 状态为 {release['status']}，禁止交付",
                    status=release["status"],
                )
            if release["delivered_l"] + volume_l > release["loaded_l"]:
                raise Conflict(
                    "交付量超过已装车量",
                    loaded_m3=_l_to_m3(release["loaded_l"]),
                    delivered_m3=_l_to_m3(release["delivered_l"]),
                )
            delivery_id = self._next_id(cur, "delivery")
            cur.execute(
                "INSERT INTO deliveries(id, release_id, customer_id, volume_l, delivered_at,"
                " created_at) VALUES (?,?,?,?,?,?)",
                (
                    delivery_id,
                    release_id,
                    release["customer_id"],
                    volume_l,
                    delivered_iso,
                    self._now(),
                ),
            )
            new_status = (
                RELEASE_COMPLETED
                if release["delivered_l"] + volume_l >= release["authorized_l"]
                else RELEASE_ACTIVE
            )
            cur.execute(
                "UPDATE releases SET delivered_l = delivered_l + ?, status=? WHERE id=?",
                (volume_l, new_status, release_id),
            )
            self._record_event(
                cur,
                "delivery_confirmed",
                {
                    "delivery_id": delivery_id,
                    "release_id": release_id,
                    "callback_id": callback_id,
                    "volume_m3": _l_to_m3(volume_l),
                },
                occurred_at=delivered_iso,
            )
            return {
                "delivery": self._delivery_dict(self._delivery_row(cur, delivery_id)),
                "release": self._release_dict(self._release_row(cur, release_id)),
            }

        return self._run_idempotent(callback_id, "delivery_callback", work)

    def correct_delivery(self, delivery_id, correction_id, delta_m3, reason):
        """已交付记录只能追加更正，原记录保持不变。"""
        delta_l = _m3_to_l(delta_m3, "delta_m3")
        if delta_l == 0:
            raise DomainError("更正量不能为 0")

        def work(cur):
            delivery = self._delivery_row(cur, delivery_id)
            cur.execute(
                "INSERT INTO delivery_corrections(id, delivery_id, delta_l, reason, created_at)"
                " VALUES (?,?,?,?,?)",
                (correction_id, delivery_id, delta_l, reason, self._now()),
            )
            self._notify(
                cur,
                "DELIVERY_CORRECTED",
                {
                    "delivery_id": delivery_id,
                    "release_id": delivery["release_id"],
                    "customer_id": delivery["customer_id"],
                    "delta_m3": _l_to_m3(delta_l),
                    "reason": reason,
                },
            )
            self._record_event(
                cur,
                "delivery_corrected",
                {
                    "delivery_id": delivery_id,
                    "correction_id": correction_id,
                    "delta_m3": _l_to_m3(delta_l),
                    "reason": reason,
                },
            )
            corrections = [
                {
                    "id": row["id"],
                    "delta_m3": _l_to_m3(row["delta_l"]),
                    "reason": row["reason"],
                    "created_at": row["created_at"],
                }
                for row in cur.execute(
                    "SELECT * FROM delivery_corrections WHERE delivery_id=? ORDER BY created_at",
                    (delivery_id,),
                ).fetchall()
            ]
            return {
                "delivery": self._delivery_dict(delivery),
                "corrections": corrections,
            }

        return self._run_idempotent(correction_id, "correction", work)

    # ------------------------------------------------------------------
    # 冻结复核
    # ------------------------------------------------------------------
    def resolve_freeze(self, freeze_id, decision, resolved_at=None):
        """质量人员复核冻结：release 恢复放行，void 作废放行单。"""
        if decision not in ("release", "void"):
            raise DomainError("decision 必须是 release 或 void")
        resolved_iso = _to_utc_iso(
            self._clock() if resolved_at is None else _parse_ts(resolved_at, "resolved_at")
        )

        def work(cur):
            freeze = self._freeze_row(cur, freeze_id)
            if freeze["status"] != "open":
                raise Conflict(f"冻结 {freeze_id} 已处理")
            cur.execute(
                "UPDATE freezes SET status='resolved', resolved_at=?, resolution=? WHERE id=?",
                (resolved_iso, decision, freeze_id),
            )
            cur.execute(
                "UPDATE tasks SET status='resolved', resolved_at=?"
                " WHERE kind='review_freeze' AND ref_id=? AND status='open'",
                (resolved_iso, freeze_id),
            )
            release = self._release_row(cur, freeze["release_id"])
            if decision == "void":
                cur.execute(
                    "UPDATE releases SET status=? WHERE id=?",
                    (RELEASE_VOIDED, release["id"]),
                )
                self._notify(
                    cur,
                    "RELEASE_VOIDED",
                    {"release_id": release["id"], "customer_id": release["customer_id"],
                     "freeze_id": freeze_id},
                )
            else:
                open_left = cur.execute(
                    "SELECT COUNT(*) AS n FROM freezes WHERE release_id=? AND status='open'",
                    (release["id"],),
                ).fetchone()["n"]
                if open_left == 0:
                    new_status = (
                        RELEASE_COMPLETED
                        if release["delivered_l"] >= release["authorized_l"]
                        else RELEASE_ACTIVE
                    )
                    cur.execute(
                        "UPDATE releases SET status=? WHERE id=?",
                        (new_status, release["id"]),
                    )
                self._notify(
                    cur,
                    "RELEASE_UNFROZEN",
                    {"release_id": release["id"], "customer_id": release["customer_id"],
                     "freeze_id": freeze_id},
                )
            self._record_event(
                cur,
                "freeze_resolved",
                {"freeze_id": freeze_id, "decision": decision},
                occurred_at=resolved_iso,
            )
            return {
                "freeze": self._freeze_dict(self._freeze_row(cur, freeze_id)),
                "release": self._release_dict(self._release_row(cur, freeze["release_id"])),
            }

        with self._transact() as cur:
            return work(cur)

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def get_batch(self, batch_id):
        with self._lock:
            return self._batch_dict(self._batch_row(self._conn.cursor(), batch_id))

    def list_batches(self):
        with self._lock:
            rows = self._conn.execute("SELECT * FROM batches ORDER BY created_at, id").fetchall()
            return [self._batch_dict(r) for r in rows]

    def genealogy(self, batch_id):
        """批次谱系：祖先、后代及全部转移边。"""
        with self._lock:
            cur = self._conn.cursor()
            self._batch_row(cur, batch_id)
            ancestors = self._walk(cur, batch_id, "up")
            descendants = self._walk(cur, batch_id, "down")
            closure = sorted(set(ancestors) | set(descendants))
            marks = ",".join("?" * len(closure))
            edges = cur.execute(
                f"SELECT parent, child, volume_l, kind FROM batch_edges"
                f" WHERE parent IN ({marks}) OR child IN ({marks})",
                closure + closure,
            ).fetchall()
            rows = cur.execute(
                f"SELECT * FROM batches WHERE id IN ({marks})", closure
            ).fetchall()
            by_id = {r["id"]: self._batch_dict(r) for r in rows}
            return {
                "batch": by_id[batch_id],
                "ancestors": [by_id[i] for i in ancestors if i != batch_id],
                "descendants": [by_id[i] for i in descendants if i != batch_id],
                "edges": [
                    {
                        "parent": e["parent"],
                        "child": e["child"],
                        "volume_m3": _l_to_m3(e["volume_l"]),
                        "kind": e["kind"],
                    }
                    for e in edges
                ],
            }

    def sample_impact(self, sample_id):
        """质量反查：一个检测样本影响的全部批次、放行、交付与冻结。"""
        with self._lock:
            cur = self._conn.cursor()
            sample = self._sample_row(cur, sample_id)
            result = cur.execute(
                "SELECT * FROM results WHERE sample_id=?", (sample_id,)
            ).fetchone()
            batch_ids = self._walk(cur, sample["batch_id"], "down")
            marks = ",".join("?" * len(batch_ids))
            batches = cur.execute(
                f"SELECT * FROM batches WHERE id IN ({marks})", batch_ids
            ).fetchall()
            releases = cur.execute(
                f"SELECT * FROM releases WHERE batch_id IN ({marks})", batch_ids
            ).fetchall()
            release_ids = [r["id"] for r in releases]
            deliveries = []
            if release_ids:
                rmarks = ",".join("?" * len(release_ids))
                deliveries = cur.execute(
                    f"SELECT * FROM deliveries WHERE release_id IN ({rmarks})",
                    release_ids,
                ).fetchall()
                freezes = cur.execute(
                    f"SELECT * FROM freezes WHERE sample_id=? OR release_id IN ({rmarks})",
                    (sample_id, *release_ids),
                ).fetchall()
            else:
                freezes = cur.execute(
                    "SELECT * FROM freezes WHERE sample_id=?", (sample_id,)
                ).fetchall()
            return {
                "sample": self._sample_dict(sample, result),
                "batches": [self._batch_dict(b) for b in batches],
                "releases": [self._release_dict(r) for r in releases],
                "deliveries": [self._delivery_dict(d) for d in deliveries],
                "freezes": [self._freeze_dict(f) for f in freezes],
            }

    def list_releases(self, customer_id=None):
        with self._lock:
            if customer_id:
                rows = self._conn.execute(
                    "SELECT * FROM releases WHERE customer_id=? ORDER BY created_at, id",
                    (customer_id,),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM releases ORDER BY created_at, id"
                ).fetchall()
            return [self._release_dict(r) for r in rows]

    def get_release(self, release_id):
        with self._lock:
            return self._release_dict(self._release_row(self._conn.cursor(), release_id))

    def list_deliveries(self, customer_id=None):
        with self._lock:
            if customer_id:
                rows = self._conn.execute(
                    "SELECT * FROM deliveries WHERE customer_id=? ORDER BY created_at, id",
                    (customer_id,),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM deliveries ORDER BY created_at, id"
                ).fetchall()
            return [self._delivery_dict(r) for r in rows]

    def get_delivery(self, delivery_id):
        with self._lock:
            cur = self._conn.cursor()
            delivery = self._delivery_row(cur, delivery_id)
            corrections = cur.execute(
                "SELECT * FROM delivery_corrections WHERE delivery_id=? ORDER BY created_at",
                (delivery_id,),
            ).fetchall()
            data = self._delivery_dict(delivery)
            data["corrections"] = [
                {
                    "id": c["id"],
                    "delta_m3": _l_to_m3(c["delta_l"]),
                    "reason": c["reason"],
                    "created_at": c["created_at"],
                }
                for c in corrections
            ]
            return data

    def list_freezes(self, status=None):
        with self._lock:
            if status:
                rows = self._conn.execute(
                    "SELECT * FROM freezes WHERE status=? ORDER BY created_at, id", (status,)
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM freezes ORDER BY created_at, id"
                ).fetchall()
            return [self._freeze_dict(r) for r in rows]

    def list_tasks(self, status=None):
        with self._lock:
            if status:
                rows = self._conn.execute(
                    "SELECT * FROM tasks WHERE status=? ORDER BY created_at, id", (status,)
                ).fetchall()
            else:
                rows = self._conn.execute("SELECT * FROM tasks ORDER BY created_at, id").fetchall()
            return [dict(r) for r in rows]

    def list_notifications(self, pending_only=False):
        with self._lock:
            sql = "SELECT * FROM notifications"
            if pending_only:
                sql += " WHERE dispatched=0"
            sql += " ORDER BY created_at, id"
            return [
                {
                    "id": r["id"],
                    "kind": r["kind"],
                    "payload": json.loads(r["payload"]),
                    "dispatched": bool(r["dispatched"]),
                    "created_at": r["created_at"],
                    "dispatched_at": r["dispatched_at"],
                }
                for r in self._conn.execute(sql).fetchall()
            ]

    def dispatch_notifications(self):
        """外发全部待发送通知（出站队列排空）；重启后未发送的仍在队列中。"""

        def work(cur):
            rows = cur.execute(
                "SELECT * FROM notifications WHERE dispatched=0 ORDER BY created_at, id"
            ).fetchall()
            now = self._now()
            sent = []
            for row in rows:
                cur.execute(
                    "UPDATE notifications SET dispatched=1, dispatched_at=? WHERE id=?",
                    (now, row["id"]),
                )
                sent.append(
                    {
                        "id": row["id"],
                        "kind": row["kind"],
                        "payload": json.loads(row["payload"]),
                    }
                )
            return {"dispatched": sent}

        with self._transact() as cur:
            return work(cur)

    def inventory(self):
        """罐区与批次库存总览。"""
        with self._lock:
            cur = self._conn.cursor()
            tanks = []
            for tank in cur.execute("SELECT * FROM tanks ORDER BY id").fetchall():
                batches = cur.execute(
                    "SELECT id, remaining_l FROM batches WHERE location=? AND remaining_l > 0"
                    " ORDER BY id",
                    (tank["id"],),
                ).fetchall()
                used = sum(b["remaining_l"] for b in batches)
                tanks.append(
                    {
                        "id": tank["id"],
                        "name": tank["name"],
                        "capacity_m3": _l_to_m3(tank["capacity_l"]),
                        "used_m3": _l_to_m3(used),
                        "batches": [
                            {"id": b["id"], "remaining_m3": _l_to_m3(b["remaining_l"])}
                            for b in batches
                        ],
                    }
                )
            return {"tanks": tanks}

    def conservation_report(self):
        """体积守恒校验：进水总量 = 批次剩余 + 在途 + 已交付（毛量）。"""
        with self._lock:
            cur = self._conn.cursor()
            intake = cur.execute(
                "SELECT COALESCE(SUM(volume_l),0) AS v FROM batches WHERE kind='intake'"
            ).fetchone()["v"]
            remaining = cur.execute(
                "SELECT COALESCE(SUM(remaining_l),0) AS v FROM batches"
            ).fetchone()["v"]
            loaded = cur.execute(
                "SELECT COALESCE(SUM(loaded_l),0) AS v FROM releases"
            ).fetchone()["v"]
            delivered_rel = cur.execute(
                "SELECT COALESCE(SUM(delivered_l),0) AS v FROM releases"
            ).fetchone()["v"]
            delivered = cur.execute(
                "SELECT COALESCE(SUM(volume_l),0) AS v FROM deliveries"
            ).fetchone()["v"]
            corrections = cur.execute(
                "SELECT COALESCE(SUM(delta_l),0) AS v FROM delivery_corrections"
            ).fetchone()["v"]
            in_transit = loaded - delivered_rel
            balanced = intake == remaining + in_transit + delivered
            return {
                "intake_m3": _l_to_m3(intake),
                "remaining_m3": _l_to_m3(remaining),
                "in_transit_m3": _l_to_m3(in_transit),
                "delivered_m3": _l_to_m3(delivered),
                "corrections_m3": _l_to_m3(corrections),
                "balanced": balanced,
            }

    def get_config(self):
        return self._config
