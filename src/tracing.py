"""再生水批次追踪服务——领域核心。

设计要点：
- 事件溯源：所有状态变化以不可变事件追加到 JSONL 日志（fsync 落盘），
  重启后重放恢复全部状态，包括冻结、通知、待复核任务。
- 批次谱系：进水 -> 处理（可标记旁路）/ 拆分 -> 混配 -> 放行 -> 装车交付，
  父子边记录体积贡献，拆分/合并/处理逐项做体积守恒校验。
- 旁路水不自动继承上游合格状态，只能凭本批次留样检测转正。
- 检测结果不合格/撤回/迟到沿谱系向下游传播，冻结受影响放行；
  已交付部分只能追加质量通告/更正，原记录不可变。
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

VOL_EPS = 1e-6


def now_iso(tz_name: str = "Asia/Shanghai") -> str:
    return datetime.now(ZoneInfo(tz_name)).isoformat()


def parse_ts(value: str | None, default_tz: str = "Asia/Shanghai") -> datetime:
    if value is None:
        return datetime.now(ZoneInfo(default_tz))
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo(default_tz))
    return dt


def iso(dt: datetime) -> str:
    return dt.isoformat()


class ServiceError(Exception):
    def __init__(self, code: str, message: str, http_status: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status


# 质量状态排序：数值越大越差
_Q_RANK = {"qualified": 0, "provisional": 1, "unknown": 2, "unqualified": 3}


class EventStore:
    path: str

    def __init__(self, data_dir: str):
        os.makedirs(data_dir, exist_ok=True)
        self.path = os.path.join(data_dir, "events.log")
        if not os.path.exists(self.path):
            open(self.path, "a").close()
        self._lock = threading.Lock()
        self._fh = open(self.path, "a+", encoding="utf-8")

    def append(self, event: dict) -> None:
        line = json.dumps(event, ensure_ascii=False, sort_keys=True)
        with self._lock:
            self._fh.write(line + "\n")
            self._fh.flush()
            os.fsync(self._fh.fileno())

    def read_all(self) -> list[dict]:
        with self._lock:
            self._fh.flush()
            with open(self.path, "r", encoding="utf-8") as fh:
                return [json.loads(line) for line in fh if line.strip()]

    def close(self) -> None:
        with self._lock:
            self._fh.close()


class TracingService:
    """所有命令方法都在全局锁内执行；事件先落盘再更新内存态。"""

    def __init__(self, store: EventStore, config: dict | None = None):
        self.store = store
        self.config = config or {}
        tz_name = self.config.get("site", {}).get("timezone", "Asia/Shanghai")
        self.tz = ZoneInfo(tz_name)
        self._lock = threading.RLock()

        self.batches: dict[str, dict] = {}
        self.samples: dict[str, dict] = {}
        self.releases: dict[str, dict] = {}
        self.dispatches: dict[str, dict] = {}
        self.notifications: list[dict] = []
        self.tasks: dict[str, dict] = {}
        self.corrections: list[dict] = []
        self.tank_levels: dict[str, float] = {}
        self.tank_capacity: dict[str, float] = {}
        for tank in self.config.get("tanks", []):
            self.tank_capacity[tank["id"]] = float(tank["capacity_m3"])
            self.tank_levels.setdefault(tank["id"], 0.0)
        self.vehicles = {v["id"]: v for v in self.config.get("vehicles", [])}
        self.customers = {c["id"]: c for c in self.config.get("customers", [])}
        self.units = {u["id"]: u for u in self.config.get("process_units", [])}
        due_hours = self.config.get("sampling_rules", {}).get("result_due_hours", 24)
        self.result_due = timedelta(hours=due_hours)

        self._seq = 0
        self._idem: dict[str, str] = {}
        self._idem_seen: set[str] = set()
        self._late_marked: set[str] = set()

        events = self.store.read_all()
        for event in events:
            self._apply(event)
        # 重放后恢复全部幂等键；批次按创建顺序即拓扑序，自上而下重算质量，
        # 使重启后的派生质量（含旁路/混配/污染继承）与事件流一致
        for event in events:
            key = event.get("idempotency_key")
            if key:
                self._idem_seen.add(key)
                if event["type"] == "dispatch_registered":
                    self._idem[key] = event["payload"]["dispatch_id"]
        for bid in self.batches:
            self._recompute_batch_quality(bid)

    # ------------------------------------------------------------------ 工具

    def _next_id(self, prefix: str) -> str:
        self._seq += 1
        return f"{prefix}-{self._seq:04d}"

    def _emit(self, event_type: str, payload: dict, idem_key: str | None = None) -> dict:
        if idem_key is not None and idem_key in self._idem_seen:
            raise ServiceError("idem_replayed", "幂等键已处理，重复请求被拒绝", 409)
        event = {
            "event_id": f"event-{self._seq + 1:05d}",
            "type": event_type,
            "recorded_at": now_iso(self.config.get("site", {}).get("timezone", "Asia/Shanghai")),
            "payload": payload,
        }
        if idem_key:
            event["idempotency_key"] = idem_key
        self.store.append(event)
        self._apply(event)
        if idem_key:
            self._idem_seen.add(idem_key)
        return event

    def _need_batch(self, batch_id: str) -> dict:
        batch = self.batches.get(batch_id)
        if batch is None:
            raise ServiceError("batch_not_found", f"批次 {batch_id} 不存在", 404)
        return batch

    def _batch_available(self, batch: dict) -> float:
        """可继续使用（混配/处理转出/放行）的体积。"""
        committed = sum(r["volume"] for r in self.releases.values() if r["batch_id"] == batch["id"])
        return batch["volume"] - batch.get("allocated", 0.0) - committed

    def _release_remaining(self, release: dict) -> tuple[float, float]:
        reserved = confirmed = 0.0
        for did in release["dispatches"]:
            d = self.dispatches[did]
            if d["status"] in ("loaded", "frozen"):
                reserved += d["volume"]
            elif d["status"] == "confirmed":
                confirmed += d["volume"]
        return release["volume"] - reserved - confirmed, reserved + confirmed

    def _check_tank_deltas(self, deltas: dict[str, float]) -> None:
        for tank_id, delta in deltas.items():
            if tank_id not in self.tank_capacity:
                raise ServiceError("tank_not_found", f"罐 {tank_id} 不在罐区清单", 404)
            new_level = self.tank_levels.get(tank_id, 0.0) + delta
            if abs(delta) < VOL_EPS:
                continue
            if new_level > self.tank_capacity[tank_id] + VOL_EPS:
                raise ServiceError(
                    "tank_capacity_exceeded",
                    f"罐 {tank_id} 容量 {self.tank_capacity[tank_id]} m³，装量将达 {new_level:.3f} m³",
                    409,
                )
            if new_level < -VOL_EPS:
                raise ServiceError("tank_insufficient", f"罐 {tank_id} 存量不足", 409)

    # -------------------------------------------------------------- 命令：进水

    def receive_influent(self, cmd: dict) -> dict:
        with self._lock:
            volume = float(cmd.get("volume_m3", 0))
            tank = cmd.get("tank")
            if volume <= 0:
                raise ServiceError("bad_volume", "进水体积必须为正")
            if tank not in self.tank_capacity:
                raise ServiceError("tank_not_found", f"罐 {tank} 不在罐区清单", 404)
            at = parse_ts(cmd.get("occurred_at"), self.config.get("site", {}).get("timezone", "Asia/Shanghai"))
            self._check_tank_deltas({tank: volume})
            bid = self._next_id("B")
            self._emit(
                "influent_received",
                {
                    "batch_id": bid,
                    "tank": tank,
                    "volume_m3": volume,
                    "source": cmd.get("source"),
                    "occurred_at": iso(at),
                },
                idem_key=cmd.get("idempotency_key"),
            )
            return self.batches[bid]

    # -------------------------------------------------------- 命令：处理单元过料

    def process_water(self, cmd: dict) -> dict:
        with self._lock:
            parent = self._need_batch(cmd["batch_id"])
            unit_id = cmd.get("unit")
            if unit_id not in self.units:
                raise ServiceError("unit_not_found", f"处理单元 {unit_id} 不在工艺清单", 404)
            bypass = bool(cmd.get("bypass", False))
            in_vol = float(cmd.get("input_volume_m3", parent["volume"] - parent.get("allocated", 0.0)))
            loss = float(cmd.get("loss_m3", 0.0))
            if in_vol <= 0 or loss < 0 or loss >= in_vol:
                raise ServiceError("bad_volume", "处理进料体积/损耗不合法")
            if in_vol > self._batch_available(parent) + VOL_EPS:
                raise ServiceError("volume_not_conserved", "进料体积超过批次可用体积", 409)
            out_vol = in_vol - loss
            dest = cmd.get("dest_tank")
            at = parse_ts(cmd.get("occurred_at"), self.config.get("site", {}).get("timezone", "Asia/Shanghai"))
            params = cmd.get("process_params") or dict(self.units[unit_id].get("params", {}))
            out_id = self._next_id("B")
            deltas = {dest: out_vol}
            deltas[parent["tank"]] = deltas.get(parent["tank"], 0.0) - in_vol
            self._check_tank_deltas(deltas)
            self._emit(
                "water_processed",
                {
                    "in_batch_id": parent["id"],
                    "out_batch_id": out_id,
                    "unit": unit_id,
                    "bypass": bypass,
                    "process_params": params,
                    "input_volume_m3": in_vol,
                    "loss_m3": loss,
                    "output_volume_m3": out_vol,
                    "src_tank": parent["tank"],
                    "dest_tank": dest,
                    "occurred_at": iso(at),
                },
                idem_key=cmd.get("idempotency_key"),
            )
            return self.batches[out_id]

    # -------------------------------------------------------------- 命令：拆分

    def split_batch(self, cmd: dict) -> list[dict]:
        with self._lock:
            parent = self._need_batch(cmd["batch_id"])
            outputs = cmd.get("outputs", [])
            if len(outputs) < 2:
                raise ServiceError("bad_split", "拆分至少产生两个子批次")
            total = 0.0
            norm = []
            for o in outputs:
                v = float(o["volume_m3"])
                if v <= 0:
                    raise ServiceError("bad_volume", "拆分体积必须为正")
                if o.get("tank") not in self.tank_capacity:
                    raise ServiceError("tank_not_found", f"罐 {o.get('tank')} 不在罐区清单", 404)
                total += v
                norm.append((v, o["tank"]))
            available = self._batch_available(parent)
            if abs(total - available) > VOL_EPS:
                raise ServiceError(
                    "volume_not_conserved",
                    f"拆分体积之和 {total:.3f} 必须等于批次可用体积 {available:.3f}（体积守恒）",
                    409,
                )
            deltas: dict[str, float] = {parent["tank"]: -total}
            for v, tank in norm:
                deltas[tank] = deltas.get(tank, 0.0) + v
            self._check_tank_deltas(deltas)
            at = parse_ts(cmd.get("occurred_at"), self.config.get("site", {}).get("timezone", "Asia/Shanghai"))
            child_ids = []
            payload_outputs = []
            for v, tank in norm:
                cid = self._next_id("B")
                child_ids.append(cid)
                payload_outputs.append({"batch_id": cid, "volume_m3": v, "tank": tank})
            self._emit(
                "batch_split",
                {
                    "parent_batch_id": parent["id"],
                    "outputs": payload_outputs,
                    "src_tank": parent["tank"],
                    "occurred_at": iso(at),
                },
                idem_key=cmd.get("idempotency_key"),
            )
            return [self.batches[c] for c in child_ids]

    # -------------------------------------------------------------- 命令：混配

    def blend_water(self, cmd: dict) -> dict:
        with self._lock:
            inputs = cmd.get("inputs", [])
            if not inputs:
                raise ServiceError("bad_blend", "混配至少需要一个输入批次")
            norm = []
            total = 0.0
            seen = set()
            for item in inputs:
                bid = item["batch_id"]
                if bid in seen:
                    raise ServiceError("bad_blend", f"混配输入 {bid} 重复")
                seen.add(bid)
                b = self._need_batch(bid)
                v = float(item["volume_m3"])
                if v <= 0:
                    raise ServiceError("bad_volume", "混配体积必须为正")
                if v > self._batch_available(b) + VOL_EPS:
                    raise ServiceError(
                        "volume_not_conserved",
                        f"批次 {bid} 可用体积不足，无法贡献 {v:.3f} m³",
                        409,
                    )
                if b["quality"] == "unqualified":
                    raise ServiceError("input_unqualified", f"不合格批次 {bid} 不得混配", 409)
                if b.get("taint", False):
                    raise ServiceError(
                        "input_evidence_compromised",
                        f"批次 {bid} 的检测证据链已污染（结果不合格/撤回未闭环），不得混配",
                        409,
                    )
                norm.append((b, v))
                total += v
            loss = float(cmd.get("loss_m3", 0.0))
            if loss < 0 or loss >= total:
                raise ServiceError("bad_volume", "混配损耗不合法")
            out_vol = total - loss
            dest = cmd.get("dest_tank")
            if dest not in self.tank_capacity:
                raise ServiceError("tank_not_found", f"罐 {dest} 不在罐区清单", 404)
            at = parse_ts(cmd.get("occurred_at"), self.config.get("site", {}).get("timezone", "Asia/Shanghai"))
            out_id = self._next_id("B")
            deltas: dict[str, float] = {dest: out_vol}
            for b, v in norm:
                deltas[b["tank"]] = deltas.get(b["tank"], 0.0) - v
            self._check_tank_deltas(deltas)
            self._emit(
                "water_blended",
                {
                    "inputs": [{"batch_id": b["id"], "volume_m3": v} for b, v in norm],
                    "out_batch_id": out_id,
                    "total_input_m3": total,
                    "loss_m3": loss,
                    "output_volume_m3": out_vol,
                    "dest_tank": dest,
                    "occurred_at": iso(at),
                },
                idem_key=cmd.get("idempotency_key"),
            )
            return self.batches[out_id]

    # -------------------------------------------------------------- 命令：采样

    def register_sample(self, cmd: dict) -> dict:
        with self._lock:
            batch = self._need_batch(cmd["batch_id"])
            kind = cmd.get("kind", "release")
            if kind not in ("unit", "bypass", "release"):
                raise ServiceError("bad_sample_kind", "采样类型须为 unit/bypass/release")
            taken_at = parse_ts(cmd.get("taken_at"), self.config.get("site", {}).get("timezone", "Asia/Shanghai"))
            sid = self._next_id("S")
            self._emit(
                "sample_registered",
                {
                    "sample_id": sid,
                    "batch_id": batch["id"],
                    "kind": kind,
                    "taken_at": iso(taken_at),
                    "due_at": iso(taken_at + self.result_due),
                },
                idem_key=cmd.get("idempotency_key"),
            )
            return self.samples[sid]

    def record_result(self, cmd: dict) -> dict:
        """检测结果录入；迟到/推翻既往合格结论都会重新评估下游。"""
        with self._lock:
            sid = cmd["sample_id"]
            sample = self.samples.get(sid)
            if sample is None:
                raise ServiceError("sample_not_found", f"样本 {sid} 不存在", 404)
            if sample["status"] == "withdrawn":
                raise ServiceError("sample_withdrawn", "样本已撤回，不能再录入结果", 409)
            received_at = parse_ts(
                cmd.get("received_at"), self.config.get("site", {}).get("timezone", "Asia/Shanghai")
            )
            conforms = bool(cmd.get("conforms"))
            values = cmd.get("values", {}) or {}
            late = received_at > parse_ts(sample["due_at"])
            version = len(sample["versions"]) + 1
            self._emit(
                "sample_result_recorded",
                {
                    "sample_id": sid,
                    "version": version,
                    "received_at": iso(received_at),
                    "late": late,
                    "conforms": conforms,
                    "values": values,
                    "lab": cmd.get("lab"),
                },
                idem_key=cmd.get("idempotency_key"),
            )
            self._recompute_downstream(sample["batch_id"], received_at)
            return self.samples[sid]

    def withdraw_sample(self, cmd: dict) -> dict:
        with self._lock:
            sid = cmd["sample_id"]
            sample = self.samples.get(sid)
            if sample is None:
                raise ServiceError("sample_not_found", f"样本 {sid} 不存在", 404)
            at = parse_ts(cmd.get("withdrawn_at"), self.config.get("site", {}).get("timezone", "Asia/Shanghai"))
            self._emit(
                "sample_withdrawn",
                {"sample_id": sid, "withdrawn_at": iso(at), "reason": cmd.get("reason", "")},
                idem_key=cmd.get("idempotency_key"),
            )
            self._recompute_downstream(sample["batch_id"], at)
            return self.samples[sid]

    # -------------------------------------------------------------- 命令：放行

    def create_release(self, cmd: dict) -> dict:
        with self._lock:
            batch = self._need_batch(cmd["batch_id"])
            volume = float(cmd.get("volume_m3", 0))
            if volume <= 0:
                raise ServiceError("bad_volume", "放行体积必须为正")
            if volume > self._batch_available(batch) + VOL_EPS:
                raise ServiceError("insufficient_inventory", "成品批次可用库存不足", 409)
            customer_id = cmd.get("customer_id")
            if customer_id not in self.customers:
                raise ServiceError("customer_not_found", f"客户 {customer_id} 不存在", 404)
            vehicle_id = cmd.get("vehicle_id")
            if vehicle_id and vehicle_id not in self.vehicles:
                raise ServiceError("vehicle_not_found", f"车辆 {vehicle_id} 不存在", 404)
            sample = self.samples.get(cmd.get("sample_id", ""))
            if sample is None or sample["batch_id"] != batch["id"] or sample["kind"] != "release":
                raise ServiceError(
                    "release_sample_required",
                    "放行必须指定本成品批次的放行留样（release 样本）",
                    409,
                )
            if sample["status"] == "withdrawn":
                raise ServiceError("sample_withdrawn", "放行留样已撤回，不得放行", 409)
            if batch.get("taint", False):
                raise ServiceError(
                    "evidence_compromised",
                    "批次谱系中存在未闭环的不合格/撤回检测，证据链污染，禁止新放行",
                    409,
                )
            provisional = bool(cmd.get("allow_provisional", False))
            if sample["status"] == "qualified" and batch["quality"] == "qualified":
                basis = "qualified"
            elif sample["status"] == "pending" and provisional and batch["quality"] != "unqualified":
                basis = "provisional"
            elif sample["status"] == "unqualified" or batch["quality"] == "unqualified":
                raise ServiceError("quality_unqualified", "检测不合格，禁止放行", 409)
            else:
                raise ServiceError(
                    "quality_basis_missing",
                    "留样尚未出结果：合格放行需等待结果，或显式 allow_provisional 临时放行",
                    409,
                )
            at = parse_ts(cmd.get("issued_at"), self.config.get("site", {}).get("timezone", "Asia/Shanghai"))
            no = self._next_id("FX")
            self._emit(
                "release_created",
                {
                    "release_no": no,
                    "batch_id": batch["id"],
                    "tank": batch["tank"],
                    "volume_m3": volume,
                    "customer_id": customer_id,
                    "destination": cmd.get("destination"),
                    "vehicle_id": vehicle_id,
                    "sample_id": sample["id"],
                    "basis": basis,
                    "issued_by": cmd.get("issued_by"),
                    "issued_at": iso(at),
                },
                idem_key=cmd.get("idempotency_key"),
            )
            return self.releases[no]

    # ------------------------------------------------------- 命令：装车与交付回调

    def register_dispatch(self, cmd: dict) -> dict:
        with self._lock:
            no = cmd["release_no"]
            release = self.releases.get(no)
            if release is None:
                raise ServiceError("release_not_found", f"放行单 {no} 不存在", 404)
            idem = cmd.get("idempotency_key")
            # 重复回调最先短路：无论放行单此后是否被冻结，都返回原单且不重复扣量
            if idem and idem in self._idem:
                return self.dispatches[self._idem[idem]]
            if release["frozen"]:
                raise ServiceError("release_frozen", f"放行单 {no} 已冻结，禁止装车", 409)
            volume = float(cmd.get("volume_m3", 0))
            if volume <= 0:
                raise ServiceError("bad_volume", "装车体积必须为正")
            remaining, _ = self._release_remaining(release)
            if volume > remaining + VOL_EPS:
                raise ServiceError(
                    "insufficient_inventory",
                    f"放行单 {no} 剩余可装 {remaining:.3f} m³，申请 {volume:.3f} m³",
                    409,
                )
            if volume > self.tank_levels.get(release["tank"], 0.0) + VOL_EPS:
                raise ServiceError(
                    "insufficient_inventory",
                    f"罐 {release['tank']} 当前可用库存 {self.tank_levels.get(release['tank'], 0.0):.3f} m³，"
                    "并发装车总量不得超过可用库存",
                    409,
                )
            vehicle_id = cmd.get("vehicle_id") or release["vehicle_id"]
            if vehicle_id not in self.vehicles:
                raise ServiceError("vehicle_not_found", f"车辆 {vehicle_id} 不存在", 404)
            cap = float(self.vehicles[vehicle_id]["capacity_m3"])
            if volume > cap + VOL_EPS:
                raise ServiceError(
                    "vehicle_overloaded",
                    f"车辆 {vehicle_id} 容量 {cap} m³，无法装载 {volume:.3f} m³",
                    409,
                )
            at = parse_ts(cmd.get("loaded_at"), self.config.get("site", {}).get("timezone", "Asia/Shanghai"))
            did = self._next_id("LC")
            self._emit(
                "dispatch_registered",
                {
                    "dispatch_id": did,
                    "release_no": no,
                    "vehicle_id": vehicle_id,
                    "volume_m3": volume,
                    "loaded_at": iso(at),
                },
                idem_key=idem,
            )
            if idem:
                self._idem[idem] = did
            return self.dispatches[did]

    def confirm_dispatch_callback(self, cmd: dict) -> dict:
        """过磅/客户签收回调。同一装车单重复回调必须幂等，不重复扣量。"""
        with self._lock:
            did = cmd["dispatch_id"]
            dispatch = self.dispatches.get(did)
            if dispatch is None:
                raise ServiceError("dispatch_not_found", f"装车单 {did} 不存在", 404)
            if "volume_m3" in cmd and abs(float(cmd["volume_m3"]) - dispatch["volume"]) > VOL_EPS:
                raise ServiceError(
                    "callback_volume_mismatch",
                    f"回调体积 {cmd['volume_m3']} 与装车登记 {dispatch['volume']} 不一致",
                    409,
                )
            if dispatch["status"] == "confirmed":
                # 幂等重放：原样返回，不再扣量
                return {"dispatch": dispatch, "deduplicated": True}
            if dispatch["status"] == "cancelled":
                raise ServiceError("dispatch_cancelled", f"装车单 {did} 已取消", 409)
            if dispatch["status"] == "frozen":
                raise ServiceError("dispatch_frozen", f"装车单 {did} 已冻结，等待质量处置", 409)
            at = parse_ts(
                cmd.get("confirmed_at"), self.config.get("site", {}).get("timezone", "Asia/Shanghai")
            )
            self._emit(
                "dispatch_confirmed",
                {
                    "dispatch_id": did,
                    "release_no": dispatch["release_no"],
                    "confirmed_at": iso(at),
                    "received_by": cmd.get("received_by"),
                },
                idem_key=cmd.get("idempotency_key"),
            )
            return {"dispatch": self.dispatches[did], "deduplicated": False}

    def cancel_dispatch(self, cmd: dict) -> dict:
        with self._lock:
            did = cmd["dispatch_id"]
            dispatch = self.dispatches.get(did)
            if dispatch is None:
                raise ServiceError("dispatch_not_found", f"装车单 {did} 不存在", 404)
            if dispatch["status"] == "confirmed":
                raise ServiceError("already_delivered", "已交付装车单不能取消，只能追加更正", 409)
            if dispatch["status"] == "cancelled":
                return dispatch
            at = parse_ts(
                cmd.get("cancelled_at"), self.config.get("site", {}).get("timezone", "Asia/Shanghai")
            )
            self._emit(
                "dispatch_cancelled",
                {"dispatch_id": did, "cancelled_at": iso(at), "reason": cmd.get("reason", "")},
                idem_key=cmd.get("idempotency_key"),
            )
            return self.dispatches[did]

    # ------------------------------------------------------- 命令：已交付记录更正

    def append_correction(self, cmd: dict) -> dict:
        with self._lock:
            did = cmd["dispatch_id"]
            dispatch = self.dispatches.get(did)
            if dispatch is None:
                raise ServiceError("dispatch_not_found", f"装车单 {did} 不存在", 404)
            if dispatch["status"] != "confirmed":
                raise ServiceError("not_delivered", "只能对已交付记录追加更正", 409)
            note = (cmd.get("note") or "").strip()
            if not note:
                raise ServiceError("bad_correction", "更正内容不能为空")
            at = parse_ts(cmd.get("occurred_at"), self.config.get("site", {}).get("timezone", "Asia/Shanghai"))
            cid = self._next_id("CR")
            self._emit(
                "delivery_correction_appended",
                {
                    "correction_id": cid,
                    "dispatch_id": did,
                    "release_no": dispatch["release_no"],
                    "note": note,
                    "author": cmd.get("author"),
                    "occurred_at": iso(at),
                },
                idem_key=cmd.get("idempotency_key"),
            )
            return self.corrections[-1]

    # ----------------------------------------------------------- 超时巡检/冻结

    def sweep_overdue(self, now_value: str | None = None) -> dict:
        """结果超过约定时限未出：冻结依赖它的临时放行，生成通知与待复核任务。"""
        with self._lock:
            now = parse_ts(now_value, self.config.get("site", {}).get("timezone", "Asia/Shanghai"))
            frozen_releases: list[str] = []
            for sid, sample in self.samples.items():
                if sample["status"] != "pending" or sid in self._late_marked:
                    continue
                if now <= parse_ts(sample["due_at"]):
                    continue
                self._late_marked.add(sid)
                self._emit("sample_marked_late", {"sample_id": sid, "at": iso(now)})
                for release in self._downstream_releases(sample["batch_id"]):
                    if release["basis"] == "provisional" and not release["frozen"]:
                        self._freeze_release(release, "result_overdue", sid, now)
                        frozen_releases.append(release["release_no"])
            return {"at": iso(now), "frozen_releases": frozen_releases}

    def close_unqualified_finding(self, cmd: dict) -> dict:
        """质量负责人对不合格结论作正式裁定（复检确认/留样异常等），关闭证据污染。

        不合格是事实性结论，补检合格不自动翻案；只有显式裁定关闭后，
        该样本才不再污染谱系。裁定本身作为不可变事件保留。
        """
        with self._lock:
            sid = cmd["sample_id"]
            sample = self.samples.get(sid)
            if sample is None:
                raise ServiceError("sample_not_found", f"样本 {sid} 不存在", 404)
            if sample["status"] != "unqualified":
                raise ServiceError(
                    "not_unqualified", "只能对不合格结论作裁定关闭", 409)
            if sample.get("finding_closed"):
                return sample
            note = (cmd.get("note") or "").strip()
            if not note:
                raise ServiceError("bad_finding_note", "裁定说明不能为空")
            at = parse_ts(cmd.get("closed_at"), self.config.get("site", {}).get("timezone", "Asia/Shanghai"))
            self._emit(
                "sample_finding_closed",
                {"sample_id": sid, "closed_by": cmd.get("closed_by"),
                 "note": note, "closed_at": iso(at)},
                idem_key=cmd.get("idempotency_key"),
            )
            self._recompute_downstream(sample["batch_id"], at)
            return sample

    def resolve_review_task(self, cmd: dict) -> dict:
        with self._lock:
            tid = cmd["task_id"]
            task = self.tasks.get(tid)
            if task is None:
                raise ServiceError("task_not_found", f"待复核任务 {tid} 不存在", 404)
            if task["status"] != "open":
                return task
            at = parse_ts(cmd.get("resolved_at"), self.config.get("site", {}).get("timezone", "Asia/Shanghai"))
            lift = bool(cmd.get("lift_freeze", False))
            lifted: list[str] = []
            if lift:
                for no in task["releases"]:
                    release = self.releases[no]
                    bound = self.samples.get(release["sample_id"])
                    # 放行单绑定的留样本身撤回/不合格：合格依据灭失，只能作废重开
                    if bound and bound["status"] in ("withdrawn", "unqualified"):
                        raise ServiceError(
                            "freeze_must_hold",
                            f"放行单 {no} 绑定留样已{bound['status']}，合格依据灭失，"
                            "不得解冻，应作废并重新留样放行",
                            409,
                        )
                    batch = self.batches[release["batch_id"]]
                    if batch.get("taint", False) or batch["quality"] == "unqualified":
                        raise ServiceError(
                            "freeze_must_hold",
                            f"放行单 {no} 的证据污染或不合格结论尚未消除，不能解除冻结",
                            409,
                        )
                for no in task["releases"]:
                    release = self.releases[no]
                    if release["frozen"]:
                        self._emit(
                            "release_unfrozen",
                            {"release_no": no, "task_id": tid, "at": iso(at)},
                        )
                        lifted.append(no)
            self._emit(
                "review_task_resolved",
                {
                    "task_id": tid,
                    "resolved_by": cmd.get("resolved_by"),
                    "note": cmd.get("note", ""),
                    "lift_freeze": lift,
                    "lifted_releases": lifted,
                    "resolved_at": iso(at),
                },
                idem_key=cmd.get("idempotency_key"),
            )
            return self.tasks[tid]

    # ----------------------------------------------------------- 质量传播内部逻辑

    def _downstream_releases(self, batch_id: str) -> list[dict]:
        """从某批次沿谱系向下，找到全部受影响放行单。"""
        seen: set[str] = set()
        stack = [batch_id]
        reached: set[str] = set()
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            reached.add(cur)
            stack.extend(self.batches[cur].get("children", []))
        return [r for r in self.releases.values() if r["batch_id"] in reached]

    def _recompute_batch_quality(self, batch_id: str) -> None:
        b = self.batches[batch_id]
        # ---- 证据污染（taint）：本批次样本不合格/撤回，或任一父批次被污染 ----
        # 污染沿整条谱系向下游传播，不被下游自己的合格检测掩盖（证据链断裂须冻结复核）。
        # 撤回（结果作废）：该批次补采并取得更晚的合格样本即解除；
        # 不合格（超标事实）：只能由质量负责人在复核任务中显式裁定关闭才能解除。
        local_taint = False
        for sid in b.get("sample_ids", []):
            s = self.samples[sid]
            cleared = False
            if s["status"] == "withdrawn":
                decided_at = s.get("withdrawn_at")
                cleared = any(
                    self.samples[q]["status"] == "qualified"
                    and self.samples[q]["versions"]
                    and self.samples[q]["versions"][-1]["received_at"] > decided_at
                    for q in b["sample_ids"]
                )
                if not cleared:
                    local_taint = True
            elif s["status"] == "unqualified":
                decided_at = s["versions"][-1]["received_at"]
                closed = s.get("finding_closed") and (s.get("finding_closed_at") or "") > decided_at
                if not closed:
                    local_taint = True
        if b["parents"]:
            inherited_taint = any(self.batches[pid].get("taint", False) for pid, _ in b["parents"])
        else:
            inherited_taint = False
        b["taint"] = local_taint or inherited_taint

        # ---- 物理质量状态（供放行走合格/临时依据判断） ----
        # 撤回视为结果作废、不参与判定；不合格硬结论除非经质量裁定关闭，否则保持最严档；
        # 旁路水不继承上游合格状态，只能凭本批次留样合格转正。
        own = [self.samples[s] for s in b.get("sample_ids", [])]
        effective = [
            s for s in own
            if not (
                s["status"] == "withdrawn"
                or (s["status"] == "unqualified" and s.get("finding_closed"))
            )
        ]
        if any(s["status"] == "unqualified" for s in effective):
            b["quality"] = "unqualified"
            return
        if effective and all(s["status"] == "qualified" for s in effective):
            b["quality"] = "qualified"
            return
        # 2) 沿父边继承
        if not b["parents"]:
            inherited = "unknown"
        elif b["kind"] == "processed" and b.get("bypass"):
            # 旁路：绝不自动继承上游合格状态
            inherited = "unknown"
        elif b["kind"] == "blend":
            # 混配按最差父批次质量计
            inherited = max(
                (self.batches[pid]["quality"] for pid, _ in b["parents"]),
                key=lambda q: _Q_RANK[q],
            )
        else:  # processed（非旁路）/ split
            inherited = self.batches[b["parents"][0][0]]["quality"]
        if any(s["status"] == "pending" for s in effective):
            inherited = max(inherited, "provisional", key=lambda q: _Q_RANK[q])
        b["quality"] = inherited

    def _recompute_downstream(self, start_batch: str, at: datetime) -> None:
        """样本结论变化后，自顶向下重算质量，并冻结/转正相关放行。"""
        order: list[str] = []
        seen: set[str] = set()
        stack = [start_batch]
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            order.append(cur)
            stack.extend(self.batches[cur].get("children", []))
        for bid in order:
            self._recompute_batch_quality(bid)

        affected = set(order)
        for release in self.releases.values():
            if release["batch_id"] not in affected:
                continue
            sample = self.samples.get(release["sample_id"])
            batch = self.batches[release["batch_id"]]
            own_bad = (
                batch["quality"] == "unqualified"
                or (sample and sample["status"] in ("unqualified", "withdrawn"))
            )
            # 上游检测不合格/撤回使证据链污染：即使本批次留样合格也要冻结复核
            tainted = batch.get("taint", False)
            if own_bad:
                self._ensure_frozen(
                    release, "quality_unqualified",
                    sample["id"] if sample else None, at)
                continue
            if tainted:
                taint_sample = self._nearest_taint_source(release["batch_id"])
                self._ensure_frozen(release, "evidence_compromised", taint_sample, at)
                continue
            # 迟到结果合格/批次质量恢复：解除此前因结果超时产生的冻结；
            # 质量/证据问题冻结只能由质量流程处置，不自动解除
            if (
                release["frozen"]
                and release["freeze_reason"] == "result_overdue"
                and self.batches[release["batch_id"]]["quality"] in ("qualified", "provisional")
            ):
                self._emit("release_unfrozen", {"release_no": release["release_no"], "at": iso(at)})
            if (
                release["basis"] == "provisional"
                and not release["frozen"]
                and sample
                and sample["status"] == "qualified"
                and self.batches[release["batch_id"]]["quality"] == "qualified"
            ):
                self._emit(
                    "release_basis_changed",
                    {"release_no": release["release_no"], "basis": "qualified", "at": iso(at)},
                )

    def _nearest_taint_source(self, batch_id: str) -> str | None:
        """沿父边找到最近的、自身样本不合格/撤回的批次样本。"""
        seen: set[str] = set()
        stack = [batch_id]
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            b = self.batches[cur]
            for sid in b.get("sample_ids", []):
                if self.samples[sid]["status"] in ("unqualified", "withdrawn"):
                    return sid
            stack.extend(pid for pid, _ in b["parents"])
        return None

    _FREEZE_SEVERITY = {"result_overdue": 1, "evidence_compromised": 2, "quality_unqualified": 3}

    def _ensure_frozen(self, release: dict, reason: str, sample_id: str | None, at: datetime) -> None:
        """冻结或升级冻结原因（超时 → 证据污染 → 不合格）。"""
        if not release["frozen"]:
            self._freeze_release(release, reason, sample_id, at)
            return
        if self._FREEZE_SEVERITY[reason] > self._FREEZE_SEVERITY.get(release["freeze_reason"], 0):
            no = release["release_no"]
            self._emit(
                "release_freeze_escalated",
                {"release_no": no, "from_reason": release["freeze_reason"],
                 "to_reason": reason, "sample_id": sample_id, "at": iso(at)},
            )
            delivered = [
                did for did in release["dispatches"]
                if self.dispatches[did]["status"] == "confirmed"
            ]
            frozen_dispatches = [
                did for did in release["dispatches"]
                if self.dispatches[did]["status"] in ("loaded", "frozen")
            ]
            self._notify(
                audience={"type": "role", "role": "质量人员"},
                severity="critical",
                title=f"放行单 {no} 冻结升级为 {reason}",
                detail={"release_no": no, "reason": reason, "sample_id": sample_id,
                        "frozen_dispatches": frozen_dispatches, "delivered_dispatches": delivered},
                at=at,
            )
            self._upsert_review_task(sample_id, no, frozen_dispatches + delivered, reason, at)

    def _freeze_release(self, release: dict, reason: str, sample_id: str | None, at: datetime) -> None:
        no = release["release_no"]
        self._emit(
            "release_frozen",
            {"release_no": no, "reason": reason, "sample_id": sample_id, "at": iso(at)},
        )
        frozen_dispatches: list[str] = []
        delivered: list[str] = []
        for did in release["dispatches"]:
            d = self.dispatches[did]
            if d["status"] == "loaded":
                self._emit("dispatch_frozen", {"dispatch_id": did, "at": iso(at)})
                frozen_dispatches.append(did)
            elif d["status"] == "confirmed":
                delivered.append(did)
        self._notify(
            audience={"type": "role", "role": "质量人员"},
            severity="critical",
            title=f"放行单 {no} 已冻结（{reason}）",
            detail={"release_no": no, "reason": reason, "sample_id": sample_id,
                    "frozen_dispatches": frozen_dispatches, "delivered_dispatches": delivered},
            at=at,
        )
        for did in delivered:
            d = self.dispatches[did]
            self._notify(
                audience={"type": "customer", "customer_id": release["customer_id"]},
                severity="critical",
                title=f"交付 {did} 对应水质需复核，请暂停使用",
                detail={"dispatch_id": did, "release_no": no, "sample_id": sample_id, "reason": reason},
                at=at,
            )
        self._upsert_review_task(sample_id, no, frozen_dispatches + delivered, reason, at)

    def _upsert_review_task(
        self,
        sample_id: str | None,
        release_no: str,
        dispatch_ids: list[str],
        reason: str,
        at: datetime,
    ) -> None:
        open_task = None
        if sample_id:
            for t in self.tasks.values():
                if t["status"] == "open" and t["sample_id"] == sample_id:
                    open_task = t
                    break
        if open_task is None:
            tid = self._next_id("RV")
            self._emit(
                "review_task_opened",
                {
                    "task_id": tid,
                    "sample_id": sample_id,
                    "releases": [release_no],
                    "dispatches": dispatch_ids,
                    "reason": reason,
                    "opened_at": iso(at),
                },
            )
            return
        new_r = [no for no in [release_no] if no not in open_task["releases"]]
        new_d = [d for d in dispatch_ids if d not in open_task["dispatches"]]
        if new_r or new_d:
            self._emit(
                "review_task_updated",
                {"task_id": open_task["task_id"], "releases": new_r, "dispatches": new_d, "at": iso(at)},
            )

    def _notify(
        self,
        audience: dict,
        severity: str,
        title: str,
        detail: dict,
        at: datetime,
    ) -> None:
        nid = self._next_id("NT")
        self._emit(
            "notification_created",
            {
                "notification_id": nid,
                "audience": audience,
                "severity": severity,
                "title": title,
                "detail": detail,
                "created_at": iso(at),
            },
        )

    # ------------------------------------------------------------------ 重放

    def _apply(self, event: dict) -> None:
        p = event["payload"]
        self._seq = int(event["event_id"].split("-")[-1])
        kind = event["type"]

        if kind == "influent_received":
            bid = p["batch_id"]
            self.batches[bid] = {
                "id": bid,
                "kind": "influent",
                "occurred_at": p["occurred_at"],
                "tank": p["tank"],
                "volume": p["volume_m3"],
                "allocated": 0.0,
                "parents": [],
                "children": [],
                "quality": "unknown",
                "taint": False,
                "source": p.get("source"),
                "sample_ids": [],
            }
            self.tank_levels[p["tank"]] = self.tank_levels.get(p["tank"], 0.0) + p["volume_m3"]

        elif kind == "water_processed":
            pid, oid = p["in_batch_id"], p["out_batch_id"]
            parent = self.batches[pid]
            parent["allocated"] += p["input_volume_m3"]
            parent["children"].append(oid)
            self.batches[oid] = {
                "id": oid,
                "kind": "processed",
                "occurred_at": p["occurred_at"],
                "tank": p["dest_tank"],
                "volume": p["output_volume_m3"],
                "allocated": 0.0,
                "parents": [[pid, p["input_volume_m3"]]],
                "children": [],
                "unit": p["unit"],
                "bypass": p["bypass"],
                "process_params": p["process_params"],
                "quality": "unknown",
                "taint": False,
                "sample_ids": [],
            }
            self.tank_levels[parent["tank"]] -= p["input_volume_m3"]
            self.tank_levels[p["dest_tank"]] = self.tank_levels.get(p["dest_tank"], 0.0) + p["output_volume_m3"]

        elif kind == "batch_split":
            parent = self.batches[p["parent_batch_id"]]
            total = sum(o["volume_m3"] for o in p["outputs"])
            parent["allocated"] += total
            for o in p["outputs"]:
                cid = o["batch_id"]
                parent["children"].append(cid)
                self.batches[cid] = {
                    "id": cid,
                    "kind": "split",
                    "occurred_at": p["occurred_at"],
                    "tank": o["tank"],
                    "volume": o["volume_m3"],
                    "allocated": 0.0,
                    "parents": [[parent["id"], o["volume_m3"]]],
                    "children": [],
                    "quality": parent["quality"],
                    "taint": False,
                    "sample_ids": [],
                }
                self.tank_levels[parent["tank"]] -= o["volume_m3"]
                self.tank_levels[o["tank"]] = self.tank_levels.get(o["tank"], 0.0) + o["volume_m3"]

        elif kind == "water_blended":
            out_id = p["out_batch_id"]
            for item in p["inputs"]:
                src = self.batches[item["batch_id"]]
                src["allocated"] += item["volume_m3"]
                src["children"].append(out_id)
            self.batches[out_id] = {
                "id": out_id,
                "kind": "blend",
                "occurred_at": p["occurred_at"],
                "tank": p["dest_tank"],
                "volume": p["output_volume_m3"],
                "allocated": 0.0,
                "parents": [[i["batch_id"], i["volume_m3"]] for i in p["inputs"]],
                "children": [],
                "quality": "unknown",
                "taint": False,
                "sample_ids": [],
            }
            for item in p["inputs"]:
                self.tank_levels[self.batches[item["batch_id"]]["tank"]] -= item["volume_m3"]
            self.tank_levels[p["dest_tank"]] = self.tank_levels.get(p["dest_tank"], 0.0) + p["output_volume_m3"]
            self._recompute_batch_quality(out_id)

        elif kind == "sample_registered":
            sid = p["sample_id"]
            self.samples[sid] = {
                "id": sid,
                "batch_id": p["batch_id"],
                "kind": p["kind"],
                "taken_at": p["taken_at"],
                "due_at": p["due_at"],
                "status": "pending",
                "late": False,
                "versions": [],
            }
            self.batches[p["batch_id"]]["sample_ids"].append(sid)

        elif kind == "sample_result_recorded":
            sample = self.samples[p["sample_id"]]
            sample["versions"].append(
                {
                    "version": p["version"],
                    "received_at": p["received_at"],
                    "late": p["late"],
                    "conforms": p["conforms"],
                    "values": p["values"],
                    "lab": p.get("lab"),
                }
            )
            sample["status"] = "qualified" if p["conforms"] else "unqualified"
            batch = self.batches[sample["batch_id"]]
            self._recompute_batch_quality(batch["id"])

        elif kind == "sample_withdrawn":
            sample = self.samples[p["sample_id"]]
            sample["status"] = "withdrawn"
            sample["withdrawn_at"] = p["withdrawn_at"]
            sample["withdrawn_reason"] = p["reason"]
            self._recompute_batch_quality(sample["batch_id"])

        elif kind == "sample_marked_late":
            self._late_marked.add(p["sample_id"])
            self.samples[p["sample_id"]]["late"] = True

        elif kind == "sample_finding_closed":
            s = self.samples[p["sample_id"]]
            s["finding_closed"] = True
            s["finding_closed_at"] = p["closed_at"]
            s["finding_closed_by"] = p.get("closed_by")
            s["finding_closed_note"] = p.get("note")
            self._recompute_batch_quality(s["batch_id"])

        elif kind == "release_created":
            no = p["release_no"]
            self.releases[no] = {
                "release_no": no,
                "batch_id": p["batch_id"],
                "tank": p["tank"],
                "volume": p["volume_m3"],
                "customer_id": p["customer_id"],
                "destination": p.get("destination"),
                "vehicle_id": p.get("vehicle_id"),
                "sample_id": p["sample_id"],
                "basis": p["basis"],
                "issued_by": p.get("issued_by"),
                "issued_at": p["issued_at"],
                "frozen": False,
                "freeze_reason": None,
                "dispatches": [],
            }

        elif kind == "release_frozen":
            r = self.releases[p["release_no"]]
            r["frozen"] = True
            r["freeze_reason"] = p["reason"]

        elif kind == "release_freeze_escalated":
            r = self.releases[p["release_no"]]
            r["frozen"] = True
            r["freeze_reason"] = p["to_reason"]

        elif kind == "release_unfrozen":
            r = self.releases[p["release_no"]]
            r["frozen"] = False
            r["freeze_reason"] = None
            for did in r["dispatches"]:
                if self.dispatches[did]["status"] == "frozen":
                    self.dispatches[did]["status"] = "loaded"

        elif kind == "release_basis_changed":
            self.releases[p["release_no"]]["basis"] = p["basis"]

        elif kind == "dispatch_registered":
            did = p["dispatch_id"]
            self.dispatches[did] = {
                "id": did,
                "release_no": p["release_no"],
                "vehicle_id": p["vehicle_id"],
                "volume": p["volume_m3"],
                "status": "loaded",
                "loaded_at": p["loaded_at"],
                "confirmed_at": None,
            }
            self.releases[p["release_no"]]["dispatches"].append(did)
            # 装车出库即扣减罐存（冻结/取消回补），回调确认不再重复扣量
            self.tank_levels[self.releases[p["release_no"]]["tank"]] -= p["volume_m3"]

        elif kind == "dispatch_confirmed":
            d = self.dispatches[p["dispatch_id"]]
            d["status"] = "confirmed"
            d["confirmed_at"] = p["confirmed_at"]
            d["received_by"] = p.get("received_by")

        elif kind == "dispatch_cancelled":
            d = self.dispatches[p["dispatch_id"]]
            d["status"] = "cancelled"
            self.tank_levels[self.releases[d["release_no"]]["tank"]] += d["volume"]

        elif kind == "dispatch_frozen":
            self.dispatches[p["dispatch_id"]]["status"] = "frozen"

        elif kind == "notification_created":
            self.notifications.append(p)

        elif kind == "review_task_opened":
            self.tasks[p["task_id"]] = {
                "task_id": p["task_id"],
                "sample_id": p["sample_id"],
                "releases": list(p["releases"]),
                "dispatches": list(p["dispatches"]),
                "reason": p["reason"],
                "status": "open",
                "opened_at": p["opened_at"],
                "resolved_at": None,
                "resolution": None,
            }

        elif kind == "review_task_updated":
            t = self.tasks[p["task_id"]]
            t["releases"].extend(p.get("releases", []))
            t["dispatches"].extend(p.get("dispatches", []))

        elif kind == "review_task_resolved":
            t = self.tasks[p["task_id"]]
            t["status"] = "resolved"
            t["resolved_at"] = p["resolved_at"]
            t["resolution"] = {"by": p.get("resolved_by"), "note": p.get("note", ""),
                               "lift_freeze": p.get("lift_freeze", False)}

        elif kind == "delivery_correction_appended":
            self.corrections.append(p)

        else:
            raise ServiceError("unknown_event", f"无法重放未知事件 {kind}")

    # ------------------------------------------------------------------ 查询

    def batch_lineage(self, batch_id: str) -> dict:
        with self._lock:
            self._need_batch(batch_id)

            def ancestors(bid: str) -> list[dict]:
                b = self.batches[bid]
                return [
                    {
                        "batch_id": pid,
                        "contribution_m3": vol,
                        "unit": self.batches[pid].get("unit"),
                        "bypass": self.batches[pid].get("bypass", False),
                        "kind": self.batches[pid]["kind"],
                        "parents": ancestors(pid),
                    }
                    for pid, vol in b["parents"]
                ]

            def descendants(bid: str) -> list[dict]:
                b = self.batches[bid]
                out = []
                for cid in b.get("children", []):
                    c = self.batches[cid]
                    contrib = next((v for p, v in c["parents"] if p == bid), None)
                    out.append({
                        "batch_id": cid,
                        "kind": c["kind"],
                        "contribution_m3": contrib,
                        "unit": c.get("unit"),
                        "bypass": c.get("bypass", False),
                        "quality": c["quality"],
                        "children": descendants(cid),
                        "releases": [
                            self._release_view(r["release_no"])
                            for r in self.releases.values() if r["batch_id"] == cid
                        ],
                    })
                return out

            b = self.batches[batch_id]
            return {
                "batch_id": batch_id,
                "kind": b["kind"],
                "quality": b["quality"],
                "taint": b.get("taint", False),
                "tank": b["tank"],
                "volume_m3": b["volume"],
                "available_m3": self._batch_available(b),
                "unit": b.get("unit"),
                "bypass": b.get("bypass", False),
                "process_params": b.get("process_params"),
                "samples": [self._sample_view(s) for s in b["sample_ids"]],
                "ancestors": ancestors(batch_id),
                "descendants": descendants(batch_id),
                "releases": [
                    self._release_view(r["release_no"])
                    for r in self.releases.values() if r["batch_id"] == batch_id
                ],
            }

    def sample_impact(self, sample_id: str) -> dict:
        """质量人员反查：某个检测样本影响到的全部批次/放行/交付。"""
        with self._lock:
            sample = self.samples.get(sample_id)
            if sample is None:
                raise ServiceError("sample_not_found", f"样本 {sample_id} 不存在", 404)
            releases = self._downstream_releases(sample["batch_id"])
            dispatch_ids = [did for r in releases for did in r["dispatches"]]
            open_task = next(
                (t["task_id"] for t in self.tasks.values()
                 if t["status"] == "open" and t["sample_id"] == sample_id),
                None,
            )
            return {
                "sample": self._sample_view(sample_id),
                "affected_batches": self._affected_batches(sample["batch_id"]),
                "affected_releases": [self._release_view(r["release_no"]) for r in releases],
                "affected_deliveries": [
                    self._delivery_view(self.dispatches[did])
                    for did in dispatch_ids if self.dispatches[did]["status"] == "confirmed"
                ],
                "frozen_pending_dispatches": [
                    did for did in dispatch_ids if self.dispatches[did]["status"] == "frozen"
                ],
                "open_review_task": open_task,
            }

    def _affected_batches(self, start: str) -> list[str]:
        seen, stack, out = set(), [start], []
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            out.append(cur)
            stack.extend(self.batches[cur].get("children", []))
        return out

    def _sample_view(self, sid: str | dict) -> dict:
        s = sid if isinstance(sid, dict) else self.samples[sid]
        return {
            "sample_id": s["id"],
            "batch_id": s["batch_id"],
            "kind": s["kind"],
            "taken_at": s["taken_at"],
            "due_at": s["due_at"],
            "status": s["status"],
            "late": s["late"],
            "finding_closed": s.get("finding_closed", False),
            "finding_closed_at": s.get("finding_closed_at"),
            "finding_closed_note": s.get("finding_closed_note"),
            "versions": s["versions"],
        }

    def _release_view(self, no: str) -> dict:
        r = self.releases[no]
        remaining, committed = self._release_remaining(r)
        return {
            "release_no": no,
            "batch_id": r["batch_id"],
            "tank": r["tank"],
            "volume_m3": r["volume"],
            "remaining_m3": remaining,
            "committed_m3": committed,
            "customer_id": r["customer_id"],
            "destination": r["destination"],
            "vehicle_id": r["vehicle_id"],
            "sample_id": r["sample_id"],
            "basis": r["basis"],
            "frozen": r["frozen"],
            "freeze_reason": r["freeze_reason"],
            "issued_at": r["issued_at"],
            "dispatches": list(r["dispatches"]),
        }

    def _delivery_view(self, d: dict, customer_scoped: bool = False) -> dict:
        r = self.releases[d["release_no"]]
        corrections = [c for c in self.corrections if c["dispatch_id"] == d["id"]]
        notices = [
            n for n in self.notifications
            if n["audience"].get("type") == "customer"
            and n["audience"].get("customer_id") == r["customer_id"]
            and n["detail"].get("dispatch_id") == d["id"]
        ]
        view = {
            "dispatch_id": d["id"],
            "release_no": d["release_no"],
            "batch_id": r["batch_id"],
            "volume_m3": d["volume"],
            "vehicle_id": d["vehicle_id"],
            "loaded_at": d["loaded_at"],
            "confirmed_at": d["confirmed_at"],
            "destination": r["destination"],
            "status": d["status"],
            "corrections": corrections,
            "quality_notices": [
                {"notification_id": n["notification_id"], "title": n["title"],
                 "severity": n["severity"], "created_at": n["created_at"], "detail": n["detail"]}
                for n in notices
            ],
        }
        if not customer_scoped:
            view["customer_id"] = r["customer_id"]
        return view

    def customer_deliveries(self, customer_id: str) -> list[dict]:
        with self._lock:
            return [
                self._delivery_view(d, customer_scoped=True)
                for r in self.releases.values()
                if r["customer_id"] == customer_id
                for d in (self.dispatches[did] for did in r["dispatches"])
                if d["status"] == "confirmed"
            ]

    def notifications_for(self, principal: dict) -> list[dict]:
        with self._lock:
            role = principal["role"]
            cid = principal.get("customer_id")
            out = []
            for n in self.notifications:
                aud = n["audience"]
                if aud["type"] == "customer":
                    if cid is not None and aud["customer_id"] == cid:
                        out.append(n)
                elif role in ("质量人员", "监管人员") or aud.get("role") == role:
                    out.append(n)
            return out

    def tanks_view(self) -> dict:
        with self._lock:
            return {
                t: {"level_m3": self.tank_levels.get(t, 0.0), "capacity_m3": cap}
                for t, cap in self.tank_capacity.items()
            }

    def open_tasks(self) -> list[dict]:
        with self._lock:
            return [t for t in self.tasks.values() if t["status"] == "open"]
