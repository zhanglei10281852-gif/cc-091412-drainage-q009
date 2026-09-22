import json
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from app import create_server  # noqa: E402
from tracing import EventStore, TracingService  # noqa: E402

TZ = ZoneInfo("Asia/Shanghai")
CONFIG_PATH = str(Path(__file__).resolve().parents[1] / "reference" / "site-data.json")
with open(CONFIG_PATH, encoding="utf-8") as fh:
    CONFIG = json.load(fh)

TOKENS = {
    "dispatcher": "token-dispatcher-001",
    "operator": "token-operator-001",
    "quality": "token-quality-001",
    "regulator": "token-regulator-001",
    "c01": "token-customer-c01",
    "c02": "token-customer-c02",
}


def t(hour: int, day: int = 1, minute: int = 0) -> str:
    return datetime(2026, 9, day, hour, minute, tzinfo=TZ).isoformat()


def make_service() -> tuple[TracingService, str]:
    tmp = tempfile.mkdtemp()
    return TracingService(EventStore(tmp), CONFIG), tmp


class ServiceTestCase(unittest.TestCase):
    def assert_error_code(self, code: str, fn, *args, **kwargs):
        try:
            fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            self.assertEqual(getattr(exc, "code", None), code, str(exc))
            return exc
        self.fail(f"预期错误码 {code}，但调用成功")

    def use_service(self) -> TracingService:
        svc, _ = make_service()
        self.addCleanup(svc.store.close)
        return svc


class LineageTest(ServiceTestCase):
    """进水→处理（含旁路）→两次混配→放行→交付 的完整谱系与体积守恒。"""

    def setUp(self):
        self.svc, _ = make_service()
        self.addCleanup(self.svc.store.close)

    def _qualified_product(self):
        inf = self.svc.receive_influent(
            {"tank": "T-101", "volume_m3": 100, "source": "市政管网来水", "occurred_at": t(8)})
        uf = self.svc.process_water(
            {"batch_id": inf["id"], "unit": "UF", "dest_tank": "T-201",
             "input_volume_m3": 100, "loss_m3": 5, "occurred_at": t(9)})
        ro = self.svc.process_water(
            {"batch_id": uf["id"], "unit": "RO", "dest_tank": "T-202",
             "input_volume_m3": 95, "loss_m3": 20, "occurred_at": t(10)})
        cl = self.svc.process_water(
            {"batch_id": ro["id"], "unit": "UV-CL2", "dest_tank": "T-301",
             "input_volume_m3": 75, "loss_m3": 0, "occurred_at": t(11)})
        sample = self.svc.register_sample(
            {"batch_id": cl["id"], "kind": "release", "taken_at": t(11, minute=30)})
        self.svc.record_result(
            {"sample_id": sample["id"], "conforms": True,
             "values": {"cod_mg_l": 18}, "received_at": t(15)})
        return cl

    def test_full_lineage_with_two_blends(self):
        product = self._qualified_product()
        # 再来一路合格水
        inf2 = self.svc.receive_influent(
            {"tank": "T-101", "volume_m3": 50, "source": "市政管网来水", "occurred_at": t(12)})
        uf2 = self.svc.process_water(
            {"batch_id": inf2["id"], "unit": "UF", "dest_tank": "T-201",
             "input_volume_m3": 50, "loss_m3": 2, "occurred_at": t(12, minute=30)})
        ro2 = self.svc.process_water(
            {"batch_id": uf2["id"], "unit": "RO", "dest_tank": "T-202",
             "input_volume_m3": 48, "loss_m3": 10, "occurred_at": t(13)})
        cl2 = self.svc.process_water(
            {"batch_id": ro2["id"], "unit": "UV-CL2", "dest_tank": "T-302",
             "input_volume_m3": 38, "loss_m3": 0, "occurred_at": t(13, minute=30)})
        s2 = self.svc.register_sample({"batch_id": cl2["id"], "kind": "release", "taken_at": t(14)})
        self.svc.record_result(
            {"sample_id": s2["id"], "conforms": True, "values": {"cod_mg_l": 20},
             "received_at": t(16)})

        # 第一次混配
        blend1 = self.svc.blend_water({
            "inputs": [{"batch_id": product["id"], "volume_m3": 30},
                       {"batch_id": cl2["id"], "volume_m3": 10}],
            "dest_tank": "T-302", "loss_m3": 0, "occurred_at": t(17)})
        self.assertEqual(blend1["volume"], 40.0)
        # 第二次混配：把第一次混配水再与剩余成品混
        blend2 = self.svc.blend_water({
            "inputs": [{"batch_id": blend1["id"], "volume_m3": 40},
                       {"batch_id": product["id"], "volume_m3": 20}],
            "dest_tank": "T-301", "loss_m3": 2, "occurred_at": t(18)})
        self.assertEqual(blend2["volume"], 58.0)
        self.assertEqual(blend2["quality"], "qualified")

        lineage = self.svc.batch_lineage(blend2["id"])
        flat = []
        def walk(nodes, depth=0):
            for n in nodes:
                flat.append((depth, n["batch_id"]))
                walk(n["parents"], depth + 1)
        walk(lineage["ancestors"])
        # 两个进水祖先都可追溯
        self.assertEqual(len({b for _, b in flat if self.svc.batches[b]["kind"] == "influent"}), 2)
        # 祖先树中有第一次混配，加上查询节点自身（第二次混配）共两次
        self.assertEqual(
            sum(1 for _, b in flat if self.svc.batches[b]["kind"] == "blend"), 1)
        self.assertEqual(self.svc.batches[blend2["id"]]["kind"], "blend")

    def test_volume_conservation_split_and_blend(self):
        product = self._qualified_product()
        children = self.svc.split_batch({
            "batch_id": product["id"],
            "outputs": [{"volume_m3": 40, "tank": "T-301"},
                        {"volume_m3": 35, "tank": "T-302"}],
            "occurred_at": t(16)})
        self.assertEqual(sum(c["volume"] for c in children), 75.0)
        # 子拆分只覆盖部分体积，违反体积守恒
        self.assert_error_code(
            "volume_not_conserved",
            self.svc.split_batch,
            {"batch_id": children[0]["id"],
             "outputs": [{"volume_m3": 30, "tank": "T-301"},
                         {"volume_m3": 5, "tank": "T-302"}],
             "occurred_at": t(16, minute=30)})
        self.assert_error_code(
            "volume_not_conserved",
            self.svc.blend_water,
            {"inputs": [{"batch_id": children[0]["id"], "volume_m3": 999}],
             "dest_tank": "T-302", "occurred_at": t(17)})

    def test_bypass_does_not_inherit_quality(self):
        inf = self.svc.receive_influent(
            {"tank": "T-101", "volume_m3": 60, "occurred_at": t(8)})
        uf = self.svc.process_water({
            "batch_id": inf["id"], "unit": "UF", "dest_tank": "T-201",
            "input_volume_m3": 60, "loss_m3": 0, "occurred_at": t(9)})
        s = self.svc.register_sample({"batch_id": uf["id"], "kind": "unit", "taken_at": t(9, minute=30)})
        self.svc.record_result({"sample_id": s["id"], "conforms": True, "received_at": t(12)})
        self.assertEqual(self.svc.batches[uf["id"]]["quality"], "qualified")
        # RO 临时旁路：水从 UF 直接绕过 RO 进成品罐
        bypassed = self.svc.process_water({
            "batch_id": uf["id"], "unit": "RO", "bypass": True, "dest_tank": "T-301",
            "input_volume_m3": 60, "loss_m3": 0, "occurred_at": t(10)})
        self.assertEqual(bypassed["quality"], "unknown", "旁路水不得自动继承上游合格状态")
        # 没有自身合格检测时不能放行
        rs = self.svc.register_sample(
            {"batch_id": bypassed["id"], "kind": "release", "taken_at": t(10, minute=30)})
        self.assert_error_code(
            "quality_basis_missing",
            self.svc.create_release,
            {"batch_id": bypassed["id"], "volume_m3": 60, "customer_id": "C-01",
             "sample_id": rs["id"], "issued_at": t(11)})
        # 旁路水自身检测合格后才转正
        self.svc.record_result({"sample_id": rs["id"], "conforms": True, "received_at": t(14)})
        self.assertEqual(self.svc.batches[bypassed["id"]]["quality"], "qualified")
        release = self.svc.create_release({
            "batch_id": bypassed["id"], "volume_m3": 60, "customer_id": "C-01",
            "sample_id": rs["id"], "issued_at": t(15)})
        self.assertEqual(release["basis"], "qualified")


class FreezeAndCorrectionTest(ServiceTestCase):
    def setUp(self):
        self.svc, _ = make_service()
        self.addCleanup(self.svc.store.close)
        inf = self.svc.receive_influent(
            {"tank": "T-101", "volume_m3": 80, "occurred_at": t(8)})
        uf = self.svc.process_water({
            "batch_id": inf["id"], "unit": "UF", "dest_tank": "T-301",
            "input_volume_m3": 80, "loss_m3": 0, "occurred_at": t(9)})
        self.batch = uf
        self.sample = self.svc.register_sample(
            {"batch_id": uf["id"], "kind": "release", "taken_at": t(9, minute=30)})

    def test_late_result_unqualified_freezes_and_corrections_append_only(self):
        # 结果未出，先临时放行并发车、客户签收（已交付）
        release = self.svc.create_release({
            "batch_id": self.batch["id"], "volume_m3": 50, "customer_id": "C-01",
            "vehicle_id": "京A-RW001", "sample_id": self.sample["id"],
            "allow_provisional": True, "issued_at": t(10)})
        self.assertEqual(release["basis"], "provisional")
        dispatch = self.svc.register_dispatch({
            "release_no": release["release_no"], "vehicle_id": "京A-RW001",
            "volume_m3": 30, "loaded_at": t(10, minute=30),
            "idempotency_key": "load-1"})
        self.svc.confirm_dispatch_callback({
            "dispatch_id": dispatch["id"], "confirmed_at": t(12),
            "received_by": "客户门卫"})
        # 迟到的不合格结果（采样时限 24h，次日 10:00 才收到）
        result = self.svc.record_result({
            "sample_id": self.sample["id"], "conforms": False,
            "values": {"fecal_coliform_per_l": 9999}, "received_at": t(10, day=2)})
        self.assertTrue(result["versions"][0]["late"])
        r = self.svc.releases[release["release_no"]]
        self.assertTrue(r["frozen"])
        self.assertEqual(r["freeze_reason"], "quality_unqualified")
        # 已交付装车单保持 confirmed（记录不可变），但有客户质量通告
        self.assertEqual(self.svc.dispatches[dispatch["id"]]["status"], "confirmed")
        impact = self.svc.sample_impact(self.sample["id"])
        self.assertIn(dispatch["id"], [d["dispatch_id"] for d in impact["affected_deliveries"]])
        notices = self.svc.notifications_for(
            {"role": "客户", "customer_id": "C-01"})
        self.assertTrue(any("暂停使用" in n["title"] for n in notices))
        # C-02 看不到 C-01 的通告
        self.assertEqual(
            self.svc.notifications_for({"role": "客户", "customer_id": "C-02"}), [])
        # 待复核任务存在
        tasks = self.svc.open_tasks()
        self.assertEqual(len(tasks), 1)
        # 已交付记录不能取消/覆盖，只能追加更正
        self.assert_error_code(
            "already_delivered", self.svc.cancel_dispatch, {"dispatch_id": dispatch["id"]})
        correction = self.svc.append_correction({
            "dispatch_id": dispatch["id"],
            "note": "追加重测：该车辆水体粪大肠菌群超标，启动召回并安排合格水补换。",
            "author": "质量负责人", "occurred_at": t(11, day=2)})
        deliveries = self.svc.customer_deliveries("C-01")
        self.assertEqual(len(deliveries), 1)
        self.assertEqual(len(deliveries[0]["corrections"]), 1)
        self.assertEqual(deliveries[0]["corrections"][0]["correction_id"], correction["correction_id"])
        # 原交付体积不被更正篡改
        self.assertEqual(deliveries[0]["volume_m3"], 30)
        # 质量问题冻结不能被人工解除
        self.assert_error_code(
            "freeze_must_hold",
            self.svc.resolve_review_task,
            {"task_id": tasks[0]["task_id"], "lift_freeze": True, "resolved_by": "质量负责人"})

    def test_withdrawn_sample_freezes_undelivered_dispatch(self):
        release = self.svc.create_release({
            "batch_id": self.batch["id"], "volume_m3": 40, "customer_id": "C-02",
            "vehicle_id": "京A-RW002", "sample_id": self.sample["id"],
            "allow_provisional": True, "issued_at": t(10)})
        dispatch = self.svc.register_dispatch({
            "release_no": release["release_no"], "volume_m3": 30,
            "loaded_at": t(10, minute=30), "idempotency_key": "load-2"})
        self.svc.withdraw_sample({
            "sample_id": self.sample["id"], "reason": "采样瓶破损，结果无效",
            "withdrawn_at": t(13)})
        self.assertTrue(self.svc.releases[release["release_no"]]["frozen"])
        self.assertEqual(self.svc.dispatches[dispatch["id"]]["status"], "frozen")
        self.assert_error_code(
            "dispatch_frozen",
            self.svc.confirm_dispatch_callback, {"dispatch_id": dispatch["id"]})

    def test_overdue_sweep_freezes_then_late_qualified_result_unfreezes(self):
        release = self.svc.create_release({
            "batch_id": self.batch["id"], "volume_m3": 30, "customer_id": "C-01",
            "vehicle_id": "京A-RW003", "sample_id": self.sample["id"],
            "allow_provisional": True, "issued_at": t(10)})
        dispatch = self.svc.register_dispatch({
            "release_no": release["release_no"], "volume_m3": 15,
            "loaded_at": t(10, minute=30), "idempotency_key": "load-3"})
        # 24 小时内不冻结
        before = self.svc.sweep_overdue(t(9, day=2))
        self.assertEqual(before["frozen_releases"], [])
        after = self.svc.sweep_overdue(t(10, day=2, minute=1))
        self.assertEqual(after["frozen_releases"], [release["release_no"]])
        self.assertEqual(self.svc.dispatches[dispatch["id"]]["status"], "frozen")
        self.assertEqual(len(self.svc.open_tasks()), 1)
        # 迟到但合格：自动解冻在途装车，放行转为合格
        self.svc.record_result(
            {"sample_id": self.sample["id"], "conforms": True, "received_at": t(11, day=2)})
        r = self.svc.releases[release["release_no"]]
        self.assertFalse(r["frozen"])
        self.assertEqual(r["basis"], "qualified")
        self.assertEqual(self.svc.dispatches[dispatch["id"]]["status"], "loaded")
        # 巡检幂等：不会重复产生任务/通知
        self.svc.sweep_overdue(t(12, day=2))
        self.assertEqual(len(self.svc.open_tasks()), 1)


class InventoryAndIdempotencyTest(ServiceTestCase):
    def setUp(self):
        self.svc, _ = make_service()
        self.addCleanup(self.svc.store.close)
        inf = self.svc.receive_influent(
            {"tank": "T-301", "volume_m3": 40, "occurred_at": t(8)})
        self.sample = self.svc.register_sample(
            {"batch_id": inf["id"], "kind": "release", "taken_at": t(8, minute=30)})
        self.svc.record_result(
            {"sample_id": self.sample["id"], "conforms": True, "received_at": t(9)})
        self.release = self.svc.create_release({
            "batch_id": inf["id"], "volume_m3": 40, "customer_id": "C-01",
            "sample_id": self.sample["id"], "issued_at": t(10)})

    def test_concurrent_loads_cannot_exceed_inventory_and_release_balance(self):
        # 单车容量上限（放行额度尚足时，先撞到车辆容量）
        self.assert_error_code(
            "vehicle_overloaded",
            self.svc.register_dispatch,
            {"release_no": self.release["release_no"], "vehicle_id": "京A-RW003",
             "volume_m3": 16, "loaded_at": t(10, minute=29)})
        # 第一车装走出 30（放行剩余 10、罐存剩余 10）
        self.svc.register_dispatch({
            "release_no": self.release["release_no"], "vehicle_id": "京A-RW001",
            "volume_m3": 30, "loaded_at": t(10, minute=30), "idempotency_key": "a"})
        # 放行单剩余额度不足
        self.assert_error_code(
            "insufficient_inventory",
            self.svc.register_dispatch,
            {"release_no": self.release["release_no"], "vehicle_id": "京A-RW002",
             "volume_m3": 30, "loaded_at": t(10, minute=32), "idempotency_key": "b"})
        # 另一罐的独立放行单
        inf2 = self.svc.receive_influent(
            {"tank": "T-302", "volume_m3": 10, "occurred_at": t(11)})
        s2 = self.svc.register_sample(
            {"batch_id": inf2["id"], "kind": "release", "taken_at": t(11, minute=30)})
        self.svc.record_result({"sample_id": s2["id"], "conforms": True, "received_at": t(12)})
        r2 = self.svc.create_release({
            "batch_id": inf2["id"], "volume_m3": 10, "customer_id": "C-02",
            "sample_id": s2["id"], "issued_at": t(13)})
        self.svc.register_dispatch({
            "release_no": r2["release_no"], "vehicle_id": "京A-RW003",
            "volume_m3": 10, "loaded_at": t(13, minute=30), "idempotency_key": "c"})
        # 第一张放行单只剩 10 额度且罐实存 10：申请 15 被拦截
        self.assert_error_code(
            "insufficient_inventory",
            self.svc.register_dispatch,
            {"release_no": self.release["release_no"], "vehicle_id": "京A-RW003",
             "volume_m3": 15, "loaded_at": t(13, minute=31), "idempotency_key": "d"})
        # 边界内 10 m³ 可以装走，装完罐存为 0
        self.svc.register_dispatch({
            "release_no": self.release["release_no"], "vehicle_id": "京A-RW003",
            "volume_m3": 10, "loaded_at": t(13, minute=32), "idempotency_key": "e"})
        self.assertEqual(self.svc.tanks_view()["T-301"]["level_m3"], 0.0)

    def test_duplicate_callbacks_do_not_double_deduct(self):
        dispatch = self.svc.register_dispatch({
            "release_no": self.release["release_no"], "vehicle_id": "京A-RW003",
            "volume_m3": 15, "loaded_at": t(10, minute=30),
            "idempotency_key": "load-cb"})
        level_after_load = self.svc.tanks_view()["T-301"]["level_m3"]
        self.assertEqual(level_after_load, 25.0)
        r1 = self.svc.confirm_dispatch_callback(
            {"dispatch_id": dispatch["id"], "confirmed_at": t(12)})
        self.assertFalse(r1["deduplicated"])
        r2 = self.svc.confirm_dispatch_callback(
            {"dispatch_id": dispatch["id"], "confirmed_at": t(12)})
        self.assertTrue(r2["deduplicated"])
        # 罐存未被重复扣减
        self.assertEqual(self.svc.tanks_view()["T-301"]["level_m3"], 25.0)
        committed = self.svc._release_view(self.release["release_no"])["committed_m3"]
        self.assertEqual(committed, 15.0)
        # 装车登记回调重放同一幂等键返回原单，不重复建单
        replay = self.svc.register_dispatch({
            "release_no": self.release["release_no"], "vehicle_id": "京A-RW003",
            "volume_m3": 15, "loaded_at": t(10, minute=30),
            "idempotency_key": "load-cb"})
        self.assertEqual(replay["id"], dispatch["id"])
        self.assertEqual(len(self.release["dispatches"]), 1)

    def test_cancelled_load_restores_inventory(self):
        dispatch = self.svc.register_dispatch({
            "release_no": self.release["release_no"], "vehicle_id": "京A-RW003",
            "volume_m3": 15, "loaded_at": t(10, minute=30),
            "idempotency_key": "load-x"})
        self.assertEqual(self.svc.tanks_view()["T-301"]["level_m3"], 25.0)
        self.svc.cancel_dispatch({"dispatch_id": dispatch["id"], "reason": "车辆故障"})
        self.assertEqual(self.svc.tanks_view()["T-301"]["level_m3"], 40.0)
        # 回补后同额度可再装
        again = self.svc.register_dispatch({
            "release_no": self.release["release_no"], "vehicle_id": "京A-RW003",
            "volume_m3": 15, "loaded_at": t(11), "idempotency_key": "load-y"})
        self.assertEqual(again["status"], "loaded")


class RestartTest(ServiceTestCase):
    def test_freeze_notifications_and_tasks_survive_restart(self):
        tmp = tempfile.mkdtemp()
        def fresh():
            return TracingService(EventStore(tmp), CONFIG)
        svc = fresh()
        inf = svc.receive_influent({"tank": "T-301", "volume_m3": 30, "occurred_at": t(8)})
        sample = svc.register_sample(
            {"batch_id": inf["id"], "kind": "release", "taken_at": t(8, minute=30)})
        release = svc.create_release({
            "batch_id": inf["id"], "volume_m3": 30, "customer_id": "C-01",
            "vehicle_id": "京A-RW003", "sample_id": sample["id"],
            "allow_provisional": True, "issued_at": t(10)})
        dispatch = svc.register_dispatch({
            "release_no": release["release_no"], "volume_m3": 15,
            "loaded_at": t(10, minute=30), "idempotency_key": "persisted-load"})
        svc.confirm_dispatch_callback(
            {"dispatch_id": dispatch["id"], "confirmed_at": t(11)})
        svc.record_result(
            {"sample_id": sample["id"], "conforms": False, "received_at": t(20)})
        svc.append_correction({
            "dispatch_id": dispatch["id"], "note": "初判异常，复核中",
            "author": "质量负责人", "occurred_at": t(21)})

        # 重启：重放事件日志
        svc2 = fresh()
        self.assertTrue(svc2.releases[release["release_no"]]["frozen"])
        self.assertEqual(svc2.dispatches[dispatch["id"]]["status"], "confirmed")
        self.assertEqual(len(svc2.open_tasks()), 1)
        self.assertTrue(svc2.notifications_for({"role": "质量人员"}))
        self.assertEqual(svc2.tanks_view()["T-301"]["level_m3"], 15.0)
        self.assertEqual(svc2.batches[inf["id"]]["quality"], "unqualified")
        # 幂等键仍然有效：装车回调重放不重建装车单
        replay = svc2.register_dispatch({
            "release_no": release["release_no"], "volume_m3": 15,
            "loaded_at": t(10, minute=30), "idempotency_key": "persisted-load"})
        self.assertEqual(replay["id"], dispatch["id"])
        # 客户视角的更正与质量通告重启后仍在
        deliveries = svc2.customer_deliveries("C-01")
        self.assertEqual(len(deliveries), 1)
        self.assertEqual(len(deliveries[0]["corrections"]), 1)
        self.assertEqual(len(deliveries[0]["quality_notices"]), 1)
        svc.store.close()
        svc2.store.close()


class ConcurrencyTest(ServiceTestCase):
    """真实多线程：并发装车不超库存/额度；重复确认回调不重复扣量。"""

    def _setup_release(self, volume: int, tank: str = "T-301"):
        svc = self.use_service()
        inf = svc.receive_influent(
            {"tank": tank, "volume_m3": volume, "occurred_at": t(8)})
        sample = svc.register_sample(
            {"batch_id": inf["id"], "kind": "release", "taken_at": t(8, minute=30)})
        svc.record_result(
            {"sample_id": sample["id"], "conforms": True, "received_at": t(9)})
        release = svc.create_release({
            "batch_id": inf["id"], "volume_m3": volume, "customer_id": "C-01",
            "sample_id": sample["id"], "issued_at": t(10)})
        return svc, release

    def test_parallel_loads_never_oversell(self):
        svc, release = self._setup_release(40)
        # 8 个线程各抢 10 m³（车容 15），库存只有 40，恰好 4 单成功
        results: list[tuple[bool, str]] = []
        barrier = threading.Barrier(8)

        def worker(idx: int):
            barrier.wait()
            try:
                svc.register_dispatch({
                    "release_no": release["release_no"], "vehicle_id": "京A-RW003",
                    "volume_m3": 10, "loaded_at": t(10, minute=30),
                    "idempotency_key": f"par-{idx}"})
                results.append((True, str(idx)))
            except Exception as exc:  # noqa: BLE001
                results.append((False, getattr(exc, "code", "error")))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        successes = [r for ok, r in results if ok]
        failures = [r for ok, r in results if not ok]
        self.assertEqual(len(successes), 4)
        self.assertEqual(len(failures), 4)
        self.assertTrue(all(code == "insufficient_inventory" for code in failures))
        # 总装载量等于库存，罐存与放行余额一致
        self.assertEqual(svc.tanks_view()["T-301"]["level_m3"], 0.0)
        view = svc._release_view(release["release_no"])
        self.assertEqual(view["committed_m3"], 40.0)
        self.assertEqual(view["remaining_m3"], 0.0)

    def test_parallel_duplicate_confirm_callbacks_deduct_once(self):
        svc, release = self._setup_release(15)
        dispatch = svc.register_dispatch({
            "release_no": release["release_no"], "vehicle_id": "京A-RW003",
            "volume_m3": 15, "loaded_at": t(10, minute=30), "idempotency_key": "par-load"})
        outcomes: list[bool] = []
        barrier = threading.Barrier(6)

        def callback():
            barrier.wait()
            out = svc.confirm_dispatch_callback(
                {"dispatch_id": dispatch["id"], "confirmed_at": t(12)})
            outcomes.append(out["deduplicated"])

        threads = [threading.Thread(target=callback) for _ in range(6)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        # 恰好一次真正确认，其余全部幂等
        self.assertEqual(sorted(outcomes).count(False), 1)
        self.assertEqual(sorted(outcomes).count(True), 5)
        self.assertEqual(svc.tanks_view()["T-301"]["level_m3"], 0.0)
        self.assertEqual(
            svc._release_view(release["release_no"])["committed_m3"], 15.0)


class TaintPropagationTest(ServiceTestCase):
    """题述核心场景：上游检测撤回/不合格，即使下游留样合格也要冻结。"""

    def _chain_with_blend(self):
        svc = self.use_service()
        # 上游批次
        up = svc.receive_influent({"tank": "T-301", "volume_m3": 50, "occurred_at": t(8)})
        s_up = svc.register_sample(
            {"batch_id": up["id"], "kind": "release", "taken_at": t(8, minute=30)})
        svc.record_result({"sample_id": s_up["id"], "conforms": True, "received_at": t(9)})
        # 另一路合格水
        other = svc.receive_influent({"tank": "T-302", "volume_m3": 50, "occurred_at": t(8)})
        s_ot = svc.register_sample(
            {"batch_id": other["id"], "kind": "release", "taken_at": t(8, minute=30)})
        svc.record_result({"sample_id": s_ot["id"], "conforms": True, "received_at": t(9)})
        # 混配：各 25
        blend = svc.blend_water({
            "inputs": [{"batch_id": up["id"], "volume_m3": 25},
                       {"batch_id": other["id"], "volume_m3": 25}],
            "dest_tank": "T-302", "occurred_at": t(10)})
        s_bl = svc.register_sample(
            {"batch_id": blend["id"], "kind": "release", "taken_at": t(10, minute=30)})
        svc.record_result({"sample_id": s_bl["id"], "conforms": True, "received_at": t(11)})
        release = svc.create_release({
            "batch_id": blend["id"], "volume_m3": 30, "customer_id": "C-01",
            "vehicle_id": "京A-RW001", "sample_id": s_bl["id"], "issued_at": t(12)})
        dispatch = svc.register_dispatch({
            "release_no": release["release_no"], "vehicle_id": "京A-RW001",
            "volume_m3": 30, "loaded_at": t(12, minute=30), "idempotency_key": "t-1"})
        return svc, s_up, release, dispatch, up, blend

    def test_upstream_withdrawal_taints_and_freezes_despite_own_qualified_sample(self):
        svc, s_up, release, dispatch, up, blend = self._chain_with_blend()
        svc.withdraw_sample({
            "sample_id": s_up["id"], "reason": "实验室回查：采样操作违规，结果撤回",
            "withdrawn_at": t(10, day=2)})
        # 上游与混配批次被污染（混配自有留样虽合格，证据链仍断裂）
        self.assertTrue(svc.batches[up["id"]]["taint"])
        self.assertTrue(svc.batches[blend["id"]]["taint"])
        r = svc.releases[release["release_no"]]
        self.assertTrue(r["frozen"])
        self.assertEqual(r["freeze_reason"], "evidence_compromised")
        self.assertEqual(svc.dispatches[dispatch["id"]]["status"], "frozen")
        # 质量反查：从上游样本能看到下游全部放行
        impact = svc.sample_impact(s_up["id"])
        self.assertIn(release["release_no"], [x["release_no"] for x in impact["affected_releases"]])
        self.assertIn(blend["id"], impact["affected_batches"])
        # 污染未消除前不能解冻
        task = svc.open_tasks()[0]
        self.assert_error_code(
            "freeze_must_hold",
            svc.resolve_review_task,
            {"task_id": task["task_id"], "lift_freeze": True, "resolved_by": "质量负责人"})

    def test_resample_qualified_clears_taint_and_allows_unfreeze(self):
        svc, s_up, release, dispatch, up, blend = self._chain_with_blend()
        svc.withdraw_sample({
            "sample_id": s_up["id"], "reason": "结果撤回", "withdrawn_at": t(10, day=2)})
        task = svc.open_tasks()[0]
        # 对上游批次重新采样并合格：污染链清除
        s_new = svc.register_sample(
            {"batch_id": up["id"], "kind": "unit", "taken_at": t(11, day=2)})
        svc.record_result(
            {"sample_id": s_new["id"], "conforms": True, "received_at": t(15, day=2)})
        self.assertFalse(svc.batches[up["id"]]["taint"])
        self.assertFalse(svc.batches[blend["id"]]["taint"])
        svc.resolve_review_task({
            "task_id": task["task_id"], "lift_freeze": True,
            "resolved_by": "质量负责人", "note": "补检合格，恢复发运"})
        self.assertFalse(svc.releases[release["release_no"]]["frozen"])
        self.assertEqual(svc.dispatches[dispatch["id"]]["status"], "loaded")

    def test_overdue_freeze_escalates_when_unqualified_result_arrives(self):
        svc = self.use_service()
        inf = svc.receive_influent({"tank": "T-301", "volume_m3": 20, "occurred_at": t(8)})
        s = svc.register_sample(
            {"batch_id": inf["id"], "kind": "release", "taken_at": t(8, minute=30)})
        release = svc.create_release({
            "batch_id": inf["id"], "volume_m3": 20, "customer_id": "C-01",
            "vehicle_id": "京A-RW003", "sample_id": s["id"],
            "allow_provisional": True, "issued_at": t(10)})
        svc.sweep_overdue(t(10, day=2))
        self.assertEqual(
            svc.releases[release["release_no"]]["freeze_reason"], "result_overdue")
        # 迟到的不合格结果到达：冻结升级
        svc.record_result(
            {"sample_id": s["id"], "conforms": False, "received_at": t(11, day=2)})
        r = svc.releases[release["release_no"]]
        self.assertEqual(r["freeze_reason"], "quality_unqualified")
        self.assertTrue(r["frozen"])
        # 补检合格也不能解除质量冻结：该批次仍因原样本不合格未闭环
        s_new = svc.register_sample({"batch_id": inf["id"], "kind": "unit", "taken_at": t(12, day=2)})
        svc.record_result(
            {"sample_id": s_new["id"], "conforms": True, "received_at": t(14, day=2)})
        task = svc.open_tasks()[0]
        self.assert_error_code(
            "freeze_must_hold",
            svc.resolve_review_task,
            {"task_id": task["task_id"], "lift_freeze": True, "resolved_by": "质量负责人"})
        # 即使质量裁定关闭该不合格结论，绑定留样本身曾不合格，放行单仍须作废重开
        svc.close_unqualified_finding({
            "sample_id": s["id"], "closed_by": "质量负责人",
            "note": "实验室污染导致误判，裁定关闭", "closed_at": t(15, day=2)})
        self.assert_error_code(
            "freeze_must_hold",
            svc.resolve_review_task,
            {"task_id": task["task_id"], "lift_freeze": True, "resolved_by": "质量负责人"})

    def test_upstream_unqualified_finding_closed_allows_unfreeze(self):
        svc, s_up, release, dispatch, up, blend = self._chain_with_blend()
        # 上游留样迟到且不合格
        svc.record_result({
            "sample_id": s_up["id"], "conforms": False, "received_at": t(10, day=2)})
        self.assertEqual(
            svc.releases[release["release_no"]]["freeze_reason"], "evidence_compromised")
        task = svc.open_tasks()[0]
        # 补检合格不能自动翻案
        s_new = svc.register_sample({"batch_id": up["id"], "kind": "unit", "taken_at": t(11, day=2)})
        svc.record_result(
            {"sample_id": s_new["id"], "conforms": True, "received_at": t(12, day=2)})
        self.assertTrue(svc.batches[blend["id"]]["taint"])
        self.assert_error_code(
            "freeze_must_hold",
            svc.resolve_review_task,
            {"task_id": task["task_id"], "lift_freeze": True, "resolved_by": "质量负责人"})
        # 质量负责人正式裁定关闭（复检确认系留样异常）→ 污染消除 → 可解冻
        svc.close_unqualified_finding({
            "sample_id": s_up["id"], "closed_by": "质量负责人",
            "note": "复测合格，判定为留样污染，关闭不合格结论", "closed_at": t(13, day=2)})
        self.assertFalse(svc.batches[blend["id"]]["taint"])
        svc.resolve_review_task({
            "task_id": task["task_id"], "lift_freeze": True,
            "resolved_by": "质量负责人", "note": "证据链恢复，继续发运"})
        self.assertFalse(svc.releases[release["release_no"]]["frozen"])
        self.assertEqual(svc.dispatches[dispatch["id"]]["status"], "loaded")

    def test_tainted_batch_cannot_be_blended_or_released(self):
        svc, s_up, release, dispatch, up, blend = self._chain_with_blend()
        other = svc.receive_influent({"tank": "T-101", "volume_m3": 10, "occurred_at": t(13)})
        svc.withdraw_sample({
            "sample_id": s_up["id"], "reason": "结果撤回", "withdrawn_at": t(10, day=2)})
        # 混配批次物理质量仍合格（自有留样合格），但证据链被上游污染
        self.assertEqual(svc.batches[blend["id"]]["quality"], "qualified")
        self.assertTrue(svc.batches[blend["id"]]["taint"])
        # 被污染批次不得再用于混配
        self.assert_error_code(
            "input_evidence_compromised",
            svc.blend_water,
            {"inputs": [{"batch_id": blend["id"], "volume_m3": 10},
                        {"batch_id": other["id"], "volume_m3": 10}],
             "dest_tank": "T-101", "occurred_at": t(11, day=2)})
        # 证据污染批次禁止新放行（其自有留样合格，但上游证据链断裂）
        s_rel = svc.register_sample(
            {"batch_id": blend["id"], "kind": "release", "taken_at": t(11, day=2)})
        svc.record_result(
            {"sample_id": s_rel["id"], "conforms": True, "received_at": t(12, day=2)})
        self.assert_error_code(
            "evidence_compromised",
            svc.create_release,
            {"batch_id": blend["id"], "volume_m3": 1, "customer_id": "C-01",
             "sample_id": s_rel["id"], "issued_at": t(15)})

    def test_generic_idempotency_key_survives_restart(self):
        tmp = tempfile.mkdtemp()
        def fresh():
            return TracingService(EventStore(tmp), CONFIG)
        svc = fresh()
        inf = svc.receive_influent(
            {"tank": "T-301", "volume_m3": 10, "occurred_at": t(8),
             "idempotency_key": "influent-uniq-1"})
        svc.store.close()
        svc2 = fresh()
        self.addCleanup(svc2.store.close)
        self.assertEqual(svc2.batches[inf["id"]]["volume"], 10)
        # 重启后同一幂等键再次提交被拒绝（不会重复进水）
        self.assert_error_code(
            "idem_replayed",
            svc2.receive_influent,
            {"tank": "T-301", "volume_m3": 10, "occurred_at": t(8),
             "idempotency_key": "influent-uniq-1"})
        self.assertEqual(svc2.tanks_view()["T-301"]["level_m3"], 10.0)


class HttpApiTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.server = create_server(data_dir=self.tmp, config_path=CONFIG_PATH)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.server.service.store.close()

    def _req(self, method, path, token=None, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.load(resp)
        except urllib.error.HTTPError as exc:
            return exc.code, json.load(exc)

    def test_health_is_process_only(self):
        status, body = self._req("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ok")

    def test_auth_and_customer_isolation(self):
        # 无令牌 401
        self.assertEqual(self._req("GET", "/api/tanks")[0], 401)
        # 客户不能看罐区
        self.assertEqual(self._req("GET", "/api/tanks", TOKENS["c01"])[0], 403)
        # 调度员建水并放行给 C-01
        st, inf = self._req("POST", "/api/influent", TOKENS["dispatcher"],
                            {"tank": "T-301", "volume_m3": 30, "occurred_at": t(8)})
        self.assertEqual(st, 200)
        st, sample = self._req("POST", "/api/samples", TOKENS["quality"],
                               {"batch_id": inf["id"], "kind": "release", "taken_at": t(8, minute=30)})
        self.assertEqual(st, 200)
        st, _ = self._req("POST", f"/api/samples/{sample['id']}/results", TOKENS["quality"],
                          {"conforms": True, "received_at": t(9)})
        self.assertEqual(st, 200)
        st, release = self._req("POST", "/api/releases", TOKENS["dispatcher"],
                                {"batch_id": inf["id"], "volume_m3": 30, "customer_id": "C-01",
                                 "vehicle_id": "京A-RW003", "sample_id": sample["id"],
                                 "issued_at": t(10)})
        self.assertEqual(st, 200)
        st, dispatch = self._req("POST", "/api/dispatches", TOKENS["operator"],
                                 {"release_no": release["release_no"], "volume_m3": 15,
                                  "loaded_at": t(10, minute=30), "idempotency_key": "http-1"})
        self.assertEqual(st, 200)
        st, _ = self._req("POST", f"/api/dispatches/{dispatch['id']}/confirm", TOKENS["operator"],
                          {"confirmed_at": t(12)})
        self.assertEqual(st, 200)
        # C-01 看到交付，C-02 看不到
        st, d01 = self._req("GET", "/api/my/deliveries", TOKENS["c01"])
        self.assertEqual(st, 200)
        self.assertEqual(len(d01), 1)
        st, d02 = self._req("GET", "/api/my/deliveries", TOKENS["c02"])
        self.assertEqual(st, 200)
        self.assertEqual(d02, [])
        # 客户看不到其他客户信息字段
        self.assertNotIn("customer_id", d01[0])
        # 质量反查接口客户无权
        self.assertEqual(
            self._req("GET", f"/api/samples/{sample['id']}/impact", TOKENS["c01"])[0], 403)
        # 质量人员可反查
        st, impact = self._req("GET", f"/api/samples/{sample['id']}/impact", TOKENS["quality"])
        self.assertEqual(st, 200)
        self.assertIn(inf["id"], impact["affected_batches"])
        # 谱系查询
        st, lineage = self._req("GET", f"/api/batches/{inf['id']}/lineage", TOKENS["regulator"])
        self.assertEqual(st, 200)
        self.assertEqual(lineage["volume_m3"], 30)
        # 监管只读账户不能下发命令
        self.assertEqual(
            self._req("POST", "/api/influent", TOKENS["regulator"],
                      {"tank": "T-301", "volume_m3": 1})[0], 403)


if __name__ == "__main__":
    unittest.main()
