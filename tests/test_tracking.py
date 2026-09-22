"""再生水批次追踪服务不变量测试。"""

import json
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tracking.service import Conflict, Forbidden, NotFound, TrackingService, load_config  # noqa: E402
from app import create_server  # noqa: E402

CONFIG = load_config(Path(__file__).resolve().parents[1] / "reference" / "plant_config.json")

T0 = "2026-09-01T08:00:00+08:00"
T1 = "2026-09-01T10:00:00+08:00"
T2 = "2026-09-01T20:00:00+08:00"
T3 = "2026-09-02T09:00:00+08:00"
T4 = "2026-09-03T10:00:00+08:00"  # 超出 SR-EFFLUENT 24h 期限（采样于 T1）


class ServiceTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = str(Path(self.tmp.name) / "tracking.db")
        self.svc = TrackingService(self.db, config=CONFIG)
        self.addCleanup(self.svc.close)

    # -- 常用流程 ------------------------------------------------------
    def intake(self, batch_id="B-IN-1", volume=1000):
        return self.svc.register_intake(batch_id, volume, T0)

    def qualify(self, batch_id, sample_id="S-PASS-1", collected=T1, recorded=T2):
        self.svc.collect_sample(sample_id, batch_id, "SR-EFFLUENT", collected)
        return self.svc.record_result(
            sample_id, "pass", {"turbidity": 0.4, "residual_chlorine": 0.6}, recorded
        )

    def qualified_batch(self, batch_id="B-IN-1", volume=1000):
        self.intake(batch_id, volume)
        self.svc.transfer(batch_id, "U-COAG", T0)
        self.qualify(batch_id)
        return batch_id

    def assert_balanced(self):
        report = self.svc.conservation_report()
        self.assertTrue(report["balanced"], report)
        return report


class IntakeAndTransferTest(ServiceTestCase):
    def test_intake_creates_root_batch_pending(self):
        batch = self.intake(volume=250)
        self.assertEqual(batch["quality"], "pending")
        self.assertEqual(batch["remaining_m3"], 250)
        self.assert_balanced()

    def test_duplicate_intake_rejected(self):
        self.intake()
        with self.assertRaises(Conflict):
            self.intake()

    def test_naive_timestamp_rejected(self):
        with self.assertRaises(Exception):
            self.svc.register_intake("B-X", 10, "2026-09-01 08:00:00")

    def test_release_requires_qualified(self):
        self.intake()
        with self.assertRaises(Conflict):
            self.svc.create_release("RO-1", "B-IN-1", "CUST-A", 10)


class VolumeConservationTest(ServiceTestCase):
    def test_split_conserves_volume(self):
        self.intake(volume=1000)
        result = self.svc.split_batch("B-IN-1", [300, 700])
        children = result["children"]
        self.assertEqual([c["remaining_m3"] for c in children], [300, 700])
        self.assertEqual(result["parent"]["remaining_m3"], 0)
        self.assert_balanced()

    def test_split_exceeding_remaining_rejected(self):
        self.intake(volume=100)
        with self.assertRaises(Conflict):
            self.svc.split_batch("B-IN-1", [60, 60])
        self.assert_balanced()

    def test_merge_conserves_volume_and_sums(self):
        self.intake("B-1", 400)
        self.intake("B-2", 600)
        merged = self.svc.merge_batches(["B-1", "B-2"], volumes_m3=[150, 250])
        self.assertEqual(merged["remaining_m3"], 400)
        self.assertEqual(self.svc.get_batch("B-1")["remaining_m3"], 250)
        self.assertEqual(self.svc.get_batch("B-2")["remaining_m3"], 350)
        self.assert_balanced()

    def test_merge_default_takes_full_remaining(self):
        self.intake("B-1", 100)
        self.intake("B-2", 200)
        merged = self.svc.merge_batches(["B-1", "B-2"])
        self.assertEqual(merged["remaining_m3"], 300)
        self.assertEqual(self.svc.get_batch("B-1")["remaining_m3"], 0)
        self.assert_balanced()

    def test_split_merge_chain_stays_balanced(self):
        self.intake(volume=1000)
        children = self.svc.split_batch("B-IN-1", [400, 600])["children"]
        merged = self.svc.merge_batches([children[0]["id"], children[1]["id"]],
                                        volumes_m3=[100, 200])
        self.assertEqual(merged["remaining_m3"], 300)
        report = self.assert_balanced()
        self.assertEqual(report["intake_m3"], 1000)
        self.assertEqual(report["remaining_m3"], 1000)

    def test_merge_quality_rules(self):
        self.qualified_batch("B-Q", 100)
        self.intake("B-P", 100)
        merged = self.svc.merge_batches(["B-Q", "B-P"], volumes_m3=[50, 50])
        self.assertEqual(merged["quality"], "pending")  # 未检批次混入 → 不能算合格
        self.qualify("B-P", sample_id="S-PASS-2")
        merged2 = self.svc.merge_batches(["B-Q", "B-P"], volumes_m3=[50, 50])
        self.assertEqual(merged2["quality"], "qualified")


class BypassTest(ServiceTestCase):
    def test_bypass_strips_inherited_qualification(self):
        self.qualified_batch("B-IN-1", 500)
        # 超滤单元 09:00-11:00 临时旁路
        self.svc.open_bypass("U-UF", "2026-09-01T09:00:00+08:00",
                             "2026-09-01T11:00:00+08:00", "膜组抢修")
        batch = self.svc.transfer("B-IN-1", "U-UF", "2026-09-01T10:00:00+08:00")
        self.assertEqual(batch["quality"], "suspect")
        self.assertTrue(batch["bypassed"])
        with self.assertRaises(Conflict):
            self.svc.create_release("RO-1", "B-IN-1", "CUST-A", 10)

    def test_bypassed_water_requalified_by_new_sample(self):
        self.qualified_batch("B-IN-1", 500)
        self.svc.open_bypass("U-UF", "2026-09-01T09:00:00+08:00",
                             "2026-09-01T11:00:00+08:00", "膜组抢修")
        self.svc.transfer("B-IN-1", "U-UF", "2026-09-01T10:00:00+08:00")
        self.qualify("B-IN-1", sample_id="S-AFTER-BYPASS")
        release = self.svc.create_release("RO-1", "B-IN-1", "CUST-A", 10)
        self.assertEqual(release["status"], "active")

    def test_transfer_outside_window_keeps_quality(self):
        self.qualified_batch("B-IN-1", 500)
        self.svc.open_bypass("U-UF", "2026-09-01T09:00:00+08:00",
                             "2026-09-01T11:00:00+08:00", "膜组抢修")
        batch = self.svc.transfer("B-IN-1", "U-UF", "2026-09-01T12:00:00+08:00")
        self.assertEqual(batch["quality"], "qualified")

    def test_bypass_flag_flows_to_descendants(self):
        self.qualified_batch("B-IN-1", 500)
        self.svc.open_bypass("U-UF", "2026-09-01T09:00:00+08:00",
                             "2026-09-01T11:00:00+08:00", "膜组抢修")
        self.svc.transfer("B-IN-1", "U-UF", "2026-09-01T10:00:00+08:00")
        child = self.svc.split_batch("B-IN-1", [200, 300])["children"][0]
        self.assertTrue(child["bypassed"])
        self.assertEqual(child["quality"], "suspect")


class FreezeTest(ServiceTestCase):
    def released(self, release_id="RO-1", customer="CUST-A", volume=100):
        self.qualified_batch("B-IN-1", 500)
        return self.svc.create_release(release_id, "B-IN-1", customer, volume)

    def test_late_result_freezes_release(self):
        self.released()
        # 放行前采的平行样，结果迟到（超过 24h 期限）
        self.svc.collect_sample("S-LATE", "B-IN-1", "SR-EFFLUENT", T1)
        outcome = self.svc.record_result("S-LATE", "pass", {}, T4)
        self.assertTrue(outcome["sample"]["result"]["late"])
        self.assertEqual(len(outcome["freezes"]), 1)
        release = self.svc.get_release("RO-1")
        self.assertEqual(release["status"], "frozen")
        # 冻结期间禁止装车
        with self.assertRaises(Conflict):
            self.svc.load_vehicle("RO-1", "L-1", "京A10001", 10)
        # 生成通知与待复核任务
        self.assertEqual(len(self.svc.list_freezes(status="open")), 1)
        tasks = self.svc.list_tasks(status="open")
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]["kind"], "review_freeze")
        pending = self.svc.list_notifications(pending_only=True)
        self.assertEqual(pending[0]["kind"], "RELEASE_FROZEN")

    def test_retracted_result_freezes_release(self):
        self.released()
        outcome = self.svc.retract_result("S-PASS-1", "试剂污染，结果作废", T3)
        self.assertEqual(len(outcome["freezes"]), 1)
        self.assertEqual(self.svc.get_release("RO-1")["status"], "frozen")
        # 批次失去合格依据
        self.assertEqual(self.svc.get_batch("B-IN-1")["quality"], "pending")

    def test_failed_result_freezes_and_marks_unqualified(self):
        self.released()
        self.svc.collect_sample("S-FAIL", "B-IN-1", "SR-EFFLUENT", T1)
        outcome = self.svc.record_result("S-FAIL", "fail", {"E.coli": 10}, T2)
        self.assertEqual(outcome["batch_quality"], "unqualified")
        self.assertEqual(self.svc.get_release("RO-1")["status"], "frozen")

    def test_freeze_follows_genealogy_through_blends(self):
        # 两次混配后，上游样本能冻结到最终放行
        self.qualified_batch("B-A", 300)
        self.intake("B-B", 200)
        self.qualify("B-B", sample_id="S-B")
        merged1 = self.svc.merge_batches(["B-A", "B-B"], volumes_m3=[100, 100])
        self.qualify(merged1["id"], sample_id="S-M1")
        self.intake("B-C", 100)
        self.qualify("B-C", sample_id="S-C")
        merged2 = self.svc.merge_batches([merged1["id"], "B-C"], volumes_m3=[150, 50])
        self.qualify(merged2["id"], sample_id="S-M2")
        self.svc.create_release("RO-FINAL", merged2["id"], "CUST-A", 100)
        # 撤回最上游 B-A 的合格结果
        outcome = self.svc.retract_result("S-PASS-1", "原始记录缺失", T3)
        self.assertEqual(self.svc.get_release("RO-FINAL")["status"], "frozen")
        self.assertIn(outcome["freezes"][0],
                      [f["id"] for f in self.svc.list_freezes(status="open")])
        # 下游批次被降级，不能新开放行
        self.assertEqual(self.svc.get_batch(merged2["id"])["quality"], "suspect")
        with self.assertRaises(Conflict):
            self.svc.create_release("RO-X", merged2["id"], "CUST-B", 10)

    def test_resolve_freeze_release_and_void(self):
        self.released()
        self.svc.collect_sample("S-LATE", "B-IN-1", "SR-EFFLUENT", T1)
        self.svc.record_result("S-LATE", "pass", {}, T4)
        freeze_id = self.svc.list_freezes(status="open")[0]["id"]
        outcome = self.svc.resolve_freeze(freeze_id, "release")
        self.assertEqual(outcome["release"]["status"], "active")
        self.assertEqual(self.svc.list_tasks(status="open"), [])
        # 再次冻结后作废
        self.svc.collect_sample("S-LATE2", "B-IN-1", "SR-EFFLUENT", T1)
        self.svc.record_result("S-LATE2", "fail", {}, T4)
        freeze_id = self.svc.list_freezes(status="open")[0]["id"]
        outcome = self.svc.resolve_freeze(freeze_id, "void")
        self.assertEqual(outcome["release"]["status"], "voided")
        with self.assertRaises(Conflict):
            self.svc.load_vehicle("RO-1", "L-9", "京A10001", 10)


class LoadingConcurrencyTest(ServiceTestCase):
    def test_concurrent_loading_never_exceeds_inventory(self):
        self.qualified_batch("B-IN-1", 50)
        self.svc.create_release("RO-1", "B-IN-1", "CUST-A", 50)

        def attempt(i):
            try:
                self.svc.load_vehicle("RO-1", f"L-{i}", "京A10002", 10)
                return True
            except Conflict:
                return False

        with ThreadPoolExecutor(max_workers=10) as pool:
            results = list(pool.map(attempt, range(10)))
        self.assertEqual(sum(results), 5)
        release = self.svc.get_release("RO-1")
        self.assertEqual(release["loaded_m3"], 50)
        self.assertEqual(self.svc.get_batch("B-IN-1")["remaining_m3"], 0)
        self.assert_balanced()

    def test_load_id_replay_does_not_double_deduct(self):
        self.qualified_batch("B-IN-1", 50)
        self.svc.create_release("RO-1", "B-IN-1", "CUST-A", 50)
        first = self.svc.load_vehicle("RO-1", "L-1", "京A10002", 20)
        second = self.svc.load_vehicle("RO-1", "L-1", "京A10002", 20)
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        self.assertEqual(self.svc.get_release("RO-1")["loaded_m3"], 20)
        self.assertEqual(self.svc.get_batch("B-IN-1")["remaining_m3"], 30)

    def test_vehicle_capacity_enforced(self):
        self.qualified_batch("B-IN-1", 100)
        self.svc.create_release("RO-1", "B-IN-1", "CUST-A", 100)
        with self.assertRaises(Conflict):
            self.svc.load_vehicle("RO-1", "L-1", "京A10001", 25)  # 核载 20


class DeliveryTest(ServiceTestCase):
    def delivered(self):
        self.qualified_batch("B-IN-1", 100)
        self.svc.create_release("RO-1", "B-IN-1", "CUST-A", 60)
        self.svc.load_vehicle("RO-1", "L-1", "京A10002", 30)
        return self.svc.confirm_delivery("RO-1", "CB-1", 30, T3)

    def test_duplicate_callback_not_double_counted(self):
        first = self.delivered()
        again = self.svc.confirm_delivery("RO-1", "CB-1", 30, T3)
        self.assertFalse(first["replayed"])
        self.assertTrue(again["replayed"])
        self.assertEqual(again["delivery"]["id"], first["delivery"]["id"])
        release = self.svc.get_release("RO-1")
        self.assertEqual(release["delivered_m3"], 30)
        self.assertEqual(len(self.svc.list_deliveries("CUST-A")), 1)
        self.assert_balanced()

    def test_delivery_cannot_exceed_loaded(self):
        self.qualified_batch("B-IN-1", 100)
        self.svc.create_release("RO-1", "B-IN-1", "CUST-A", 60)
        self.svc.load_vehicle("RO-1", "L-1", "京A10002", 20)
        with self.assertRaises(Conflict):
            self.svc.confirm_delivery("RO-1", "CB-1", 30, T3)

    def test_delivered_record_append_only_correction(self):
        delivered = self.delivered()
        delivery_id = delivered["delivery"]["id"]
        corrected = self.svc.correct_delivery(delivery_id, "CORR-1", -5, "计量复核偏差")
        self.assertFalse(corrected["replayed"])
        # 原记录不变，更正以追加形式存在
        fetched = self.svc.get_delivery(delivery_id)
        self.assertEqual(fetched["volume_m3"], 30)
        self.assertEqual(len(fetched["corrections"]), 1)
        self.assertEqual(fetched["corrections"][0]["delta_m3"], -5)
        # 重复更正键幂等
        replay = self.svc.correct_delivery(delivery_id, "CORR-1", -5, "计量复核偏差")
        self.assertTrue(replay["replayed"])
        self.assertEqual(len(self.svc.get_delivery(delivery_id)["corrections"]), 1)
        # 更正产生通知
        kinds = [n["kind"] for n in self.svc.list_notifications()]
        self.assertIn("DELIVERY_CORRECTED", kinds)


class TankTest(ServiceTestCase):
    def test_tank_capacity_enforced(self):
        self.intake("B-1", 400)
        self.svc.store_in_tank("B-1", "T-01")
        self.intake("B-2", 200)
        with self.assertRaises(Conflict):
            self.svc.store_in_tank("B-2", "T-01")  # 400+200 > 500
        self.svc.store_in_tank("B-2", "T-02")
        inventory = self.svc.inventory()
        tanks = {t["id"]: t for t in inventory["tanks"]}
        self.assertEqual(tanks["T-01"]["used_m3"], 400)
        self.assertEqual(tanks["T-02"]["used_m3"], 200)


class ImpactAndGenealogyTest(ServiceTestCase):
    def test_sample_impact_reverse_lookup(self):
        # 谱系：B-IN-1 拆分 → 一支与 B-2 混配 → 放行 → 交付
        self.qualified_batch("B-IN-1", 500)
        children = self.svc.split_batch("B-IN-1", [200, 300])["children"]
        child = children[0]
        self.intake("B-2", 100)
        self.qualify("B-2", sample_id="S-B2")
        merged = self.svc.merge_batches([child["id"], "B-2"], volumes_m3=[200, 100])
        self.qualify(merged["id"], sample_id="S-M")
        self.svc.create_release("RO-1", merged["id"], "CUST-A", 100)
        self.svc.load_vehicle("RO-1", "L-1", "京A10002", 30)
        self.svc.confirm_delivery("RO-1", "CB-1", 30, T3)

        impact = self.svc.sample_impact("S-PASS-1")  # B-IN-1 的出厂样
        batch_ids = {b["id"] for b in impact["batches"]}
        # 两个拆分子批次同属该样本影响范围，即使只有一支进入混配
        self.assertEqual(
            batch_ids, {"B-IN-1", children[0]["id"], children[1]["id"], merged["id"]}
        )
        self.assertEqual([r["id"] for r in impact["releases"]], ["RO-1"])
        self.assertEqual(len(impact["deliveries"]), 1)

        # 撤回后反查可见冻结
        self.svc.retract_result("S-PASS-1", "留样复测不合格", T4)
        impact = self.svc.sample_impact("S-PASS-1")
        self.assertEqual(len(impact["freezes"]), 1)
        self.assertEqual(impact["freezes"][0]["reason"], "retracted_result")

    def test_genealogy_reports_edges(self):
        self.intake("B-IN-1", 500)
        children = self.svc.split_batch("B-IN-1", [200, 300])["children"]
        merged = self.svc.merge_batches([children[0]["id"], children[1]["id"]],
                                        volumes_m3=[100, 100])
        tree = self.svc.genealogy(merged["id"])
        self.assertEqual(len(tree["ancestors"]), 3)
        edge_volumes = sorted(e["volume_m3"] for e in tree["edges"])
        self.assertEqual(edge_volumes, [100, 100, 200, 300])


class PersistenceTest(ServiceTestCase):
    def test_restart_keeps_freezes_notifications_tasks(self):
        self.qualified_batch("B-IN-1", 100)
        self.svc.create_release("RO-1", "B-IN-1", "CUST-A", 50)
        self.svc.collect_sample("S-LATE", "B-IN-1", "SR-EFFLUENT", T1)
        self.svc.record_result("S-LATE", "pass", {}, T4)
        self.svc.close()

        # 模拟重启：同一数据库路径重新打开
        svc2 = TrackingService(self.db, config=CONFIG)
        self.addCleanup(svc2.close)
        self.assertEqual(svc2.get_release("RO-1")["status"], "frozen")
        self.assertEqual(len(svc2.list_freezes(status="open")), 1)
        self.assertEqual(len(svc2.list_tasks(status="open")), 1)
        pending = svc2.list_notifications(pending_only=True)
        self.assertEqual(len(pending), 1)
        # 重启后通知仍可外发，任务仍可复核
        dispatched = svc2.dispatch_notifications()
        self.assertEqual(len(dispatched["dispatched"]), 1)
        self.assertEqual(svc2.list_notifications(pending_only=True), [])
        freeze_id = svc2.list_freezes(status="open")[0]["id"]
        svc2.resolve_freeze(freeze_id, "release")
        self.assertEqual(svc2.get_release("RO-1")["status"], "active")


class HttpApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.svc = TrackingService(str(Path(cls.tmp.name) / "api.db"), config=CONFIG)
        cls.server = create_server(service=cls.svc, host="127.0.0.1", port=0)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.svc.close()
        cls.tmp.cleanup()

    def call(self, method, path, body=None, role=None, actor_id=None):
        headers = {"Content-Type": "application/json"}
        if role:
            headers["X-Actor-Role"] = role
        if actor_id:
            headers["X-Actor-Id"] = actor_id
        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = urllib.request.Request(
            self.base + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(request) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as exc:
            return exc.code, json.load(exc)

    def test_health(self):
        status, payload = self.call("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "ok")

    def test_full_flow_and_customer_isolation(self):
        # HTTP 头无法直接携带中文，角色用英文令牌（服务端同时接受中文角色名）
        ops = {"role": "ops"}
        quality = {"role": "quality"}
        dispatcher = {"role": "dispatcher"}

        status, _ = self.call("POST", "/intakes", {
            "intake_id": "B-HTTP-1", "volume_m3": 100, "source_time": T0}, **ops)
        self.assertEqual(status, 200)
        self.call("POST", "/samples", {
            "sample_id": "S-HTTP-1", "batch_id": "B-HTTP-1",
            "rule_id": "SR-EFFLUENT", "collected_at": T1}, **quality)
        self.call("POST", "/samples/S-HTTP-1/results", {
            "verdict": "pass", "analytes": {"turbidity": 0.3}, "recorded_at": T2}, **quality)
        status, _ = self.call("POST", "/releases", {
            "release_id": "RO-HTTP-1", "batch_id": "B-HTTP-1",
            "customer_id": "CUST-A", "volume_m3": 40}, **dispatcher)
        self.assertEqual(status, 200)
        status, _ = self.call("POST", "/releases", {
            "release_id": "RO-HTTP-2", "batch_id": "B-HTTP-1",
            "customer_id": "CUST-B", "volume_m3": 40}, **dispatcher)
        self.assertEqual(status, 200)
        self.call("POST", "/releases/RO-HTTP-1/loads", {
            "load_id": "L-HTTP-1", "vehicle_id": "京A10002", "volume_m3": 30}, **dispatcher)
        status, delivery = self.call(
            "POST", "/releases/RO-HTTP-1/delivery-callbacks",
            {"callback_id": "CB-HTTP-1", "volume_m3": 30, "delivered_at": T3}, **dispatcher)
        self.assertEqual(status, 200)
        delivery_id = delivery["delivery"]["id"]

        # 客户只能看到自己的交付
        status, mine = self.call("GET", "/deliveries", role="customer", actor_id="CUST-A")
        self.assertEqual(status, 200)
        self.assertEqual([d["id"] for d in mine], [delivery_id])
        status, _ = self.call("GET", "/deliveries", role="customer", actor_id="CUST-B")
        self.assertEqual(status, 200)
        status, _ = self.call("GET", f"/deliveries/{delivery_id}",
                              role="customer", actor_id="CUST-B")
        self.assertEqual(status, 404)  # 他人交付不可见
        status, _ = self.call("GET", "/deliveries?customer_id=CUST-B",
                              role="customer", actor_id="CUST-A")
        self.assertEqual(status, 403)
        # 客户无权访问库存与反查
        status, _ = self.call("GET", "/inventory", role="customer", actor_id="CUST-A")
        self.assertEqual(status, 403)
        status, _ = self.call("GET", "/samples/S-HTTP-1/impact",
                              role="customer", actor_id="CUST-A")
        self.assertEqual(status, 403)
        # 质量人员可反查
        status, impact = self.call("GET", "/samples/S-HTTP-1/impact", **quality)
        self.assertEqual(status, 200)
        self.assertEqual(impact["releases"][0]["customer_id"], "CUST-A")
        # 运维不能登记检测结果（越权）
        status, _ = self.call("POST", "/samples/S-HTTP-2/results",
                              {"verdict": "pass"}, **ops)
        self.assertEqual(status, 403)


if __name__ == "__main__":
    unittest.main()
