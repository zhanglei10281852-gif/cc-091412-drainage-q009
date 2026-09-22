"""再生水批次追踪 HTTP 服务。

角色（X-Actor-Role 头，支持中文或英文）：
  quality    质量人员   —— 检测结果、撤回、冻结复核、交付更正、样本反查
  dispatcher 调度员     —— 放行单、装车、交付回调、旁路登记
  ops        运维人员   —— 进水、单元流转、拆分/混配、入罐、采样
  regulator  监管人员   —— 全部只读
  readonly   只读用户   —— 全部只读
  customer   客户       —— 只能看到自己的放行与交付（X-Actor-Id 为客户编号）
"""

import json
import os
import re
import tempfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from tracking.service import DomainError, Forbidden, NotFound, TrackingService, load_config

SERVICE_NAME = "reclaimed-water-tracking"

ROLE_ALIASES = {
    "质量人员": "quality",
    "调度员": "dispatcher",
    "运维人员": "ops",
    "监管人员": "regulator",
    "只读用户": "readonly",
    "客户": "customer",
}

NON_CUSTOMER = {"quality", "dispatcher", "ops", "regulator", "readonly"}
ANY_ROLE = NON_CUSTOMER | {"customer"}

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "reference" / "plant_config.json"


def _default_db_path():
    return os.environ.get(
        "TRACKING_DB",
        os.path.join(tempfile.gettempdir(), "reclaimed-water-tracking.db"),
    )


def default_service():
    config_path = os.environ.get("TRACKING_CONFIG", str(DEFAULT_CONFIG_PATH))
    config = load_config(config_path) if Path(config_path).exists() else None
    return TrackingService(_default_db_path(), config=config)


# ----------------------------------------------------------------------
# 路由处理函数
# ----------------------------------------------------------------------
def _h_health(svc, actor, match, body, query):
    return {"status": "ok", "service": SERVICE_NAME}


def _h_config(svc, actor, match, body, query):
    return svc.get_config()


def _h_inventory(svc, actor, match, body, query):
    return svc.inventory()


def _h_conservation(svc, actor, match, body, query):
    return svc.conservation_report()


def _h_intake(svc, actor, match, body, query):
    return svc.register_intake(
        body["intake_id"], body["volume_m3"], body["source_time"], body.get("metadata")
    )


def _h_transfer(svc, actor, match, body, query):
    return svc.transfer(match.group("bid"), body["unit_id"], body["occurred_at"])


def _h_bypass(svc, actor, match, body, query):
    return svc.open_bypass(
        match.group("uid"), body["start"], body["end"], body.get("reason")
    )


def _h_split(svc, actor, match, body, query):
    return svc.split_batch(match.group("bid"), body["parts_m3"], body.get("occurred_at"))


def _h_merge(svc, actor, match, body, query):
    return svc.merge_batches(
        body["batch_ids"], body.get("volumes_m3"), body.get("occurred_at")
    )


def _h_store(svc, actor, match, body, query):
    return svc.store_in_tank(match.group("bid"), body["tank_id"])


def _h_get_batch(svc, actor, match, body, query):
    return svc.get_batch(match.group("bid"))


def _h_list_batches(svc, actor, match, body, query):
    return svc.list_batches()


def _h_genealogy(svc, actor, match, body, query):
    return svc.genealogy(match.group("bid"))


def _h_collect(svc, actor, match, body, query):
    return svc.collect_sample(
        body["sample_id"], body["batch_id"], body.get("rule_id"), body["collected_at"]
    )


def _h_result(svc, actor, match, body, query):
    return svc.record_result(
        match.group("sid"), body["verdict"], body.get("analytes"), body.get("recorded_at")
    )


def _h_retract(svc, actor, match, body, query):
    return svc.retract_result(
        match.group("sid"), body["reason"], body.get("retracted_at")
    )


def _h_impact(svc, actor, match, body, query):
    return svc.sample_impact(match.group("sid"))


def _h_create_release(svc, actor, match, body, query):
    return svc.create_release(
        body["release_id"],
        body["batch_id"],
        body["customer_id"],
        body["volume_m3"],
        body.get("purpose"),
    )


def _customer_scope(actor, query):
    """客户角色强制只能看自己；其余角色可按 customer_id 过滤。"""
    requested = (query.get("customer_id") or [None])[0]
    if actor["role"] == "customer":
        if not actor["id"]:
            raise Forbidden("客户请求缺少 X-Actor-Id")
        if requested and requested != actor["id"]:
            raise Forbidden("客户只能查看自己的信息")
        return actor["id"]
    return requested


def _h_list_releases(svc, actor, match, body, query):
    return svc.list_releases(_customer_scope(actor, query))


def _h_get_release(svc, actor, match, body, query):
    release = svc.get_release(match.group("rid"))
    if actor["role"] == "customer" and release["customer_id"] != actor["id"]:
        raise NotFound("放行单不存在")
    return release


def _h_load(svc, actor, match, body, query):
    return svc.load_vehicle(
        match.group("rid"),
        body["load_id"],
        body["vehicle_id"],
        body["volume_m3"],
        body.get("occurred_at"),
    )


def _h_delivery_callback(svc, actor, match, body, query):
    return svc.confirm_delivery(
        match.group("rid"),
        body["callback_id"],
        body["volume_m3"],
        body.get("delivered_at"),
    )


def _h_list_deliveries(svc, actor, match, body, query):
    return svc.list_deliveries(_customer_scope(actor, query))


def _h_get_delivery(svc, actor, match, body, query):
    delivery = svc.get_delivery(match.group("did"))
    if actor["role"] == "customer" and delivery["customer_id"] != actor["id"]:
        raise NotFound("交付记录不存在")
    return delivery


def _h_correct(svc, actor, match, body, query):
    return svc.correct_delivery(
        match.group("did"), body["correction_id"], body["delta_m3"], body["reason"]
    )


def _h_freezes(svc, actor, match, body, query):
    return svc.list_freezes((query.get("status") or [None])[0])


def _h_resolve_freeze(svc, actor, match, body, query):
    return svc.resolve_freeze(match.group("fid"), body["decision"], body.get("resolved_at"))


def _h_tasks(svc, actor, match, body, query):
    return svc.list_tasks((query.get("status") or [None])[0])


def _h_notifications(svc, actor, match, body, query):
    pending = (query.get("pending") or ["0"])[0] in ("1", "true")
    return svc.list_notifications(pending_only=pending)


def _h_dispatch(svc, actor, match, body, query):
    return svc.dispatch_notifications()


ROUTES = [
    ("GET", r"/health", _h_health, None),
    ("GET", r"/config", _h_config, NON_CUSTOMER),
    ("GET", r"/inventory", _h_inventory, NON_CUSTOMER),
    ("GET", r"/conservation", _h_conservation, NON_CUSTOMER),
    ("POST", r"/intakes", _h_intake, {"ops", "dispatcher"}),
    ("POST", r"/batches/merge", _h_merge, {"ops"}),
    ("POST", r"/batches/(?P<bid>[^/]+)/transfers", _h_transfer, {"ops"}),
    ("POST", r"/batches/(?P<bid>[^/]+)/split", _h_split, {"ops"}),
    ("POST", r"/batches/(?P<bid>[^/]+)/store", _h_store, {"ops"}),
    ("POST", r"/units/(?P<uid>[^/]+)/bypass", _h_bypass, {"ops", "dispatcher"}),
    ("GET", r"/batches/(?P<bid>[^/]+)/genealogy", _h_genealogy, NON_CUSTOMER),
    ("GET", r"/batches/(?P<bid>[^/]+)", _h_get_batch, NON_CUSTOMER),
    ("GET", r"/batches", _h_list_batches, NON_CUSTOMER),
    ("POST", r"/samples", _h_collect, {"ops", "quality"}),
    ("POST", r"/samples/(?P<sid>[^/]+)/results", _h_result, {"quality"}),
    ("POST", r"/samples/(?P<sid>[^/]+)/retract", _h_retract, {"quality"}),
    ("GET", r"/samples/(?P<sid>[^/]+)/impact", _h_impact, NON_CUSTOMER),
    ("POST", r"/releases", _h_create_release, {"dispatcher", "quality"}),
    ("GET", r"/releases", _h_list_releases, ANY_ROLE),
    ("GET", r"/releases/(?P<rid>[^/]+)", _h_get_release, ANY_ROLE),
    ("POST", r"/releases/(?P<rid>[^/]+)/loads", _h_load, {"dispatcher"}),
    ("POST", r"/releases/(?P<rid>[^/]+)/delivery-callbacks", _h_delivery_callback,
     {"dispatcher", "ops"}),
    ("GET", r"/deliveries", _h_list_deliveries, ANY_ROLE),
    ("GET", r"/deliveries/(?P<did>[^/]+)", _h_get_delivery, ANY_ROLE),
    ("POST", r"/deliveries/(?P<did>[^/]+)/corrections", _h_correct, {"quality"}),
    ("GET", r"/freezes", _h_freezes, NON_CUSTOMER),
    ("POST", r"/freezes/(?P<fid>[^/]+)/resolve", _h_resolve_freeze, {"quality"}),
    ("GET", r"/tasks", _h_tasks, NON_CUSTOMER),
    ("GET", r"/notifications", _h_notifications, NON_CUSTOMER),
    ("POST", r"/notifications/dispatch", _h_dispatch, {"ops", "dispatcher", "quality"}),
]

COMPILED_ROUTES = [(method, re.compile(f"^{pattern}$"), handler, roles)
                   for method, pattern, handler, roles in ROUTES]


def _actor_from(headers):
    raw = headers.get("X-Actor-Role", "readonly")
    return {
        "role": ROLE_ALIASES.get(raw, raw),
        "id": headers.get("X-Actor-Id"),
    }


class Handler(BaseHTTPRequestHandler):
    service = None  # 由 create_server 注入

    def do_GET(self):  # noqa: N802
        self._dispatch("GET")

    def do_POST(self):  # noqa: N802
        self._dispatch("POST")

    def _dispatch(self, method):
        actor = _actor_from(self.headers)
        split = urlsplit(self.path)
        path, query = split.path, parse_qs(split.query)
        body = {}
        if method == "POST":
            length = int(self.headers.get("Content-Length") or 0)
            if length:
                try:
                    body = json.loads(self.rfile.read(length).decode("utf-8"))
                except (ValueError, UnicodeDecodeError):
                    self._send(400, {"error": "invalid_json", "message": "请求体不是合法 JSON"})
                    return
        for route_method, pattern, handler, roles in COMPILED_ROUTES:
            if route_method != method:
                continue
            match = pattern.match(path)
            if not match:
                continue
            try:
                if roles is not None and actor["role"] not in roles:
                    raise Forbidden(f"角色 {actor['role']} 无权访问 {path}")
                result = handler(self.service, actor, match, body, query)
                self._send(200, result)
            except DomainError as exc:
                payload = {"error": exc.code, "message": exc.message}
                payload.update(exc.detail)
                self._send(exc.status, payload)
            except KeyError as exc:
                self._send(400, {"error": "missing_field", "message": f"缺少字段 {exc}"})
            except Exception as exc:  # noqa: BLE001
                self._send(500, {"error": "internal_error", "message": str(exc)})
            return
        self._send(404, {"error": "not_found"})

    def _send(self, status, payload):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *_args):
        return


def create_server(service=None, host=None, port=None):
    service = service or default_service()
    host = host or os.environ.get("HOST", "0.0.0.0")
    port = int(port if port is not None else os.environ.get("PORT", "8000"))

    class BoundHandler(Handler):
        pass

    BoundHandler.service = service
    server = ThreadingHTTPServer((host, port), BoundHandler)
    server.daemon_threads = True
    server.tracking_service = service
    return server
