"""再生水批次追踪服务 HTTP 入口。

- 健康接口仅表示进程存活（沿用基线约定）。
- /api/* 需要 Bearer 令牌；客户令牌只能访问本人的交付与通知。
- 落盘位置由 DATA_DIR 指定，站点资料由 SITE_CONFIG 指定，测试不依赖主机隐藏状态。
"""

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from tracing import EventStore, ServiceError, TracingService

SERVICE_NAME = "reclaimed-water-batch-tracing"

_STAFF_ROLES = {"调度员", "运维人员", "质量人员", "监管人员"}
_READ_ONLY_ROLES = {"监管人员"}


def _default_config_path() -> str:
    env = os.environ.get("SITE_CONFIG")
    if env:
        return env
    return str(Path(__file__).resolve().parents[1] / "reference" / "site-data.json")


def build_service() -> TracingService:
    data_dir = os.environ.get("DATA_DIR", str(Path.cwd() / "data"))
    with open(_default_config_path(), encoding="utf-8") as fh:
        config = json.load(fh)
    return TracingService(EventStore(data_dir), config)


class Handler(BaseHTTPRequestHandler):
    service: TracingService | None = None

    def _send_json(self, status: int, body: dict | list) -> None:
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if not length:
            return {}
        try:
            data = json.loads(self.rfile.read(length).decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ServiceError("bad_json", f"请求体不是合法 JSON：{exc}", 400)
        return data if isinstance(data, dict) else {}

    def _principal(self) -> dict:
        auth = self.headers.get("Authorization", "")
        token = auth[7:] if auth.startswith("Bearer ") else ""
        principal = (self.service.config.get("principals", {}) or {}).get(token)
        if principal is None:
            raise ServiceError("unauthorized", "缺少或无效的 Bearer 令牌", 401)
        return principal

    def _require_roles(self, principal: dict, roles: set[str]) -> None:
        if principal["role"] not in roles:
            raise ServiceError("forbidden", f"角色 {principal['role']} 无权执行该操作", 403)

    # -------------------------------------------------------------- 路由

    def do_GET(self):  # noqa: N802
        path = self.path.split("?")[0]
        try:
            if path == "/health":
                self._send_json(200, {"status": "ok", "service": SERVICE_NAME})
                return
            principal = self._principal()
            if path == "/api/my/deliveries":
                self._require_roles(principal, {"客户"})
                body = self.service.customer_deliveries(principal["customer_id"])
            elif path == "/api/notifications":
                body = self.service.notifications_for(principal)
            elif path == "/api/tanks":
                self._require_roles(principal, _STAFF_ROLES)
                body = self.service.tanks_view()
            elif path == "/api/review-tasks":
                self._require_roles(principal, {"质量人员", "监管人员"})
                body = self.service.open_tasks()
            elif path == "/api/releases":
                self._require_roles(principal, _STAFF_ROLES)
                body = [
                    self.service._release_view(no)  # noqa: SLF001
                    for no in self.service.releases
                ]
            elif path.startswith("/api/batches/") and path.endswith("/lineage"):
                self._require_roles(principal, _STAFF_ROLES)
                body = self.service.batch_lineage(path.split("/")[3])
            elif path.startswith("/api/samples/") and path.endswith("/impact"):
                self._require_roles(principal, {"质量人员", "监管人员"})
                body = self.service.sample_impact(path.split("/")[3])
            else:
                raise ServiceError("not_found", "接口不存在", 404)
            self._send_json(200, body)
        except ServiceError as exc:
            self._send_json(exc.http_status, {"error": exc.code, "message": exc.message})

    def do_POST(self):  # noqa: N802
        path = self.path.split("?")[0]
        try:
            principal = self._principal()
            cmd = self._read_json()

            if path == "/api/influent":
                self._require_roles(principal, {"调度员", "运维人员"})
                body = self.service.receive_influent(cmd)
            elif path == "/api/process":
                self._require_roles(principal, {"调度员", "运维人员"})
                body = self.service.process_water(cmd)
            elif path == "/api/split":
                self._require_roles(principal, {"调度员", "运维人员"})
                body = self.service.split_batch(cmd)
            elif path == "/api/blend":
                self._require_roles(principal, {"调度员", "运维人员"})
                body = self.service.blend_water(cmd)
            elif path == "/api/samples":
                self._require_roles(principal, {"质量人员", "运维人员", "调度员"})
                body = self.service.register_sample(cmd)
            elif path.startswith("/api/samples/") and path.endswith("/results"):
                self._require_roles(principal, {"质量人员"})
                cmd["sample_id"] = path.split("/")[3]
                body = self.service.record_result(cmd)
            elif path.startswith("/api/samples/") and path.endswith("/withdraw"):
                self._require_roles(principal, {"质量人员"})
                cmd["sample_id"] = path.split("/")[3]
                body = self.service.withdraw_sample(cmd)
            elif path.startswith("/api/samples/") and path.endswith("/close-finding"):
                self._require_roles(principal, {"质量人员"})
                cmd["sample_id"] = path.split("/")[3]
                body = self.service.close_unqualified_finding(cmd)
            elif path == "/api/releases":
                self._require_roles(principal, {"调度员", "质量人员"})
                body = self.service.create_release(cmd)
            elif path == "/api/dispatches":
                self._require_roles(principal, {"调度员", "运维人员"})
                body = self.service.register_dispatch(cmd)
            elif path.startswith("/api/dispatches/") and path.endswith("/confirm"):
                self._require_roles(principal, {"调度员", "运维人员"})
                cmd["dispatch_id"] = path.split("/")[3]
                body = self.service.confirm_dispatch_callback(cmd)
            elif path.startswith("/api/dispatches/") and path.endswith("/cancel"):
                self._require_roles(principal, {"调度员", "运维人员"})
                cmd["dispatch_id"] = path.split("/")[3]
                body = self.service.cancel_dispatch(cmd)
            elif path == "/api/corrections":
                self._require_roles(principal, {"质量人员", "调度员"})
                body = self.service.append_correction(cmd)
            elif path == "/api/sweep":
                self._require_roles(principal, {"质量人员", "调度员"})
                body = self.service.sweep_overdue(cmd.get("now"))
            elif path.startswith("/api/review-tasks/") and path.endswith("/resolve"):
                self._require_roles(principal, {"质量人员"})
                cmd["task_id"] = path.split("/")[3]
                body = self.service.resolve_review_task(cmd)
            else:
                raise ServiceError("not_found", "接口不存在", 404)

            self._send_json(200, body)
        except ServiceError as exc:
            self._send_json(exc.http_status, {"error": exc.code, "message": exc.message})

    def log_message(self, *_args):
        return


def create_server(data_dir: str | None = None, config_path: str | None = None):
    port = int(os.environ.get("PORT", "8000"))
    host = os.environ.get("HOST", "0.0.0.0")
    if data_dir:
        os.environ["DATA_DIR"] = data_dir
    if config_path:
        os.environ["SITE_CONFIG"] = config_path
    service = build_service()
    handler = type("BoundHandler", (Handler,), {"service": service})
    server = ThreadingHTTPServer((host, port), handler)
    server.service = service  # type: ignore[attr-defined]
    return server
