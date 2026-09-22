import json
import sys
import tempfile
import threading
import unittest
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from app import SERVICE_NAME, create_server  # noqa: E402


class HealthTest(unittest.TestCase):
    def test_health_returns_service_identity(self):
        server = create_server(data_dir=tempfile.mkdtemp())
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.shutdown)
        self.addCleanup(server.server_close)
        self.addCleanup(server.service.store.close)
        with urllib.request.urlopen(f"http://127.0.0.1:{server.server_port}/health") as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(json.load(response), {"status": "ok", "service": SERVICE_NAME})


if __name__ == "__main__":
    unittest.main()
