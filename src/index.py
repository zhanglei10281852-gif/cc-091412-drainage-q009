import os
from pathlib import Path

from app import create_server

if __name__ == "__main__":
    os.environ.setdefault("DATA_DIR", str(Path.cwd() / "data"))
    server = create_server()
    print("再生水批次追踪服务已启动", flush=True)
    server.serve_forever()
