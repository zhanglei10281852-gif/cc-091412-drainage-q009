# 污水处理与地下管网服务基础工程

这是面向城市排水、污水处理和地下管网运维的 Python 服务起始工程，保留进程健康检查和运行配置，业务模块按实际保护、调度或追踪场景接入。

## 运行

需要 Python 3.11 或更高版本。直接执行 `python src/index.py` 启动服务，默认监听 8000 端口；`python -m unittest discover` 执行基线测试，也可以使用 `docker compose up --build` 启动容器。
