# 再生水批次追踪服务

面向再生水厂的批次谱系追踪服务：把进水、处理单元、检测样本、混配、放行和客户接收串成可回溯的批次谱系，支撑质量负责人在客户投诉后的回查。

## 运行

需要 Python 3.11 或更高版本，仅依赖标准库。

- `python src/index.py` 启动服务，默认监听 8000 端口
- `python -m unittest discover` 执行全部测试
- `docker compose up --build` 容器化启动

运行时配置：

| 环境变量 | 含义 | 默认 |
| --- | --- | --- |
| `TRACKING_DB` | SQLite 落盘位置 | 系统临时目录下 `reclaimed-water-tracking.db` |
| `TRACKING_CONFIG` | 厂区参考配置（工艺/采样/罐区/车辆） | `reference/plant_config.json` |
| `PORT` / `HOST` | 监听地址 | `8000` / `0.0.0.0` |

## 业务规则

- **谱系**：进水登记为根批次；拆分/混配生成子批次并记录转移边，体积以升为单位整数记账，严格守恒（`GET /conservation` 可随时校验）。
- **旁路**：`POST /units/{id}/bypass` 登记旁路窗口；窗口内流经该单元的水降级为 `suspect`，不自动继承合格状态，必须重新采样合格后才能放行；旁路标记沿谱系传递。
- **检测**：采样按 `reference/plant_config.json` 中的规则生成结果期限；结果迟到、判废或撤回时，谱系下游批次降级、受影响放行单冻结，并生成通知与待复核任务。
- **放行与交付**：仅 `qualified` 批次可开放行单；放行单是额度，装车时才在事务内条件扣减库存，并发装车不会超卖；交付回调按 `callback_id` 幂等，重复回调不重复扣量。
- **已交付记录**：不可修改，只能 `POST /deliveries/{id}/corrections` 追加更正（更正键同样幂等）。
- **持久化**：全部状态在 SQLite 中，重启后冻结、未外发通知、待复核任务继续有效。

## 角色与数据隔离

请求头 `X-Actor-Role`（接受中文角色名或英文令牌）与 `X-Actor-Id`：

| 角色 | 权限 |
| --- | --- |
| `quality` 质量人员 | 检测结果登记/撤回、冻结复核、交付更正、样本反查 |
| `dispatcher` 调度员 | 放行单、装车、交付回调、旁路登记 |
| `ops` 运维人员 | 进水、单元流转、拆分/混配、入罐、采样 |
| `regulator` / `readonly` | 只读 |
| `customer` 客户 | 只能看到自己的放行与交付（`X-Actor-Id` 为客户编号） |

## 主要接口

```
POST /intakes                          进水登记（生成根批次）
POST /batches/{id}/transfers           流经处理单元（自动应用旁路规则）
POST /units/{id}/bypass                登记旁路窗口
POST /batches/{id}/split               拆分（体积守恒）
POST /batches/merge                    混配（体积守恒）
POST /batches/{id}/store               入罐（容量校验）
POST /samples                          采样
POST /samples/{id}/results             登记检测结果（迟到/判废触发冻结）
POST /samples/{id}/retract             撤回结果（触发冻结）
GET  /samples/{id}/impact              质量反查：样本影响的全部批次/放行/交付/冻结
GET  /batches/{id}/genealogy           批次谱系（祖先/后代/转移边）
POST /releases                         开放行单
POST /releases/{id}/loads              装车（并发安全、幂等）
POST /releases/{id}/delivery-callbacks 交付回调（幂等）
POST /deliveries/{id}/corrections      追加更正（原记录不变）
GET  /releases /deliveries             列表（客户自动隔离）
GET  /freezes  POST /freezes/{id}/resolve   冻结与复核
GET  /tasks                            待复核任务
GET  /notifications  POST /notifications/dispatch  通知出站队列
GET  /inventory /conservation          库存总览 / 体积守恒校验
GET  /health                           进程存活
```

时间字段统一使用带时区的 ISO 8601 字符串；事件同时保留来源时间与接收时间。
