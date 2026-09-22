# 再生水批次追踪服务

把再生水的**进水 → 处理单元（含临时旁路）→ 采样检测 → 拆分/混配 → 放行 → 车辆装载 → 客户接收**串成可回溯的批次谱系，解决"台账说不清哪些检测结果对应最终交付水量"的问题。

## 运行

需要 Python 3.11+（仅标准库）。

```bash
python src/index.py            # 默认 0.0.0.0:8000，事件日志落在 ./data
PORT=8123 DATA_DIR=/var/tracing python src/index.py
python -m unittest discover    # 全部测试
docker compose up --build
```

站点资料（工艺参数、采样规则、罐区容量、车辆、客户、令牌）在 `reference/site-data.json`，可用 `SITE_CONFIG` 覆盖；落盘位置由 `DATA_DIR` 指定，测试只使用临时目录。

## 核心设计

- **事件溯源（`src/tracing.py`）**：所有状态变化以不可变事件追加写入 `DATA_DIR/events.log`（逐条 fsync）。重启重放恢复全部状态——冻结、通知、待复核任务、罐存、装车幂等映射都不丢。**已交付记录（装车/签收）创建后不可修改**，质量问题只能通过"追加更正 + 客户质量通告"表达。
- **批次谱系**：批次间父子边记录每个上游批次的体积贡献（`parents: [[batch_id, m3], …]`）。处理过料、拆分、混配都强制**体积守恒**（拆分体积之和必须等于可用体积；混配输出 = 输入合计 − 损耗），并同时校验罐区容量。
- **旁路不继承合格状态**：标记 `bypass=true` 的处理过料，产物质量为 `unknown`，只能凭**本批次留样检测合格**转正，不会沿用工段上游的合格结论。
- **检测传播（两条独立的链）**：
  - 物理质量链：不合格/撤回结果沿下游按最差档传播，混配按最差父批次计。
  - 证据污染链（taint）：上游样本不合格/撤回即使被下游自己的合格留样掩盖，也会污染全部下游批次（证据链断裂）。受影响放行一律冻结，原因分别记为 `quality_unqualified` / `evidence_compromised`；在污染批次**补采并取得新合格样本**后污染消除，质量复核任务可人工解冻。
- **结果迟到**：超过采样规则时限（默认 24h）未出结果，巡检 `/api/sweep` 冻结依赖该留样的临时放行（`result_overdue`）；迟到但合格的结果到达后自动解冻并把放行依据转为 `qualified`。
- **并发安全**：服务层单把可重入锁串行化所有命令。装车时同时校验放行单余额、车辆容量、罐内可用库存（装出即扣罐存，取消回补，签收回调不再扣量）。装车接口支持 `idempotency_key`；同一放行单重复回调返回原单；签收回调对同一装车单天然幂等。
- **多租户可见性**：Bearer 令牌（见 `reference/site-data.json` 的 `principals`）。客户令牌只能访问 `/api/my/deliveries` 与本人通知；质量/监管可查谱系与样本影响面；监管账户只读。

## 接口

除 `/health` 外均需 `Authorization: Bearer <token>`，请求/响应均为 JSON，时间统一为带时区 ISO 8601。

| 方法 & 路径 | 角色 | 说明 |
| --- | --- | --- |
| `POST /api/influent` | 调度/运维 | 进水登记（罐、体积、来源、时间） |
| `POST /api/process` | 调度/运维 | 处理单元过料，可 `bypass=true`，记工艺参数与损耗 |
| `POST /api/split` | 调度/运维 | 批次拆分（体积守恒） |
| `POST /api/blend` | 调度/运维 | 多批次混配（体积守恒，按最差质量） |
| `POST /api/samples` | 质量/运维/调度 | 采样登记（`kind`: unit/bypass/release） |
| `POST /api/samples/{id}/results` | 质量 | 录入检测结果（可多次版本、迟到、合格/不合格） |
| `POST /api/samples/{id}/withdraw` | 质量 | 撤回检测结果（视为结果作废），触发冻结传播 |
| `POST /api/samples/{id}/close-finding` | 质量 | 正式裁定关闭不合格结论（复检确认/留样异常），解除污染 |
| `POST /api/releases` | 调度/质量 | 开放行单（须绑定本批次 release 留样；`allow_provisional` 临时放行） |
| `POST /api/dispatches` | 调度/运维 | 车辆装载，带 `idempotency_key`，校验三重库存 |
| `POST /api/dispatches/{id}/confirm` | 调度/运维 | 过磅/签收回调，重复回调幂等不重复扣量 |
| `POST /api/dispatches/{id}/cancel` | 调度/运维 | 未交付装车取消并回补库存 |
| `POST /api/corrections` | 质量/调度 | 对**已交付**记录追加更正（禁止改写原记录） |
| `POST /api/sweep` | 质量/调度 | 超时结果巡检，冻结临时放行并建待复核任务 |
| `POST /api/review-tasks/{id}/resolve` | 质量 | 完成复核；污染消除后可 `lift_freeze` 解冻 |
| `GET /api/batches/{id}/lineage` | 内部 | 批次完整上下游谱系与贡献体积 |
| `GET /api/samples/{id}/impact` | 质量/监管 | **反查一个检测样本影响的全部批次、放行、交付、冻结单** |
| `GET /api/releases` `/api/tanks` `/api/review-tasks` | 内部 | 放行单/罐存/待复核任务 |
| `GET /api/my/deliveries` | 客户 | 仅本客户已交付记录、更正与质量通告 |
| `GET /api/notifications` | 全部 | 按身份过滤的冻结/质量通知 |
| `GET /health` | 公开 | 仅表示进程存活 |

## 典型事件流（题述场景）

1. 一批水经历 UF→RO 全流程合格放行的同时，另一路在 RO 段 `bypass` 临时绕过；旁路水单独留样。
2. 成品经历**两次混配**，最终批次的 `lineage` 可逐层列出每一路来水及贡献体积。
3. 放行、装车、客户签收后客户投诉；实验室**撤回旁路水留样** → 两张放行单（含已交付与在途）立即冻结为 `evidence_compromised`，生成质量通知（内部 + 客户"暂停使用"）与待复核任务。
4. 已交付装车单保持 `confirmed` 不可变，质量人员通过 `/api/corrections` 追加召回/换水更正。
5. 质量人员用 `/api/samples/{id}/impact` 反查该样本影响的全部批次与交付；对污染批次补采样、检测合格后，复核任务解除冻结。
6. 服务重启后，上述冻结、通知、任务、库存与幂等键全部仍在。

## 测试

`tests/test_tracing.py` 覆盖：两次混配谱系、拆分/混配体积守恒、旁路不继承质量、迟到不合格冻结、撤回冻结在途装车、超时巡检与迟到合格自动解冻、已交付只追加更正、并发装车不超卖（真实多线程）、重复回调不重复扣量（真实多线程）、客户隔离、重启恢复与污染链补检解冻，共 17 个用例。
