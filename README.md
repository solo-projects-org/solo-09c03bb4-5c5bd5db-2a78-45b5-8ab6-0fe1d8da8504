# 水质数仓 · 温度探头观测追溯与网关采集 API

一支温度探头发生故障时，数据员需要把它在多座测站留下的**原始观测**全部找出来复核，
且不能把同站替换探头的数据混进去。本服务提供读数入库、装机时段管理、按探头追溯，
以及按站点网关整批采集积压温度观测四类能力。

## 业务规则

### 1. 原始读数 `POST /readings`
- 报文含观测号 `observation_id`、站号 `station_id`、UTC 时刻 `ts_utc`（必须带时区）、温度 `temperature`。
- 观测号为主键：**同号同值补传幂等**（返回 `200` + `status=duplicate`，不重复入库）；
  **同号异文一律拒收**（`409 reading_conflict`），原读数保持不变。
- 不同时区写法只要换算到同一 UTC 时刻即视为同一时刻。

### 2. 装机时段表 `PUT /stations/{station_id}/installation`
- 按站**整版提交**完整装机表，每段写探头号与半开起止时刻 `[start_ts, end_ts)`，
  `end_ts` 为 `null` 表示探头至今在站（开放区间）。
- 必须携带 `expected_version`（乐观并发）：首版为 `0`，每次成功提交版本号 +1；
  版本不符返回 `409 version_conflict`。
- 校验规则（任一不满足则**整版拒绝、原表保留**）：
  - 同站各装机时段不得重叠（半开区间首尾相接 `end == next start` 不算重叠）；
  - 同一探头不得在重叠时段同时挂在两座站（`409 probe_double_mount`）；
  - 每段 `start_ts < end_ts`（`400 invalid_interval`）。
- 允许探头拆下后再装到别的站（时段不重叠的跨站换装）。

### 3. 受影响追溯 `GET /probes/{probe_id}/affected?start=...&end=...`
- 按探头 + UTC 半开区间返回：
  - `install_segments`：窗口内与该探头相关的装机段（归属依据）；
  - `attributed_readings`：时刻落在该探头装机段内的读数，每条附对应装机记录；
  - `unattributed_readings`：该探头曾挂载过的站上、窗口内但**未被装机段覆盖**的读数，
    明确标注原因，绝不冒充归属；从未装过该探头的站的数据不会混入。
- `end` 可省略表示开放上界。
- **迟到读数**入库后、**追溯换装**提交后，下一次查询立即反映新清单。

### 4. 网关批次采集（清空站点网关积压）
- `POST /collections/run`，报文 `{"station_id": "ST-A"}`，一次调用按网关采集**一批**：
  - 每批至多 100 行，批号、行号与原文固定；网关未确认前重复拉取返回同一批（at-least-once）。
  - 合法行沿用第 1 节读数规则入库；**坏行不卡住正常数据**——
    同号异文或字段非法的行只进隔离表，保留原文、来源（站/批/行号）与原因，
    不覆盖任何原读数，同批其他行照常接收。
  - 整批“接纳 + 隔离 + 批次状态”在**同一个本地事务**内持久化，成功提交后才向网关
    发确认；本地写入失败整体回滚，绝不提前确认网关。
  - 返回 `status`：`collected`（新落库一批）、`already_persisted`（重投递去重）、
    `no_batch`（网关积压已空）。
- 采集状态与隔离明细查询：
  - `GET /collections/status?station_id=...`：批次总数、已接纳/隔离行数、
    `persisted`（已落库待回执）与 `acked`（网关已清退）清单；
  - `GET /collections/quarantine?station_id=&batch_id=&reason_code=`：隔离明细，
    可按站/批/原因过滤，每条含原文、行号、原因码与原因说明；
  - `GET /stations/{station_id}/batches/{batch_id}`：单批接纳/隔离/回执状态。
- 隔离原因码：`invalid_json`（原文不是合法 JSON 对象）、
  `invalid_fields`（字段缺失/非法或站号不符）、`reading_conflict`（同号异文）。

#### 采集一致性（不漏行、不重复隔离、不误报完成）
| 场景 | 行为 |
| --- | --- |
| 本地写入失败 | 整批事务回滚，无半批；不向网关确认，批次下次可重新采集 |
| 网关已清退但回执丢失 | 本地保持 `persisted`，接口返回 502 而非成功；下次采集先对账，网关 410 视为已确认 |
| 多实例并发采集同一批 | 主键 `(station_id, batch_id)` + 隔离行唯一键去重，恰好落库一次，后到者复用结果 |
| 采集实例重启后重试 | 启动后首次采集自动对账所有 `persisted` 批次，幂等补确认 |
| 重复确认旧批号 | 网关校验批号必须是当前队头，否则 410，绝不会误删下一批 |
| 等待网关响应 | HTTP 调用全部在数据库事务之外，不占 SQLite 锁；其他网关采集与手工录入不受阻塞 |

### 5. 模拟站点网关（Compose 内置）
网关是独立的 FastAPI 服务（`gateway/app.py`，容器内监听 8100），状态持久化在
`gateway_data` 卷，重启不丢积压：

| 接口 | 用途 |
| --- | --- |
| `POST /test/stations/{station_id}/backlog` | 投放一批积压观测（结构化对象或原始 JSON 文本，最多 100 行） |
| `GET  /stations/{station_id}/batches/next` | 拉队头批次；未确认前重复拉取返回同一批；积压空返回 204 |
| `POST /stations/{station_id}/batches/{batch_id}/ack` | 确认清退队头；重复确认/批号不符返回 410 |
| `POST /test/faults` | 注入 `none`/`disconnect`（断线 503）/`ack_lost`（先清退后回执失败 502），可指定 `station_id`、`delay_sec` |
| `GET  /test/state` / `POST /test/reset` | 观察与复位网关积压/故障 |

## 一致性与多实例设计

| 关注点 | 实现 |
| --- | --- |
| 同一已提交状态 | 写操作用 `BEGIN IMMEDIATE`，读操作用 `BEGIN DEFERRED` 单快照，一次请求内所有 SQL 基于同一版本 |
| 失败不留半版 | 业务校验与写入在同一事务内，任何错误均 `ROLLBACK` |
| 多实例并发 | SQLite WAL + 共享数据卷；写锁冲突有界重试（`WQ_LOCK_RETRIES`/`WQ_BUSY_TIMEOUT_MS`） |
| 交错换装/补传 | 乐观版本号 + 探头双挂检查在同一写事务内完成，并发提交恰好一胜一负 |
| 时间存储 | 定宽 UTC 文本（`...%f+00:00`），字典序即时间序，半开判断直接字符串比较 |

> SQLite 的锁机制依赖本地文件系统，数据卷请使用本地磁盘，不要挂 NFS 等网络文件系统。

## 启动方式（Docker Compose）

```bash
docker compose up --build -d
# api 两副本 + nginx 负载均衡，数据持久化在命名卷 wq_data；
# gateway 单副本承载模拟站点网关，状态在 gateway_data 卷
```

- 入口（经 nginx）：<http://localhost:8080>
- **Swagger 操作入口**：<http://localhost:8080/docs>
- OpenAPI JSON：<http://localhost:8080/openapi.json>
- 模拟网关（可直接投放/注入故障）：<http://localhost:8100>，文档 <http://localhost:8100/docs>
- 健康检查：`GET /health`

单副本直连调试：

```bash
docker compose up --build api   # 容器内监听 8000
```

## 本地开发（无 Docker 时）

```bash
python3 -m virtualenv .venv
.venv/bin/pip install -r requirements-dev.txt
WQ_DB_PATH=./data/dev.db .venv/bin/uvicorn app.main:app --reload
# 自测
.venv/bin/python -m pytest tests/ -v
```

## 请求示例

```bash
# 提交读数
curl -X POST http://localhost:8080/readings -H 'Content-Type: application/json' -d '{
  "observation_id": "OBS-1001",
  "station_id": "ST-A",
  "ts_utc": "2026-01-15T08:30:00Z",
  "temperature": 21.7
}'

# 首版装机表（expected_version=0）
curl -X PUT http://localhost:8080/stations/ST-A/installation \
  -H 'Content-Type: application/json' -d '{
  "expected_version": 0,
  "segments": [
    {"probe_id": "P1", "start_ts": "2026-01-01T00:00:00Z",
     "end_ts": "2026-02-01T00:00:00Z"},
    {"probe_id": "P2", "start_ts": "2026-02-01T00:00:00Z", "end_ts": null}
  ]
}'

# 追溯故障探头 P1
curl 'http://localhost:8080/probes/P1/affected?start=2026-01-01T00:00:00Z&end=2026-03-01T00:00:00Z'

# 向模拟网关投放一批观测（含坏行），再触发采集
curl -X POST http://localhost:8100/test/stations/ST-A/backlog -H 'Content-Type: application/json' -d '{
  "batch_id": "B-20260115-01",
  "rows": [
    {"observation_id": "GW-1", "station_id": "ST-A", "ts_utc": "2026-01-15T08:00:00Z", "temperature": 21.7},
    "not-a-json-line",
    "{\"observation_id\":\"GW-2\",\"station_id\":\"ST-A\",\"ts_utc\":\"bad\",\"temperature\":1}"
  ]
}'
curl -X POST http://localhost:8080/collections/run -H 'Content-Type: application/json' \
  -d '{"station_id": "ST-A"}'
# 查询采集状态与隔离明细
curl 'http://localhost:8080/collections/status?station_id=ST-A'
curl 'http://localhost:8080/collections/quarantine?station_id=ST-A'
```

## 错误响应格式

```json
{"error": {"code": "version_conflict", "message": "...", "details": {...}}}
```

| HTTP | code | 含义 |
| --- | --- | --- |
| 400 | `invalid_interval` | 时段/查询区间起止非法 |
| 404 | `batch_not_found` | 查询的采集批次不存在 |
| 409 | `reading_conflict` | 同观测号异文补传拒收（手工录入）；采集中则转隔离 `reading_conflict` |
| 409 | `station_overlap` | 同站装机时段重叠，整版拒绝 |
| 409 | `probe_double_mount` | 探头跨站同时挂载，整版拒绝 |
| 409 | `version_conflict` | 期望版本与当前版本不一致 |
| 422 | `validation_error` | 报文/参数结构或类型校验失败 |
| 502 | `gateway_unavailable` / `ack_lost` / `gateway_pull_failed` 等 | 网关不可达或回执失败，本地批次保留待对账，**不误报完成** |
| 503 | `database_busy` | 本地写锁竞争超时，无半批写入，可重试 |
| 504 | `gateway_timeout` | 等待网关响应超时 |

## 目录结构

```
app/                数仓 API（config/db/errors/schemas/service/collection/gateway_client/routers/main）
gateway/app.py      模拟站点网关（投放/拉取/确认 + 断线与回执丢失故障注入）
tests/              pytest 场景/并发/采集测试 + 真实 HTTP 端到端冒烟脚本
Dockerfile          应用镜像（API 与网关共用，启动命令不同，含 healthcheck）
docker-compose.yml  api×2 + nginx + gateway + 持久化卷
nginx.conf          多副本负载均衡
```
