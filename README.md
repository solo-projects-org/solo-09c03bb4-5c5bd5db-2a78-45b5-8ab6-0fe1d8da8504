# 水质数仓 · 温度探头观测追溯 API

一支温度探头发生故障时，数据员需要把它在多座测站留下的**原始观测**全部找出来复核，
且不能把同站替换探头的数据混进去。本服务提供读数入库、装机时段管理与按探头追溯三类能力。

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
# api 两副本 + nginx 负载均衡，数据持久化在命名卷 wq_data
```

- 入口（经 nginx）：<http://localhost:8080>
- **Swagger 操作入口**：<http://localhost:8080/docs>
- OpenAPI JSON：<http://localhost:8080/openapi.json>
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
```

## 错误响应格式

```json
{"error": {"code": "version_conflict", "message": "...", "details": {...}}}
```

| HTTP | code | 含义 |
| --- | --- | --- |
| 400 | `invalid_interval` | 时段/查询区间起止非法 |
| 409 | `reading_conflict` | 同观测号异文补传拒收 |
| 409 | `station_overlap` | 同站装机时段重叠，整版拒绝 |
| 409 | `probe_double_mount` | 探头跨站同时挂载，整版拒绝 |
| 409 | `version_conflict` | 期望版本与当前版本不一致 |
| 422 | `validation_error` | 报文/参数结构或类型校验失败 |

## 目录结构

```
app/            FastAPI 应用（config/db/errors/schemas/service/routers/main）
tests/          19 项 pytest 场景/并发测试 + 真实 HTTP 端到端冒烟脚本
Dockerfile      应用镜像（含 healthcheck）
docker-compose.yml  api×2 + nginx + 持久化卷
nginx.conf      多副本负载均衡
```
