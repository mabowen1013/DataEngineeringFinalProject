# Real-time Trade Analysis Pipeline - Architecture Plan

## Context

Duke Data Engineering 课程 Final Project #5。需要构建一个端到端数据管道，整合 Finnhub 的两个数据源（实时交易流 + 公司新闻），生成可供下游分析的 curated 数据集。项目必须部署到树莓派 K8s 集群（3 nodes），使用 Apache Airflow 编排，本地用 Docker Compose 开发。

---

## 1. 整体架构

```
                    ┌─────────────────────────────────────────────┐
                    │            Finnhub APIs                     │
                    │  WebSocket (trades)    REST (company news)  │
                    └──────┬──────────────────────┬───────────────┘
                           │                      │
                    ┌──────▼──────┐      ┌────────▼────────┐
                    │   Trade     │      │  News Ingestion │
                    │  Ingestion  │      │   (Airflow DAG) │
                    │  Service    │      │  每15~30分钟运行  │
                    │ (长驻进程)   │      └────────┬────────┘
                    └──────┬──────┘               │
                           │                      │
                    ┌──────▼──────────────────────▼───────┐
                    │           Raw Layer                  │
                    │  Minio S3: 原始归档 (Parquet/JSON)   │
                    │  Postgres: raw_trades, raw_news     │
                    └──────────────────┬──────────────────┘
                                      │
                              ┌───────▼────────┐
                              │ Data Curation  │
                              │ (Airflow DAG)  │
                              │  每小时运行     │
                              └───────┬────────┘
                                      │
                    ┌─────────────────▼──────────────────┐
                    │         Curated Layer               │
                    │  Postgres: trade_summaries,         │
                    │  news_trade_windows, company_events │
                    └────────────────────────────────────┘
                                      │
                              ┌───────▼────────┐
                              │  Downstream    │
                              │  (不在scope内)  │
                              └────────────────┘
```

### 核心组件

| 组件 | 类型 | 说明 |
|------|------|------|
| Trade Ingestion Service | K8s Deployment (长驻进程) | WebSocket 客户端，持续接收交易数据 |
| News Ingestion DAG | Airflow DAG | 定期拉取公司新闻 |
| Data Curation DAG | Airflow DAG | 清洗、聚合、关联交易与新闻 |
| Trade Health Check DAG | Airflow DAG | 监控交易ingestion服务健康状态 |
| Postgres | K8s StatefulSet | 结构化存储（raw + curated） |
| Minio S3 | K8s StatefulSet | 原始数据归档（数据湖） |
| Airflow | K8s Deployment | 编排所有批处理任务 |

---

## 2. 关键设计决策

### WebSocket 交易流 vs Airflow 批处理的矛盾

Airflow 是批处理编排工具，不适合管理持续运行的 WebSocket 连接。解决方案：

- **Trade Ingestion Service 作为独立的 K8s Deployment 运行**（不是 Airflow task）
- 该服务持续连接 WebSocket，每 1~2 分钟将积累的交易数据批量写入：
  - Minio S3：Parquet 文件归档
  - Postgres `raw_trades` 表：供后续 DAG 查询
- Airflow 通过 `trade_health_check_dag` 监控该服务是否正常

### 存储策略

| 存储 | 用途 | 理由 |
|------|------|------|
| **Minio S3** | 原始数据归档（trades Parquet + news JSON） | 低成本持久存储，支持 Historical Replay，解耦存储与计算 |
| **Postgres** | Raw 层 + Curated 层的结构化数据 | SQL 天然适合时间窗口聚合和 JOIN 操作，JSONB 足以处理新闻的半结构化字段 |

**为什么不用 MongoDB？** 在 3 个树莓派节点上同时运行 Postgres + MongoDB + Minio + Airflow 资源压力太大。Postgres 的 JSONB 类型足以处理新闻数据的灵活 schema。Defense 时的说法：*"我们评估了 MongoDB 用于新闻存储，但选择 Postgres 以降低集群的资源负担。Postgres JSONB 提供了足够的灵活性，同时 SQL JOIN 是 curated 层的核心需求。"*

---

## 3. 数据模型 (Postgres)

### Raw Layer

```sql
-- 原始交易数据
CREATE TABLE raw_trades (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    price DECIMAL(12,4) NOT NULL,
    volume DECIMAL(18,4) NOT NULL,
    trade_timestamp TIMESTAMPTZ NOT NULL,
    conditions JSONB,                    -- 交易条件代码
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);
-- 索引：按 symbol + 时间查询
CREATE INDEX idx_trades_symbol_ts ON raw_trades(symbol, trade_timestamp);
-- 去重：同一时间同一价格同一量的交易视为重复
CREATE UNIQUE INDEX idx_trades_dedup ON raw_trades(symbol, trade_timestamp, price, volume);

-- 原始新闻数据
CREATE TABLE raw_news (
    id BIGSERIAL PRIMARY KEY,
    article_id BIGINT UNIQUE NOT NULL,   -- Finnhub 文章 ID，用于去重
    symbol VARCHAR(10) NOT NULL,
    headline TEXT NOT NULL,
    summary TEXT,
    source VARCHAR(100),
    url TEXT,
    published_at TIMESTAMPTZ NOT NULL,
    category VARCHAR(50),
    raw_json JSONB,                      -- 完整原始响应，保留扩展性
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_news_symbol_published ON raw_news(symbol, published_at);
```

### Curated Layer

```sql
-- 交易摘要（按 symbol + 时间窗口聚合）
CREATE TABLE trade_summaries (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    open_price DECIMAL(12,4),
    close_price DECIMAL(12,4),
    high_price DECIMAL(12,4),
    low_price DECIMAL(12,4),
    total_volume DECIMAL(18,4),
    trade_count INT,
    vwap DECIMAL(12,4),                  -- 成交量加权平均价
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(symbol, window_start, window_end)
);

-- 核心 curated 表：新闻事件 + 前后交易窗口
CREATE TABLE news_trade_windows (
    id BIGSERIAL PRIMARY KEY,
    news_id BIGINT REFERENCES raw_news(id),
    symbol VARCHAR(10) NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    -- 窗口定义（可调整）
    pre_window_minutes INT DEFAULT 15,
    post_window_minutes INT DEFAULT 60,
    -- 发布前交易统计
    pre_trade_count INT,
    pre_total_volume DECIMAL(18,4),
    pre_vwap DECIMAL(12,4),
    -- 发布后交易统计
    post_trade_count INT,
    post_total_volume DECIMAL(18,4),
    post_vwap DECIMAL(12,4),
    -- 变化指标
    price_change_pct DECIMAL(8,4),
    volume_change_pct DECIMAL(8,4),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 公司事件统一视图
CREATE TABLE company_events (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    event_date DATE NOT NULL,
    news_count INT DEFAULT 0,
    trade_count INT DEFAULT 0,
    total_volume DECIMAL(18,4),
    avg_price DECIMAL(12,4),
    price_range_pct DECIMAL(8,4),       -- (high-low)/low
    top_headlines JSONB,                 -- 当日主要新闻标题
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(symbol, event_date)
);
```

---

## 4. Airflow DAGs 设计

### DAG 1: `news_ingestion_dag`
- **调度**: 每 30 分钟 (`*/30 * * * *`)
- **Tasks**:
  1. `fetch_news` — 对每个跟踪的 symbol 调用 Finnhub Company News API
  2. `dedup_and_validate` — 按 article_id 去重，校验必填字段
  3. `store_to_s3` — 原始 JSON 存入 Minio S3
  4. `store_to_postgres` — 清洗后数据写入 `raw_news`
- **依赖**: fetch → dedup → [store_s3, store_postgres] (并行)

### DAG 2: `data_curation_dag`
- **调度**: 每小时 (`0 * * * *`)
- **Tasks**:
  1. `build_trade_summaries` — 将最近一小时的 raw_trades 聚合为 5 分钟窗口的 trade_summaries
  2. `build_news_trade_windows` — 找到新的 news 文章，计算其前后交易窗口的统计数据
  3. `update_company_events` — 更新当日的 company_events 汇总
  4. `data_quality_check` — 检查数据完整性（是否有 symbol 缺失交易数据等）
- **依赖**: [trade_summaries, news_trade_windows] → company_events → quality_check

### DAG 3: `trade_health_check_dag`
- **调度**: 每 5 分钟 (`*/5 * * * *`)
- **Tasks**:
  1. `check_recent_trades` — 查询 Postgres 最近 5 分钟是否有新交易写入
  2. `alert_if_stale` — 如果无数据，记录告警日志（可扩展为发送通知）

---

## 5. Trade Ingestion Service 设计

```python
# 核心逻辑伪代码
class TradeIngestionService:
    def __init__(self, symbols, finnhub_token, db_url, s3_client):
        self.symbols = symbols          # ["AAPL", "MSFT", "AMZN", ...]
        self.buffer = []                # 内存缓冲区
        self.flush_interval = 60        # 每60秒刷新一次

    async def on_message(self, msg):
        """处理每条 WebSocket 消息"""
        trades = parse_trades(msg)
        for trade in trades:
            validate_and_clean(trade)    # 处理缺失字段
            self.buffer.append(trade)

    async def flush_buffer(self):
        """定期将缓冲区写入存储"""
        if not self.buffer:
            return
        # 1. 写入 Postgres（INSERT ON CONFLICT DO NOTHING 处理重复）
        batch_insert_to_postgres(self.buffer)
        # 2. 写入 Minio S3 (Parquet 格式)
        write_parquet_to_s3(self.buffer)
        self.buffer.clear()

    async def run(self):
        """主循环"""
        ws = connect_websocket(self.finnhub_token)
        for symbol in self.symbols:
            ws.subscribe(symbol)
        # 并行：监听消息 + 定时刷新
        await asyncio.gather(
            self.listen(ws),
            self.periodic_flush()
        )
```

### 数据质量处理
- **乱序交易**: 使用 `trade_timestamp`（Finnhub 提供的时间戳）而非 ingestion 时间排序
- **缺失字段**: `conditions` 可为空，`price/volume/symbol/timestamp` 缺失则丢弃并记录日志
- **重复**: Postgres UNIQUE INDEX 自动去重（INSERT ON CONFLICT DO NOTHING）
- **新闻重复**: 按 `article_id` 去重

---

## 6. Minio S3 数据组织

```
s3://trade-pipeline/
├── raw/
│   ├── trades/
│   │   └── {date}/
│   │       └── {symbol}/
│   │           └── {timestamp}.parquet
│   └── news/
│       └── {date}/
│           └── {symbol}/
│               └── {article_id}.json
└── curated/
    └── (预留，curated 数据主要在 Postgres)
```

---

## 7. K8s 部署方案（3 个树莓派节点）

### 资源分配建议

| 节点 | 运行的主要工作负载 | 说明 |
|------|-------------------|------|
| Node 1 | Airflow (webserver + scheduler) | Airflow 最吃资源 |
| Node 2 | Postgres + Trade Ingestion | 数据库 + 写入服务放一起减少网络延迟 |
| Node 3 | Minio | 对象存储独占节点 |

*注意：K8s 会自动调度，以上通过 nodeAffinity 实现建议性分配。*

### K8s 资源清单

```
k8s/
├── namespace.yaml                    # trade-pipeline namespace
├── secrets.yaml                      # Finnhub API key, DB 密码, Minio 密码
├── postgres/
│   ├── statefulset.yaml             # Postgres 15 (ARM64 镜像)
│   ├── service.yaml                 # ClusterIP
│   ├── pvc.yaml                     # 持久化存储
│   └── init-configmap.yaml          # 初始化 SQL (建表)
├── minio/
│   ├── statefulset.yaml             # Minio (ARM64 镜像)
│   ├── service.yaml                 # ClusterIP + NodePort(console)
│   └── pvc.yaml
├── airflow/
│   ├── webserver-deployment.yaml
│   ├── scheduler-deployment.yaml
│   ├── service.yaml                 # NodePort (访问 Web UI)
│   └── configmap.yaml              # airflow.cfg, connections
├── trade-ingestion/
│   └── deployment.yaml              # 长驻 WebSocket 客户端
└── configmaps/
    └── pipeline-config.yaml         # symbols 列表, 窗口参数等
```

### ARM64 镜像注意事项
树莓派是 ARM 架构，需要确保所有 Docker 镜像支持 `linux/arm64`：
- Postgres: `arm64v8/postgres:15`
- Minio: `minio/minio` (官方支持 ARM64)
- Airflow: `apache/airflow:2.8.x` (官方支持 ARM64)
- Trade Ingestion: 自己构建，Dockerfile 用 `FROM python:3.11-slim` (支持 ARM64)

---

## 8. 本地开发环境 (Docker Compose)

与 K8s 部署使用**相同的 Docker 镜像**，只是编排方式不同：

```yaml
# docker-compose.yaml 核心结构
services:
  postgres:
    image: postgres:15
    volumes: [postgres_data:/var/lib/postgresql/data]
    environment: [POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD]

  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    volumes: [minio_data:/data]

  airflow-init:      # 初始化 Airflow DB
  airflow-webserver: # Airflow Web UI (localhost:8080)
  airflow-scheduler: # Airflow 调度器

  trade-ingestion:   # WebSocket 客户端
    build: ./services/trade_ingestion
    depends_on: [postgres, minio]
```

**本地开发流程**：
1. `docker-compose up -d` 启动所有服务
2. 访问 `localhost:8080` 查看 Airflow UI
3. 访问 `localhost:9001` 查看 Minio Console
4. 修改 DAG 后自动同步（volume mount）
5. 验证通过后，构建 ARM64 镜像部署到集群

---

## 9. 下游分析方向建议

推荐选择 **"News-to-Market Reaction Analysis" + "Event-Centered Windows"**（它们紧密相关）：

- **具体且可演示**：可以清晰地展示"某条新闻发布后，交易量/价格如何变化"
- **直接使用两个数据源**：完美满足 "must use both sources together" 的要求
- **Event-centered window 概念明确**：defense 时容易解释
- **pipeline 已天然支持**：`news_trade_windows` 表就是为此设计的

---

## 10. 项目文件结构

```
final_project/
├── dags/                             # Airflow DAGs
│   ├── news_ingestion_dag.py
│   ├── data_curation_dag.py
│   └── trade_health_check_dag.py
├── services/
│   └── trade_ingestion/              # WebSocket 客户端服务
│       ├── Dockerfile
│       ├── requirements.txt
│       └── main.py
├── k8s/                              # K8s 部署清单
│   ├── namespace.yaml
│   ├── secrets.yaml
│   ├── postgres/
│   ├── minio/
│   ├── airflow/
│   ├── trade-ingestion/
│   └── configmaps/
├── sql/
│   └── init.sql                      # 建表 DDL
├── config/
│   └── config.yaml                   # 可配置参数(symbols, 窗口大小等)
├── docker-compose.yaml               # 本地开发
├── docs/
│   ├── requirements.md               # 需求规格文档
│   └── pipeline_design.md            # Pipeline 设计文档
└── README.md                         # Setup 说明
```

---

## 11. 实施步骤

### Phase 1: 基础设施 + 本地环境
1. 编写 `docker-compose.yaml`
2. 编写 `sql/init.sql`（建表）
3. 验证 Postgres + Minio + Airflow 本地能跑通

### Phase 2: Trade Ingestion Service
4. 实现 WebSocket 客户端 (`services/trade_ingestion/main.py`)
5. 实现 micro-batch 写入 Postgres + Minio
6. 编写 Dockerfile，集成到 docker-compose
7. 验证：运行后能在 Postgres 查到交易数据

### Phase 3: Airflow DAGs
8. 实现 `news_ingestion_dag.py`
9. 实现 `data_curation_dag.py`
10. 实现 `trade_health_check_dag.py`
11. 本地 Airflow UI 验证所有 DAG 正常运行

### Phase 4: K8s 部署清单
12. 编写所有 K8s YAML（namespace, statefulsets, deployments, services, configmaps, secrets）
13. 构建 ARM64 Docker 镜像（multi-arch build）
14. 部署到树莓派集群并验证

### Phase 5: 文档 + 收尾
15. 编写 requirements.md 和 pipeline_design.md
16. 所有文件添加 author 信息
17. 提交到 group repo (ADTG)

---

## 12. 验证方案

### 本地验证
1. `docker-compose up -d` → 所有容器正常启动
2. 检查 `raw_trades` 表有持续写入的数据
3. 手动触发 `news_ingestion_dag` → 检查 `raw_news` 表有数据
4. 手动触发 `data_curation_dag` → 检查 curated 表数据正确
5. 查询 `news_trade_windows` 验证新闻前后的交易统计合理

### 集群验证
1. `kubectl apply -f k8s/` 部署所有资源
2. `kubectl get pods -n trade-pipeline` 确认所有 pod Running
3. `kubectl port-forward svc/airflow-webserver 8080:8080` 访问 Airflow UI
4. 重复本地验证的数据检查步骤

### 关于 cluster 部署的实际建议
- 本地用 Docker Compose 完成**所有开发和测试**
- 提前写好全部 K8s YAML，本地可用 `kubectl --dry-run=client` 验证语法
- 到学校后只需：push 镜像到 registry → `kubectl apply` → 验证
- 预约 cluster 时间时，准备一份部署 checklist，目标 30 分钟内完成部署
