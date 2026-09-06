# mktdata-pipeline

行情数据管道练习项目：采集 -> 归一化 -> Parquet 落库 -> DuckDB 分析。
覆盖交易系统岗位 JD 里的 data pipeline 与 Data Warehouse 两个考点。

## 与 Rust 风控/撮合服务联调

`publisher.py` 把归一化报价转换为全整数 JSON，并发布到
`market.quotes.<symbol>`；它与 `mini-orderbook-rs` 工作区中的 risk service
使用同一份 wire contract。可用模拟行情在本机演示完整执行链路：

```bash
# 终端 1（在 mini-orderbook-rs/ 下）
crates/matching-engine/.tools/nats/nats-server-v2.14.5-darwin-amd64/nats-server

# 终端 2（在 mini-orderbook-rs/ 下；此值是刻意设低的演示限额）
mkdir -p .runtime
RISK_MAX_NOTIONAL_TICKS=10000000 RISK_OUTBOX_PATH=.runtime/risk-outbox.jsonl \
  cargo run -p matching-engine --bin risk-service

# 终端 3（在 mini-orderbook-rs/ 下）
ENGINE_JOURNAL_PATH=.runtime/matching-journal.jsonl \
  cargo run -p matching-engine --bin matching-service

# 终端 4（mktdata-pipeline/ 下）
uv run python scripts/e2e_demo.py
```

最后一条会发布新鲜报价和三笔确定性订单，并验证两笔放行、一笔超限拒绝，
以及一条成交事件。实际采集也可同时落 Parquet 和发布：

```bash
uv run python scripts/run_pipeline.py --mode sim --symbol BTCUSDT --n 2000 \
  --nats-url nats://127.0.0.1:4222 --price-scale 4 --quantity-scale 4
```

同样的 `--nats-url` 参数可用于 `--mode live`。如果不传该参数，原有的
采集→Parquet 行为不变。

演示客户端也可作为本地 soak 验收器：每轮都会重新发布报价，避免因报价过期
掩盖服务问题；它只会在每轮三条风险结果和三条执行结果均符合预期时继续。

```bash
# 约 1 小时（7,200 轮，轮间 0.5 秒；服务启动方式同上）
uv run python scripts/e2e_demo.py --iterations 7200 --interval-seconds 0.5
```

传入 `RISK_OUTBOX_PATH` 后，risk service 会先将每条放行命令同步写入 outbox，
重启后从 outbox 恢复下一个执行序号并重放记录；matching service 对旧序号幂等
忽略。传入 `ENGINE_JOURNAL_PATH` 后，matching service 也会恢复订单簿状态。
Core NATS 的单次发布仍是 at-most-once；该 outbox 只覆盖 risk 进程重启后的
重放。生产级投递保证仍应使用具确认、保留和监控能力的 JetStream 或等价持久
消息系统。

## 结构

```
src/mktdata/
  schema.py    归一化 Quote schema（所有数据源的统一出口）
  sources.py   行情源：sim（离线随机游走）/ live（Binance bookTicker + 指数退避重连）
  sink.py      PyArrow 写 Parquet + ParquetBatchWriter（分批刷盘）
  analyze.py   DuckDB 直读 Parquet 出统计
scripts/run_pipeline.py  端到端入口
```

## 运行

```bash
# 推荐：uv 会按 uv.lock 同步项目和开发工具
uv sync --all-groups

# 离线模式（无网络也能跑）
uv run mktdata --mode sim --n 2000

# 真实行情（需网络，公开频道无需 API key，带重连退避）
uv run mktdata --mode live --symbol BTCUSDT --n 500

# 真实行情 + 分批落盘（大流量时用，每 5000 条刷一个文件）
uv run mktdata --mode live --symbol BTCUSDT --n 50000 --batch-size 5000

# 测试
uv run pytest

# Lint / 类型检查
uv run ruff check .
uv run mypy src

# 不使用 uv 时的 venv + pip 备用安装方式
python -m venv .venv
source .venv/bin/activate
python -m pip install -e . pytest ruff mypy
```

## 工程化要点（面试能讲清楚的）

- **aiohttp 替换阻塞 urllib**：REST 请求不再卡住事件循环；`kline_stream` 内全程 async
- **指数退避重连**：WebSocket 断开后 1s→2s→4s... 最大 60s，最多重试 10 次，连接成功重置计数
- **分批 flush 落盘**：`ParquetBatchWriter` 攒满 batch_size 条后刷写新文件，不占全量内存
- **面向接口编程**：sim/live/kline 三源均实现 `AsyncIterator`，消费端 `async for` 不关心来源
- **类型安全**：ruff + mypy strict 门禁；`sys.path.insert` hack 已移除

## 学习点（先看问题自己答，再对答案）

1. 为什么时间戳用 int 纳秒、schema 固定？

   **答案**：不同数据源的时间精度和时区各不同（秒/毫秒/带时区字符串），统一成 int 纳秒就消灭了歧义；定长整数在列式存储里对齐整齐、压缩友好、比较排序快。schema 固定意味着下游 Parquet 和 SQL 只认一种结构，新增数据源只需写一个 adapter。datetime 对象留给展示层，存储和计算用整数。

2. 为什么落 Parquet 不落 CSV？

   **答案**：列式存储同列同类型、数值相近，压缩率高（行情数据常到 1/10）；分析只读用到的列——算 spread 只扫 bid_px/ask_px 两列，CSV 却要解析每行全部字段；schema 随文件自带，字段类型不用下游猜。CSV 是无类型行式文本，SQLite 是行式 OLTP，都不适合分析扫描。

3. DuckDB 的 `read_parquet()` 为什么不搬数据就能分析？

   **答案**：DuckDB 是进程内列式分析引擎，它的扫描层原生理解 Parquet 这种列式文件格式——文件本身就是它的"存储"。存储和计算分离、把计算推到存储旁边，省去 ETL 入库步骤，这正是现代轻量 data warehouse 的典型形态（同思路的还有 ClickHouse 读 Parquet、Trino 等）。

4. async iterator 作为行情源接口的意义：sim 与 live 可互换，测试不用碰网络。

   **答案**：sim 和 live 都实现成 `AsyncIterator[Quote]`，消费端 `async for` 不关心背后是谁。好处：测试与离线开发跑 sim，不碰网络；切真实行情只改一个参数；以后加 OKX 等新源，实现同一接口即可。这就是"面向接口编程"在 Python async 协议里的落地，和 Rust 里用 trait 定义数据源接口是同一个思想。

5. 重连退避为什么用指数退避而不是固定间隔？

   **答案**：交易所维护/网络波动通常是短暂的，固定间隔（如每 5s）会在高峰期造成"重连风暴"——大量客户端同时重连又同时断开。指数退避让客户端错开重连时间，降低服务端压力；上限 60s 避免退避过长导致长时间失联。

## 扩展 TODO

- [ ] 第二个数据源（如 OKX），同币种双源 spread 对比
- [ ] tick 重采样成 bar（1s/1m OHLC）
- [ ] 按小时分区落多个 Parquet 文件，模拟真实采集器
- [ ] 换 ClickHouse 做同样的分析，对比 DuckDB

## 状态

练习项目，初始骨架搭建于 2026-08；2026-08 工程化酸洗完成（aiohttp、重连、分批落盘、ruff/mypy）；后续迭代提交见 git 历史。
