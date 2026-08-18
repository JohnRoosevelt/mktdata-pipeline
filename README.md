# mktdata-pipeline

行情数据管道练习项目：采集 -> 归一化 -> Parquet 落库 -> DuckDB 分析。
覆盖交易系统岗位 JD 里的 data pipeline 与 Data Warehouse 两个考点。

## 结构

```
src/mktdata/
  schema.py    归一化 Quote schema（所有数据源的统一出口）
  sources.py   行情源：sim（离线随机游走）/ live（Binance bookTicker WebSocket）
  sink.py      PyArrow 写 Parquet
  analyze.py   DuckDB 直读 Parquet 出统计
scripts/run_pipeline.py  端到端入口
```

## 运行

```bash
# 推荐 uv；没有 uv 就 python -m venv .venv && pip install -e .[dev]
uv venv && uv pip install -e .[dev]

# 离线模式（无网络也能跑）
python scripts/run_pipeline.py --mode sim --n 2000

# 真实行情（需网络，公开频道无需 API key）
python scripts/run_pipeline.py --mode live --symbol BTCUSDT --n 500

# 测试
pytest
```

## 学习点

1. 为什么时间戳用 int 纳秒、schema 固定？（见 schema.py 注释）
2. 为什么落 Parquet 不落 CSV？（见 sink.py 注释）
3. DuckDB 的 `read_parquet()` 为什么不搬数据就能分析？
4. async iterator 作为行情源接口的意义：sim 与 live 可互换，测试不用碰网络。

## 扩展 TODO

- [ ] 第二个数据源（如 OKX），同币种双源 spread 对比
- [ ] tick 重采样成 bar（1s/1m OHLC）
- [ ] 按小时分区落多个 Parquet 文件，模拟真实采集器
- [ ] 换 ClickHouse 做同样的分析，对比 DuckDB

## 状态

练习项目，初始骨架搭建于 2026-08；后续迭代提交见 git 历史。
