"""管道入口：采集 -> 落 Parquet -> DuckDB 分析。

用法：
  python scripts/run_pipeline.py --mode sim --symbol BTCUSDT --n 2000
  python scripts/run_pipeline.py --mode live --symbol BTCUSDT --n 500   # 需网络
  python scripts/run_pipeline.py --mode kline --symbol SOLUSDT --n 43200
"""

import argparse
import asyncio
import logging
from pathlib import Path

from mktdata.analyze import report, report_klines
from mktdata.schema import Kline
from mktdata.sink import ParquetBatchWriter, write_klines, write_parquet
from mktdata.sources import kline_stream, live_stream, sim_stream

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


async def collect_live(symbol: str, n: int, out_dir: str, batch_size: int) -> None:
    """流式采集 + 分批落盘，不占全量内存。"""
    with ParquetBatchWriter(out_dir, batch_size=batch_size) as writer:
        async for quote in live_stream(symbol, n):
            writer.write(quote)
    print(f"live: wrote {writer._total_written} ticks -> {out_dir}/")


def main() -> None:
    p = argparse.ArgumentParser(description="mktdata pipeline")
    p.add_argument("--mode", choices=["sim", "live", "kline"], default="sim")
    p.add_argument("--symbol", default="BTCUSDT")
    p.add_argument("--n", type=int, default=None)
    p.add_argument("--interval", default="1m", help="kline 模式用;如 1m/1h/1d")
    p.add_argument("--out", default="data/quotes.parquet")
    p.add_argument("--batch-size", type=int, default=5000, help="live 模式分批刷盘条数")
    args = p.parse_args()

    if args.mode == "kline":
        n = args.n if args.n is not None else 43200
        klines: list[Kline] = asyncio.run(_collect(kline_stream(args.symbol, n, args.interval)))
        out = write_klines(klines, args.out)
        print(f"wrote {len(klines)} klines -> {out}")
        print(report_klines(out))
        return

    n = args.n if args.n is not None else 2000
    if args.mode == "live":
        out_dir = str(Path(args.out).parent)
        asyncio.run(collect_live(args.symbol, n, out_dir, args.batch_size))
        print(report(Path(out_dir) / "quotes_000.parquet"))
        return

    # sim：有限数据，全量写即可
    quotes = asyncio.run(_collect(sim_stream(args.symbol, n)))
    out = write_parquet(quotes, args.out)
    print(f"wrote {len(quotes)} ticks -> {out}")
    print(report(out))


async def _collect(source) -> list:  # type: ignore[type-arg]
    return [q async for q in source]


if __name__ == "__main__":
    main()
