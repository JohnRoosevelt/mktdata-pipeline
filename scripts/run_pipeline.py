"""管道入口：采集 -> 落 Parquet -> DuckDB 分析。

用法：
  python scripts/run_pipeline.py --mode sim --symbol BTCUSDT --n 2000
  python scripts/run_pipeline.py --mode live --symbol BTCUSDT --n 500   # 需网络
"""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mktdata.analyze import report
from mktdata.schema import Quote
from mktdata.sink import write_parquet
from mktdata.sources import live_stream, sim_stream


async def collect(source) -> list[Quote]:
    return [q async for q in source]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["sim", "live"], default="sim")
    p.add_argument("--symbol", default="BTCUSDT")
    p.add_argument("--n", type=int, default=2000)
    p.add_argument("--out", default="data/quotes.parquet")
    args = p.parse_args()

    source = (
        live_stream(args.symbol, args.n)
        if args.mode == "live"
        else sim_stream(args.symbol, args.n)
    )
    quotes = asyncio.run(collect(source))
    out = write_parquet(quotes, args.out)
    print(f"wrote {len(quotes)} ticks -> {out}")
    print(report(out))


if __name__ == "__main__":
    main()
