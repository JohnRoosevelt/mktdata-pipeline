"""管道入口：采集 -> 落 Parquet -> DuckDB 分析。

用法：
  python scripts/run_pipeline.py --mode sim --symbol BTCUSDT --n 2000
  python scripts/run_pipeline.py --mode live --symbol BTCUSDT --n 500   # 需网络
  python scripts/run_pipeline.py --mode kline --symbol SOLUSDT --n 43200
"""

import argparse
import asyncio
import logging
from collections.abc import AsyncIterator
from pathlib import Path
from typing import TypeVar

from mktdata.analyze import report, report_klines
from mktdata.publisher import NatsQuotePublisher
from mktdata.schema import Kline, Quote
from mktdata.sink import ParquetBatchWriter, write_klines, write_parquet
from mktdata.sources import kline_stream, live_stream, sim_stream

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

T = TypeVar("T")


async def collect_live(
    symbol: str,
    n: int,
    out_dir: str,
    batch_size: int,
    publisher: NatsQuotePublisher | None = None,
) -> None:
    """流式采集、可选发布到 NATS，并分批落盘。"""
    with ParquetBatchWriter(out_dir, batch_size=batch_size) as writer:
        async for quote in live_stream(symbol, n):
            if publisher is not None:
                await publisher.publish(quote)
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
    p.add_argument(
        "--nats-url", help="发布行情到该 NATS URL（例如 nats://127.0.0.1:4222）"
    )
    p.add_argument("--price-scale", type=int, default=4, help="NATS 行情价格小数精度")
    p.add_argument(
        "--quantity-scale", type=int, default=4, help="NATS 行情数量小数精度"
    )
    args = p.parse_args()

    if args.mode == "kline":
        n = args.n if args.n is not None else 43200
        klines: list[Kline] = asyncio.run(
            _collect(kline_stream(args.symbol, n, args.interval))
        )
        out = write_klines(klines, args.out)
        print(f"wrote {len(klines)} klines -> {out}")
        print(report_klines(out))
        return

    n = args.n if args.n is not None else 2000
    if args.mode == "live":
        out_dir = str(Path(args.out).parent)
        asyncio.run(
            collect_live_with_optional_nats(
                args.symbol,
                n,
                out_dir,
                args.batch_size,
                args.nats_url,
                args.price_scale,
                args.quantity_scale,
            )
        )
        print(report(Path(out_dir) / "quotes_000.parquet"))
        return

    # sim：有限数据，全量写即可
    quotes = asyncio.run(
        collect_with_optional_nats(
            sim_stream(args.symbol, n),
            args.nats_url,
            args.price_scale,
            args.quantity_scale,
        )
    )
    out = write_parquet(quotes, args.out)
    print(f"wrote {len(quotes)} ticks -> {out}")
    print(report(out))


async def _collect(source: AsyncIterator[T]) -> list[T]:
    return [q async for q in source]


async def collect_with_optional_nats(
    source: AsyncIterator[Quote],
    nats_url: str | None,
    price_scale: int,
    quantity_scale: int,
) -> list[Quote]:
    """Collect a finite source and mirror every quote to NATS when configured."""
    if nats_url is None:
        return await _collect(source)

    import nats

    client = await nats.connect(nats_url)
    try:
        publisher = NatsQuotePublisher(client, price_scale, quantity_scale)
        quotes: list[Quote] = []
        async for quote in source:
            await publisher.publish(quote)
            quotes.append(quote)
        await client.flush()
        return quotes
    finally:
        await client.close()


async def collect_live_with_optional_nats(
    symbol: str,
    n: int,
    out_dir: str,
    batch_size: int,
    nats_url: str | None,
    price_scale: int,
    quantity_scale: int,
) -> None:
    if nats_url is None:
        await collect_live(symbol, n, out_dir, batch_size)
        return

    import nats

    client = await nats.connect(nats_url)
    try:
        await collect_live(
            symbol,
            n,
            out_dir,
            batch_size,
            NatsQuotePublisher(client, price_scale, quantity_scale),
        )
        await client.flush()
    finally:
        await client.close()


if __name__ == "__main__":
    main()