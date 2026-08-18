"""行情源：async iterator 统一接口。

sim_stream：本地生成随机游走行情，无网络依赖，用于离线开发与测试。
live_stream：接 Binance 公开 bookTicker 频道（无需 API key），
归一化成 Quote——新增其他交易所时，照这个函数写 adapter 即可。
"""

import asyncio
import json
import random
import time
from typing import AsyncIterator

from mktdata.schema import Quote

BINANCE_BOOK_TICKER_WS = "wss://stream.binance.com:9443/ws"


async def sim_stream(
    symbol: str,
    n: int = 2000,
    start_px: float = 100.0,
    step_ms: float = 200.0,
    seed: int = 42,
) -> AsyncIterator[Quote]:
    """随机游走生成买卖双边报价。step_ms 为 tick 间隔（模拟真实节奏）。"""
    rng = random.Random(seed)
    px = start_px
    t0 = time.time_ns()
    step_ns = int(step_ms * 1e6)
    for i in range(n):
        px = max(1.0, px + rng.gauss(0.0, 0.05))
        half = rng.uniform(0.005, 0.025)
        yield Quote(
            ts_ns=t0 + i * step_ns,
            symbol=symbol,
            bid_px=round(px - half, 4),
            bid_qty=round(rng.uniform(1, 50), 2),
            ask_px=round(px + half, 4),
            ask_qty=round(rng.uniform(1, 50), 2),
            source="sim",
        )
        await asyncio.sleep(0)  # 让出事件循环，模拟真实 async 源


async def live_stream(symbol: str, n: int = 1000) -> AsyncIterator[Quote]:
    """Binance 现货 bookTicker（最优买卖档）实时流。"""
    import websockets

    url = f"{BINANCE_BOOK_TICKER_WS}/{symbol.lower()}@bookTicker"
    async with websockets.connect(url) as ws:
        received = 0
        async for raw in ws:
            msg = json.loads(raw)
            yield Quote(
                ts_ns=time.time_ns(),
                symbol=symbol,
                bid_px=float(msg["b"]),
                bid_qty=float(msg["B"]),
                ask_px=float(msg["a"]),
                ask_qty=float(msg["A"]),
                source="binance",
            )
            received += 1
            if received >= n:
                break
