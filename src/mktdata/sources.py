"""行情源：async iterator 统一接口。

sim_stream：本地生成随机游走行情，无网络依赖，用于离线开发与测试。
live_stream：接 Binance 公开 bookTicker 频道（无需 API key），
归一化成 Quote——新增其他交易所时，照这个函数写 adapter 即可。
重连策略：WebSocket 断开后指数退避重试（最多 10 次，1s-60s）。
"""

import asyncio
import json
import logging
import random
import time
from typing import AsyncIterator

import aiohttp

from mktdata.schema import Kline, Quote

logger = logging.getLogger(__name__)

BINANCE_BOOK_TICKER_WS = "wss://stream.binance.com:9443/ws"
BINANCE_KLINES_REST = "https://api.binance.com/api/v3/klines"

_MAX_WS_RETRIES = 10
_BASE_BACKOFF_S = 1.0
_MAX_BACKOFF_S = 60.0


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
        await asyncio.sleep(0)


async def live_stream(symbol: str, n: int = 1000) -> AsyncIterator[Quote]:
    """Binance 现货 bookTicker（最优买卖档）实时流，带指数退避重连。"""
    import websockets

    url = f"{BINANCE_BOOK_TICKER_WS}/{symbol.lower()}@bookTicker"
    received = 0
    retries = 0

    while received < n and retries < _MAX_WS_RETRIES:
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                retries = 0  # 连接成功，重置重试计数
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
        except Exception:
            retries += 1
            backoff = min(_BASE_BACKOFF_S * (2 ** (retries - 1)), _MAX_BACKOFF_S)
            logger.warning(
                "WS 断开，%d/%d 次重连，%.1fs 后重试",
                retries,
                _MAX_WS_RETRIES,
                backoff,
            )
            await asyncio.sleep(backoff)

    if retries >= _MAX_WS_RETRIES and received < n:
        logger.error("重连次数耗尽，已收 %d/%d 条", received, n)


async def kline_stream(symbol: str, n: int = 43200, interval: str = "1m") -> AsyncIterator[Kline]:
    """Binance 历史 K 线(走 aiohttp,拉最近 n 根)。

    1m K 线一个月约 43200 根。Binance 单次拉取上限 1000,
    这里按升序 endTime 分页循环,直到收满 n 根。
    为了接口一致(sim/live/kline 三源可互换),仍做成 async generator。
    """
    params = {"symbol": symbol, "interval": interval, "limit": 1000}
    collected = 0
    end_time: int | None = None

    async with aiohttp.ClientSession() as session:
        while collected < n:
            req_params = dict(params)
            if end_time is not None:
                req_params["endTime"] = end_time

            async with session.get(BINANCE_KLINES_REST, params=req_params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                rows = await resp.json()

            if not rows:
                break
            for row in rows:
                yield _row_to_kline(row, symbol, interval)
                collected += 1
                if collected >= n:
                    break
            end_time = rows[0][0] - 1
            await asyncio.sleep(0)


def _row_to_kline(row: list, symbol: str, interval: str) -> Kline:
    """Binance kline 行 -> Kline。row: [openTime,open,high,low,close,volume,...,trades,...]"""
    return Kline(
        open_ts_ns=int(row[0]) * 1_000_000,
        symbol=symbol,
        open=float(row[1]),
        high=float(row[2]),
        low=float(row[3]),
        close=float(row[4]),
        volume=float(row[5]),
        quote_volume=float(row[7]),
        trades=int(row[8]),
        source=f"binance:{interval}",
    )
