"""NATS publishing boundary for normalized market quotes."""

import json
from decimal import Decimal
from typing import Any

from mktdata.schema import Quote


def to_wire_quote(
    quote: Quote, price_scale: int, quantity_scale: int
) -> dict[str, int]:
    """Convert one normalized quote to integer-only NATS payload fields."""
    return {
        "ts_ns": quote.ts_ns,
        "bid_ticks": _to_units(quote.bid_px, price_scale),
        "ask_ticks": _to_units(quote.ask_px, price_scale),
        "bid_qty": _to_units(quote.bid_qty, quantity_scale),
        "ask_qty": _to_units(quote.ask_qty, quantity_scale),
        "price_scale": price_scale,
        "quantity_scale": quantity_scale,
    }


class NatsQuotePublisher:
    """Publishes normalized quotes to ``market.quotes.<symbol>``."""

    def __init__(self, client: Any, price_scale: int, quantity_scale: int) -> None:
        self._client = client
        self._price_scale = price_scale
        self._quantity_scale = quantity_scale

    async def publish(self, quote: Quote) -> None:
        payload = json.dumps(
            to_wire_quote(quote, self._price_scale, self._quantity_scale),
            separators=(",", ":"),
        ).encode()
        await self._client.publish(f"market.quotes.{quote.symbol}", payload)


def _to_units(value: float, scale: int) -> int:
    if scale < 0:
        raise ValueError("scale must be non-negative")
    scaled = Decimal(str(value)) * Decimal(10**scale)
    if scaled != scaled.to_integral_value() or scaled < 0:
        raise ValueError(
            f"value cannot be represented exactly at scale {scale}: {value}"
        )
    return int(scaled)
