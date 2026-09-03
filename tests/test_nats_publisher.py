import asyncio
import json

from mktdata.publisher import NatsQuotePublisher, to_wire_quote
from mktdata.schema import Quote


def test_quote_is_converted_to_integer_ticks_without_float_payloads():
    quote = Quote(
        ts_ns=123,
        symbol="BTCUSDT",
        bid_px=100.1234,
        bid_qty=2.5,
        ask_px=100.5678,
        ask_qty=3.25,
        source="sim",
    )

    assert to_wire_quote(quote, price_scale=4, quantity_scale=2) == {
        "ts_ns": 123,
        "bid_ticks": 1_001_234,
        "ask_ticks": 1_005_678,
        "bid_qty": 250,
        "ask_qty": 325,
        "price_scale": 4,
        "quantity_scale": 2,
    }


def test_publisher_uses_symbol_subject_and_integer_json():
    class Client:
        published: list[tuple[str, bytes]] = []

        async def publish(self, subject: str, payload: bytes) -> None:
            self.published.append((subject, payload))

    client = Client()
    quote = Quote(123, "BTCUSDT", 1.25, 2.5, 1.5, 3.25, "sim")

    publisher = NatsQuotePublisher(client, price_scale=2, quantity_scale=2)
    asyncio.run(publisher.publish(quote))

    assert client.published == [
        (
            "market.quotes.BTCUSDT",
            json.dumps(
                {
                    "ts_ns": 123,
                    "bid_ticks": 125,
                    "ask_ticks": 150,
                    "bid_qty": 250,
                    "ask_qty": 325,
                    "price_scale": 2,
                    "quantity_scale": 2,
                },
                separators=(",", ":"),
            ).encode(),
        )
    ]
