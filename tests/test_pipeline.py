import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mktdata.analyze import analyze
from mktdata.sink import write_parquet
from mktdata.sources import sim_stream


def test_sim_to_parquet_to_analysis(tmp_path):
    quotes = asyncio.run(_collect(sim_stream("TEST", 500)))
    assert len(quotes) == 500
    out = write_parquet(quotes, tmp_path / "q.parquet")
    res = analyze(out)
    assert res["summary"]["n_ticks"] == 500
    assert res["summary"]["avg_spread"] > 0
    assert res["per_minute"], "per-minute aggregation should not be empty"


async def _collect(source):
    return [q async for q in source]
