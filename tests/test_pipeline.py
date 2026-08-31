import asyncio

from mktdata.analyze import analyze
from mktdata.sink import ParquetBatchWriter, write_parquet
from mktdata.sources import sim_stream


def test_sim_to_parquet_to_analysis(tmp_path):
    quotes = asyncio.run(_collect(sim_stream("TEST", 500)))
    assert len(quotes) == 500
    out = write_parquet(quotes, tmp_path / "q.parquet")
    res = analyze(out)
    assert res["summary"]["n_ticks"] == 500
    assert res["summary"]["avg_spread"] > 0
    assert res["per_minute"], "per-minute aggregation should not be empty"


def test_batch_writer_flushes(tmp_path):
    quotes = asyncio.run(_collect(sim_stream("TEST", 12)))
    writer = ParquetBatchWriter(tmp_path, batch_size=5)
    for q in quotes:
        writer.write(q)
    assert writer._file_index == 2, "should have flushed 2 full batches"
    assert len(writer._buffer) == 2, "2 remaining in buffer"
    writer.close()
    assert writer._file_index == 3, "close should flush remaining"
    files = list(tmp_path.glob("quotes_*.parquet"))
    assert len(files) == 3
    total_rows = 0
    for f in files:
        res = analyze(f)
        total_rows += res["summary"]["n_ticks"]
    assert total_rows == 12


async def _collect(source):
    return [q async for q in source]
