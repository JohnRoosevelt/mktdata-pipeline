import asyncio
import subprocess
import sys
from pathlib import Path

from mktdata.analyze import analyze
from mktdata.cli import main
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


def test_cli_sim_writes_parquet(tmp_path, monkeypatch):
    out = tmp_path / "q.parquet"
    monkeypatch.setattr(
        sys,
        "argv",
        ["mktdata", "--mode", "sim", "--n", "3", "--out", str(out)],
    )
    main()
    assert out.exists()


def test_module_launcher_exports_main():
    from mktdata.__main__ import main as module_main

    assert module_main is main


def test_module_launcher_help():
    result = subprocess.run(
        [sys.executable, "-m", "mktdata", "--help"],
        cwd=Path(__file__).parents[1],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "mktdata pipeline" in result.stdout


def test_script_launcher_help():
    result = subprocess.run(
        [sys.executable, "scripts/run_pipeline.py", "--help"],
        cwd=Path(__file__).parents[1],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "mktdata pipeline" in result.stdout


async def _collect(source):
    return [q async for q in source]
