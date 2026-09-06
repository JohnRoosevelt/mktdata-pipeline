"""mktdata-pipeline: 行情采集 -> 归一化 -> Parquet 落库 -> DuckDB 分析。"""

from mktdata.analyze import analyze
from mktdata.schema import Quote
from mktdata.sink import write_parquet
from mktdata.sources import live_stream, sim_stream

__all__ = ["Quote", "sim_stream", "live_stream", "write_parquet", "analyze"]
