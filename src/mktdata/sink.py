"""Parquet 落库：行情批量写列式文件。

学习点：为什么是 Parquet 而不是 CSV/SQLite？
- 列式存储：同列类型一致、数值相近，压缩率高（行情数据常能压到 1/10）。
- 分析只读用到的列：算 spread 只扫 bid_px/ask_px 两列，I/O 小几个数量级。
- 自带 schema：字段类型随文件走，下游不用猜。

新增 ParquetBatchWriter：攒满 batch_size 条后刷盘，避免全量内存收集。
"""

import logging
from pathlib import Path

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from mktdata.schema import Kline, Quote

logger = logging.getLogger(__name__)

SCHEMA = pa.schema(
    [
        ("ts_ns", pa.int64()),
        ("symbol", pa.string()),
        ("bid_px", pa.float64()),
        ("bid_qty", pa.float64()),
        ("ask_px", pa.float64()),
        ("ask_qty", pa.float64()),
        ("source", pa.string()),
    ]
)

KLINE_SCHEMA = pa.schema(
    [
        ("open_ts_ns", pa.int64()),
        ("symbol", pa.string()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.float64()),
        ("quote_volume", pa.float64()),
        ("trades", pa.int64()),
        ("source", pa.string()),
    ]
)


def write_parquet(quotes: list[Quote], path: str | Path) -> Path:
    """一次性写入多条 Quote（兼容旧接口，内部自动 schema 对齐）。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    cols: dict[str, list[object]] = {name: [] for name in SCHEMA.names}
    for q in quotes:
        d = q.to_dict()
        for name in SCHEMA.names:
            cols[name].append(d[name])
    table = pa.Table.from_pydict(cols, schema=SCHEMA)
    pq.write_table(table, path)
    return path


def write_klines(klines: list[Kline], path: str | Path) -> Path:
    """一次性写入多条 Kline。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    cols: dict[str, list[object]] = {name: [] for name in KLINE_SCHEMA.names}
    for k in klines:
        d = k.to_dict()
        for name in KLINE_SCHEMA.names:
            cols[name].append(d[name])
    table = pa.Table.from_pydict(cols, schema=KLINE_SCHEMA)
    pq.write_table(table, path)
    return path


class ParquetBatchWriter:
    """流式分批落盘：攒满 batch_size 条后刷写到新 parquet 文件。

    用法::

        writer = ParquetBatchWriter(base_path, batch_size=5000)
        async for quote in live_stream("BTCUSDT"):
            writer.write(quote)
        writer.close()

    或用上下文管理::

        with ParquetBatchWriter(base_path, batch_size=5000) as writer:
            for q in quotes:
                writer.write(q)
    """

    def __init__(self, base_path: str | Path, batch_size: int = 5000) -> None:
        self._base_path = Path(base_path)
        self._base_path.mkdir(parents=True, exist_ok=True)
        self._batch_size = batch_size
        self._buffer: list[Quote] = []
        self._file_index = 0
        self._total_written = 0

    def write(self, quote: Quote) -> None:
        """写入一条 Quote；攒满 batch_size 时自动刷盘。"""
        self._buffer.append(quote)
        if len(self._buffer) >= self._batch_size:
            self.flush()

    def flush(self) -> None:
        """将当前 buffer 刷写到磁盘（不满 batch 也刷）。"""
        if not self._buffer:
            return
        file_path = self._base_path / f"quotes_{self._file_index:03d}.parquet"
        cols: dict[str, list[object]] = {name: [] for name in SCHEMA.names}
        for q in self._buffer:
            d = q.to_dict()
            for name in SCHEMA.names:
                cols[name].append(d[name])
        table = pa.Table.from_pydict(cols, schema=SCHEMA)
        pq.write_table(table, file_path)
        logger.info("刷写 %d 条 -> %s", len(self._buffer), file_path.name)
        self._total_written += len(self._buffer)
        self._buffer.clear()
        self._file_index += 1

    def close(self) -> None:
        """刷完剩余并关闭。"""
        self.flush()
        logger.info(
            "落盘完成：共 %d 条，%d 个文件", self._total_written, self._file_index
        )

    def __enter__(self) -> "ParquetBatchWriter":
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self.close()
