"""Parquet 落库：行情批量写列式文件。

学习点：为什么是 Parquet 而不是 CSV/SQLite？
- 列式存储：同列类型一致、数值相近，压缩率高（行情数据常能压到 1/10）。
- 分析只读用到的列：算 spread 只扫 bid_px/ask_px 两列，I/O 小几个数量级。
- 自带 schema：字段类型随文件走，下游不用猜。
"""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from mktdata.schema import Quote

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


def write_parquet(quotes: list[Quote], path: str | Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    cols: dict[str, list] = {name: [] for name in SCHEMA.names}
    for q in quotes:
        d = q.to_dict()
        for name in SCHEMA.names:
            cols[name].append(d[name])
    table = pa.Table.from_pydict(cols, schema=SCHEMA)
    pq.write_table(table, path)
    return path
