"""归一化行情 schema：不同数据源统一成同一种 Quote。

为什么这么设计（学习点）：
- 时间戳统一 int 纳秒：避免各源时区/精度差异，落列式存储时是定长整数，压缩友好。
- schema 固定：下游 Parquet/分析只认一种结构，新增数据源只需写一个 adapter。
- dataclass(frozen=True)：行情是不可变事实，防误改。
"""

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class Quote:
    ts_ns: int
    symbol: str
    bid_px: float
    bid_qty: float
    ask_px: float
    ask_qty: float
    source: str

    def to_dict(self) -> dict:
        return asdict(self)
