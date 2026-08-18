"""DuckDB 分析：直接对 Parquet 跑 SQL，不搬数据。

学习点：DuckDB 是进程内列式分析引擎，`read_parquet()` 把文件当表用，
省去 ETL 入库步骤——这正是现代轻量 data warehouse 的典型姿势。
"""

from pathlib import Path

import duckdb

SUMMARY_SQL = """
select
    count(*)                                   as n_ticks,
    round(avg(ask_px - bid_px), 6)             as avg_spread,
    round(min(ask_px - bid_px), 6)             as min_spread,
    round(max(ask_px - bid_px), 6)             as max_spread,
    round(avg((ask_px - bid_px) / ((ask_px + bid_px) / 2)) * 10000, 4)
                                               as avg_spread_bps
from read_parquet(?)
"""

PER_MINUTE_SQL = """
select
    time_bucket(interval '1 minute', to_timestamp(ts_ns / 1e9)) as minute,
    count(*)                                  as ticks,
    round(avg(ask_px - bid_px), 6)            as avg_spread,
    round(avg(bid_qty + ask_qty), 2)          as avg_depth
from read_parquet(?)
group by 1
order by 1
"""


def analyze(parquet_path: str | Path) -> dict:
    con = duckdb.connect()
    try:
        row = con.execute(SUMMARY_SQL, [str(parquet_path)]).fetchone()
        summary = {
            "n_ticks": row[0],
            "avg_spread": row[1],
            "min_spread": row[2],
            "max_spread": row[3],
            "avg_spread_bps": row[4],
        }
        per_minute = [
            {"minute": str(m), "ticks": t, "avg_spread": s, "avg_depth": d}
            for (m, t, s, d) in con.execute(PER_MINUTE_SQL, [str(parquet_path)]).fetchall()
        ]
    finally:
        con.close()
    return {"summary": summary, "per_minute": per_minute}


def report(parquet_path: str | Path) -> str:
    res = analyze(parquet_path)
    lines = [f"== {Path(parquet_path).name} =="]
    s = res["summary"]
    lines.append(
        f"ticks={s['n_ticks']}  spread avg/min/max="
        f"{s['avg_spread']}/{s['min_spread']}/{s['max_spread']}  "
        f"avg={s['avg_spread_bps']} bps"
    )
    for m in res["per_minute"]:
        lines.append(
            f"  {m['minute']}  ticks={m['ticks']}  spread={m['avg_spread']}  depth={m['avg_depth']}"
        )
    return "\n".join(lines)
