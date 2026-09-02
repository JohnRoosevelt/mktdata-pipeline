"""DuckDB 分析：直接对 Parquet 跑 SQL，不搬数据。

学习点：DuckDB 是进程内列式分析引擎，`read_parquet()` 把文件当表用，
省去 ETL 入库步骤——这正是现代轻量 data warehouse 的典型姿势。
"""

from pathlib import Path
from typing import Any, cast

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


def analyze(parquet_path: str | Path) -> dict[str, Any]:
    con = duckdb.connect()
    try:
        row = cast(
            "tuple[Any, ...]", con.execute(SUMMARY_SQL, [str(parquet_path)]).fetchone()
        )
        summary = {
            "n_ticks": row[0],
            "avg_spread": row[1],
            "min_spread": row[2],
            "max_spread": row[3],
            "avg_spread_bps": row[4],
        }
        per_minute = [
            {"minute": str(m), "ticks": t, "avg_spread": s, "avg_depth": d}
            for (m, t, s, d) in con.execute(
                PER_MINUTE_SQL, [str(parquet_path)]
            ).fetchall()
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
            f"  {m['minute']}  ticks={m['ticks']}  "
            f"spread={m['avg_spread']}  depth={m['avg_depth']}"
        )
    return "\n".join(lines)


KLINE_MONTH_SQL = """
select
    count(*)                                     as n_klines,
    to_timestamp(min(open_ts_ns) / 1e9)          as first_ts,
    to_timestamp(max(open_ts_ns) / 1e9)          as last_ts,
    round(min(low), 2)                           as month_low,
    round(max(high), 2)                          as month_high,
    round(sum(volume), 2)                        as total_vol,
    round(sum(quote_volume), 2)                  as total_quote_vol
from read_parquet(?)
"""

KLINE_OPEN_CLOSE_SQL = """
select
    (select open from read_parquet(?) order by open_ts_ns limit 1)  as first_open,
    (select close from read_parquet(?) order by open_ts_ns desc limit 1) as last_close
"""

KLINE_DAILY_SQL = """
select
    date_trunc('day', to_timestamp(open_ts_ns / 1e9))              as day,
    round(first(open  order by open_ts_ns), 2)                     as open,
    round(max(high), 2)                                            as high,
    round(min(low), 2)                                             as low,
    round(last(close order by open_ts_ns), 2)                      as close,
    round(sum(volume), 2)                                          as volume
from read_parquet(?)
group by 1
order by 1
"""


def analyze_klines(parquet_path: str | Path) -> dict[str, Any]:
    con = duckdb.connect()
    try:
        m = cast(
            "tuple[Any, ...]",
            con.execute(KLINE_MONTH_SQL, [str(parquet_path)]).fetchone(),
        )
        oc = cast(
            "tuple[Any, ...]",
            con.execute(KLINE_OPEN_CLOSE_SQL, [str(parquet_path)] * 2).fetchone(),
        )
        first_open, last_close = oc[0], oc[1]
        ret = (
            None
            if first_open is None or first_open == 0
            else last_close / first_open - 1
        )
        summary = {
            "n_klines": m[0],
            "first_ts": str(m[1]),
            "last_ts": str(m[2]),
            "month_low": m[3],
            "month_high": m[4],
            "first_open": first_open,
            "last_close": last_close,
            "return_pct": None if ret is None else round(ret * 100, 2),
            "total_vol": m[5],
            "total_quote_vol": m[6],
        }
        daily = [
            {
                "day": str(day),
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            }
            for (day, open_price, high, low, close, volume) in con.execute(
                KLINE_DAILY_SQL, [str(parquet_path)]
            ).fetchall()
        ]
    finally:
        con.close()
    return {"summary": summary, "daily": daily}


def report_klines(parquet_path: str | Path) -> str:
    res = analyze_klines(parquet_path)
    s = res["summary"]
    p = Path(parquet_path).name
    lines = [f"== {p} =="]
    lines.append(
        f"{s['n_klines']} klines  {s['first_ts']} -> {s['last_ts']}"
    )
    lines.append(
        f"month: open={s['first_open']} close={s['last_close']} "
        f"ret={s['return_pct']}%  high={s['month_high']} low={s['month_low']}"
    )
    lines.append(
        f"volume={s['total_vol']} SOL  quote={s['total_quote_vol']:.0f} USDT"
    )
    for d in res["daily"]:
        lines.append(
            f"  {d['day']}  O={d['open']} H={d['high']} L={d['low']} C={d['close']}  "
            f"vol={d['volume']}  chg={(d['close']/d['open']-1)*100:+.2f}%"
        )
    return "\n".join(lines)
