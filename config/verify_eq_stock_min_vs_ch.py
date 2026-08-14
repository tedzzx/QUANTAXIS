#!/usr/bin/env python
# coding: utf-8
"""重拉 EQ 原始 1min/5min，与 ClickHouse 入库数据逐字段核对。

用于阶段 A 补充验收：验证「EQ getSync → quotes_to_ch_rows → INSERT」
链路是否保真（价格×10000、成交额元整数、时间戳对齐）。

用法:
  .venv/bin/python config/verify_eq_stock_min_vs_ch.py \\
    --codes 000001,600000 --start 2026-07-01 --end 2026-07-10
  .venv/bin/python config/verify_eq_stock_min_vs_ch.py \\
    --codes 000001 --freq 1min --start 2026-07-01 --end 2026-07-02
"""

from __future__ import annotations

import argparse
import datetime
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from QUANTAXIS.QAFetch import QAEq as eq
from QUANTAXIS.QAFetch.QAClickhouse import stock_format
from QUANTAXIS.QASU.save_ths_clickhouse import ClickHouseStorage

CHINA_TZ = ZoneInfo("Asia/Shanghai")
TABLE_BY_FREQ = {
    "1min": "quantaxis.stock_cn_1min",
    "5min": "quantaxis.stock_cn_5min",
}
FIELDS = ("open", "high", "low", "close", "volume", "total_turnover")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--codes",
        default="000001,600000",
        help="逗号分隔六位代码（默认 000001,600000）",
    )
    parser.add_argument("--start", required=True, help="起始，如 2026-07-01")
    parser.add_argument("--end", required=True, help="结束，如 2026-07-10")
    parser.add_argument(
        "--freq",
        choices=("1min", "5min", "both"),
        default="both",
        help="核对频率（默认 both）",
    )
    parser.add_argument(
        "--show-diff",
        type=int,
        default=10,
        help="每种差异最多打印条数（默认 10）",
    )
    return parser.parse_args()


def _codes(arg: str) -> list[str]:
    return [
        item.strip().split(".")[0]
        for item in arg.split(",")
        if item.strip()
    ]


def _parse_end(force_end: str) -> pd.Timestamp:
    stamp = pd.Timestamp(force_end)
    if len(force_end.strip()) <= 10:
        stamp = stamp + datetime.timedelta(hours=23, minutes=59, seconds=59)
    return stamp


def _load_ch_rows(
    clickhouse: ClickHouseStorage,
    table: str,
    order_book_ids: list[str],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> dict[tuple[str, datetime.datetime], tuple]:
    rows = clickhouse.client.execute(
        f"""
        SELECT datetime, order_book_id, open, high, low, close, volume, total_turnover
        FROM {table}
        WHERE order_book_id IN %(codes)s
          AND datetime >= %(start)s
          AND datetime <= %(end)s
        """,
        {
            "codes": tuple(order_book_ids),
            "start": start.to_pydatetime(),
            "end": end.to_pydatetime(),
        },
    )
    out: dict[tuple[str, datetime.datetime], tuple] = {}
    for row in rows:
        moment = row[0]
        if isinstance(moment, pd.Timestamp):
            moment = moment.to_pydatetime()
        key = (str(row[1]), moment)
        out[key] = tuple(int(row[i]) for i in range(2, 8))
    return out


def _eq_rows_to_map(
    rows: list[tuple],
) -> dict[tuple[str, datetime.datetime], tuple]:
    out: dict[tuple[str, datetime.datetime], tuple] = {}
    for row in rows:
        moment = row[0]
        if isinstance(moment, pd.Timestamp):
            moment = moment.to_pydatetime()
        key = (str(row[1]), moment)
        out[key] = tuple(int(row[i]) for i in range(2, 8))
    return out


def verify_one(
    code: str,
    frequence: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    clickhouse: ClickHouseStorage,
    *,
    show_diff: int,
) -> dict[str, object]:
    table = TABLE_BY_FREQ[frequence]
    order_book_id = stock_format(code)
    start_s = start.strftime("%Y-%m-%d %H:%M:%S")
    end_s = end.strftime("%Y-%m-%d %H:%M:%S")

    eq_rows = eq.fetch_stock_min_rows(
        code,
        start_s,
        end_s,
        frequence=frequence,
    )
    eq_map = _eq_rows_to_map(eq_rows)
    ch_map = _load_ch_rows(
        clickhouse,
        table,
        [order_book_id],
        start,
        end,
    )

    eq_keys = set(eq_map)
    ch_keys = set(ch_map)
    only_eq = sorted(eq_keys - ch_keys)
    only_ch = sorted(ch_keys - eq_keys)
    common = sorted(eq_keys & ch_keys)

    exact = 0
    field_mismatch: dict[str, int] = {name: 0 for name in FIELDS}
    diffs: list[tuple] = []
    max_abs = {name: 0 for name in FIELDS}

    for key in common:
        eq_vals = eq_map[key]
        ch_vals = ch_map[key]
        if eq_vals == ch_vals:
            exact += 1
            continue
        row_diff = []
        for idx, name in enumerate(FIELDS):
            delta = int(eq_vals[idx]) - int(ch_vals[idx])
            if delta != 0:
                field_mismatch[name] += 1
                max_abs[name] = max(max_abs[name], abs(delta))
                row_diff.append((name, eq_vals[idx], ch_vals[idx], delta))
        if row_diff and len(diffs) < show_diff:
            diffs.append((key[0], key[1], row_diff))

    summary = {
        "code": code,
        "order_book_id": order_book_id,
        "frequence": frequence,
        "table": table,
        "eq_rows": len(eq_map),
        "ch_rows": len(ch_map),
        "common": len(common),
        "exact": exact,
        "value_mismatch": len(common) - exact,
        "only_eq": len(only_eq),
        "only_ch": len(only_ch),
        "field_mismatch": field_mismatch,
        "max_abs": max_abs,
        "diff_examples": diffs,
        "only_eq_examples": only_eq[:show_diff],
        "only_ch_examples": only_ch[:show_diff],
    }
    return summary


def _print_summary(summary: dict[str, object]) -> None:
    freq = summary["frequence"]
    code = summary["code"]
    exact = int(summary["exact"])
    common = int(summary["common"])
    ratio = (exact / common * 100.0) if common else 0.0
    print(
        f"\n=== {code} {freq} vs {summary['table']} ===\n"
        f"EQ={summary['eq_rows']} CH={summary['ch_rows']} "
        f"common={common} exact={exact} ({ratio:.2f}%) "
        f"value_mismatch={summary['value_mismatch']} "
        f"only_EQ={summary['only_eq']} only_CH={summary['only_ch']}"
    )
    field_mismatch = summary["field_mismatch"]
    max_abs = summary["max_abs"]
    print(
        "field mismatches: "
        + ", ".join(
            f"{name}={field_mismatch[name]}(max_abs={max_abs[name]})"
            for name in FIELDS
        )
    )
    for item in summary["diff_examples"]:
        oid, moment, row_diff = item
        parts = ", ".join(
            f"{n}: EQ={ev} CH={cv} d={d}" for n, ev, cv, d in row_diff
        )
        print(f"  DIFF {oid} {moment} | {parts}")
    for key in summary["only_eq_examples"]:
        print(f"  ONLY_EQ {key[0]} {key[1]}")
    for key in summary["only_ch_examples"]:
        print(f"  ONLY_CH {key[0]} {key[1]}")


def main() -> int:
    args = _parse_args()
    codes = _codes(args.codes)
    if not codes:
        print("没有有效代码")
        return 1

    start = pd.Timestamp(args.start)
    end = _parse_end(args.end)
    freqs = ["1min", "5min"] if args.freq == "both" else [args.freq]

    print(
        f"核对区间 {start} .. {end} | codes={codes} | freq={freqs}"
    )
    clickhouse = ClickHouseStorage(None)
    overall_ok = True
    summaries: list[dict[str, object]] = []

    try:
        for frequence in freqs:
            table = TABLE_BY_FREQ[frequence]
            exists = clickhouse._table_exists(table)  # noqa: SLF001
            if not exists:
                print(f"表不存在，跳过 {table}")
                overall_ok = False
                continue
            for code in codes:
                summary = verify_one(
                    code,
                    frequence,
                    start,
                    end,
                    clickhouse,
                    show_diff=args.show_diff,
                )
                summaries.append(summary)
                _print_summary(summary)
                if (
                    summary["value_mismatch"]
                    or summary["only_eq"]
                    or summary["only_ch"]
                    or summary["eq_rows"] == 0
                ):
                    overall_ok = False
    finally:
        eq.close_client()

    print("\n==== 总览 ====")
    for summary in summaries:
        common = int(summary["common"])
        exact = int(summary["exact"])
        ratio = (exact / common * 100.0) if common else 0.0
        status = "OK" if (
            summary["value_mismatch"] == 0
            and summary["only_eq"] == 0
            and summary["only_ch"] == 0
            and summary["eq_rows"] > 0
        ) else "DIFF"
        print(
            f"[{status}] {summary['code']} {summary['frequence']}: "
            f"exact={exact}/{common} ({ratio:.2f}%) "
            f"only_EQ={summary['only_eq']} only_CH={summary['only_ch']}"
        )

    return 0 if overall_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
