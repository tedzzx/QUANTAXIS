#!/usr/bin/env python
# coding: utf-8
"""RQData 冒烟：默认走 HTTP API（443），可选写入 ClickHouse。

用法:
  .venv/bin/python config/smoke_rqdata.py --codes 000001
  .venv/bin/python config/smoke_rqdata.py --codes 000001,600000 --write
  .venv/bin/python config/smoke_rqdata.py --codes 000001 --start 2024-01-01 --end 2024-01-31
  .venv/bin/python config/smoke_rqdata.py --codes 000001 --transport tcp   # 需 16011
"""

from __future__ import annotations

import argparse
import datetime
import importlib.util
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from QUANTAXIS.QAFetch.QAClickhouse import stock_format

_data_init_path = Path(__file__).resolve().parent / "data_init.py"
_spec = importlib.util.spec_from_file_location("qa_data_init", _data_init_path)
if _spec is None or _spec.loader is None:
    raise RuntimeError(f"无法加载 {_data_init_path}")
_data_init = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_data_init)
_clickhouse_client = _data_init._clickhouse_client
init_rqdatac = _data_init.init_rqdatac


def _parse_args() -> argparse.Namespace:
    today = datetime.date.today().isoformat()
    parser = argparse.ArgumentParser(description="RQData 日线冒烟测试")
    parser.add_argument(
        "--codes",
        required=True,
        help="逗号分隔代码，支持 000001 或 000001.XSHE",
    )
    parser.add_argument("--start", default="2024-01-01", help="开始日期")
    parser.add_argument("--end", default=today, help="结束日期")
    parser.add_argument(
        "--transport",
        choices=("http", "tcp"),
        default="",
        help="http（默认，443）或 tcp（16011）",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="写入 ClickHouse quantaxis.stock_cn_day",
    )
    return parser.parse_args()


def _normalize_codes(codes_arg: str) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in codes_arg.split(","):
        raw = item.strip()
        if not raw:
            continue
        code = stock_format(raw)
        if code not in seen:
            seen.add(code)
            ordered.append(code)
    return ordered


def _write_stock_day(frame: pd.DataFrame) -> int:
    """把 get_price 日线写入 stock_cn_day（按表列对齐）。"""

    client = _clickhouse_client()
    data = frame.reset_index()
    if "date" not in data.columns and "datetime" in data.columns:
        data = data.rename(columns={"datetime": "date"})
    if "date" not in data.columns:
        raise RuntimeError(f"日线缺少 date 列: {list(data.columns)}")

    columns = [
        row[0]
        for row in client.execute("DESCRIBE TABLE quantaxis.stock_cn_day")
    ]
    for column in columns:
        if column not in data.columns:
            data[column] = 0.0 if column != "order_book_id" else ""
    payload = data.loc[:, columns].copy()
    ohlc = [c for c in ("open", "high", "low", "close") if c in payload.columns]
    if ohlc:
        payload = payload.dropna(subset=ohlc, how="all")
    if "date" in payload.columns:
        payload["date"] = pd.to_datetime(payload["date"], errors="coerce").dt.date
    # stock_cn_day：价格 Int32×10000；volume/成交额/笔数 Int64（元或股）
    price_cols = [
        c
        for c in ("open", "high", "low", "close", "limit_up", "limit_down")
        if c in payload.columns
    ]
    amount_cols = [
        c for c in ("volume", "total_turnover", "num_trades") if c in payload.columns
    ]
    for column in price_cols:
        payload[column] = (
            pd.to_numeric(payload[column], errors="coerce")
            .fillna(0.0)
            .mul(10000)
            .round()
            .astype("int64")
        )
    for column in amount_cols:
        payload[column] = (
            pd.to_numeric(payload[column], errors="coerce")
            .fillna(0.0)
            .round()
            .astype("int64")
        )
    # clickhouse-driver 对 Date 列更稳妥地接受行元组
    values = [
        tuple(row[col] for col in columns)
        for row in payload.to_dict("records")
    ]
    if not values:
        return 0
    batch = 500
    sql = f"INSERT INTO quantaxis.stock_cn_day ({', '.join(columns)}) VALUES"
    for offset in range(0, len(values), batch):
        client.execute(sql, values[offset : offset + batch])
    return len(values)


def main() -> int:
    args = _parse_args()
    if args.transport:
        os.environ["RQDATA_TRANSPORT"] = args.transport

    codes = _normalize_codes(args.codes)
    if not codes:
        print("没有有效代码")
        return 1

    print("初始化 RQData ...")
    transport = init_rqdatac()
    print(f"transport={transport}")

    if transport == "http":
        from QUANTAXIS.QAFetch.QARqdata_http import get_price, user_get_quota

        try:
            print(f"quota={user_get_quota()}")
        except Exception as exc:  # noqa: BLE001
            print(f"quota 查询跳过: {exc}")
    else:
        import rqdatac
        from rqdatac import get_price

        print(rqdatac.info())
        try:
            print(f"quota={rqdatac.user.get_quota()}")
        except Exception as exc:  # noqa: BLE001
            print(f"quota 查询跳过: {exc}")

    print(f"拉取日线 codes={codes} range={args.start}..{args.end}")
    frame = get_price(
        codes,
        start_date=args.start,
        end_date=args.end,
        frequency="1d",
        adjust_type="none",
        expect_df=True,
    )
    if frame is None or getattr(frame, "empty", True):
        print("无数据返回")
        return 2

    preview = frame.reset_index()
    if "date" not in preview.columns and "datetime" in preview.columns:
        preview = preview.rename(columns={"datetime": "date"})
    print(f"rows={len(preview)} columns={list(preview.columns)}")
    print(preview.tail(5).to_string(index=False))

    if args.write:
        written = _write_stock_day(frame)
        print(f"已写入 ClickHouse stock_cn_day: {written} 行")
    else:
        print("未写库（如需写入请加 --write）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
