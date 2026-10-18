#!/usr/bin/env python
"""
从 EQ 拉取股票、ETF、指数 K 线，写入 ClickHouse。

指数使用与股票相同的交易所 K 线市场，1 分钟 243 根、5 分钟 51 根：
``.XSHG`` 走 ``shkl``，``.XSHE`` 走 ``szkl``。``.INDX`` 只有 ``cnsikl``，
其 1 分钟线没有 14:58 和 14:59。价格按元 × 10000 的整数入库。

用法:
  .venv/bin/python config/update_eq_bars.py --asset stock --freq day
  .venv/bin/python config/update_eq_bars.py --asset etf --freq 1min --codes 510050
  .venv/bin/python config/update_eq_bars.py --asset index --freq 5min
"""

from __future__ import annotations

import argparse
import datetime
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
CONFIG = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(CONFIG) not in sys.path:
    sys.path.insert(0, str(CONFIG))

import pandas as pd
from data_init import (
    INDEX_FOCUS_CODES,
    codes_from_list,
    ensure_bar_table,
    insert_bar_rows,
    latest_bar,
)

from QUANTAXIS.QAFetch import QAEq as eq

CHINA_TZ = ZoneInfo("Asia/Shanghai")
DAY_DEFAULT_START = "1990-01-01"
MIN_DEFAULT_START = "2000-01-01"
DAY_INSERT_BATCH = 5000
MIN_INSERT_BATCH = 20000
BATCH_MAX_SPAN_DAYS = 40
BATCH_SIZE = 30
CHUNK_DAYS = {"1min": 90, "5min": 365}


class SaveSummary(TypedDict):
    """一次下载的计数。"""

    total: int
    ok: int
    skipped: int
    failed: list[tuple[str, str]]
    clickhouse_rows: int


@dataclass(frozen=True)
class BarAsset:
    """一种证券的码表、目标表和默认代码。"""

    name: str
    list_table: str
    tables: dict[str, str]
    fixed_codes: tuple[str, ...]


BAR_ASSETS: dict[str, BarAsset] = {
    "stock": BarAsset(
        "stock",
        "stock_cn_codelist",
        {
            "day": "stock_cn_day",
            "1min": "stock_cn_1min",
            "5min": "stock_cn_5min",
        },
        (),
    ),
    "etf": BarAsset(
        "etf",
        "etf_cn_codelist",
        {
            "day": "etf_cn_day",
            "1min": "etf_cn_1min",
            "5min": "etf_cn_5min",
        },
        (),
    ),
    "index": BarAsset(
        "index",
        "",
        {
            "day": "index_cn_day",
            "1min": "index_cn_1min",
            "5min": "index_cn_5min",
        },
        tuple(INDEX_FOCUS_CODES),
    ),
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="从 EQ 拉取 K 线并写入 ClickHouse")
    parser.add_argument("--asset", required=True, choices=sorted(BAR_ASSETS))
    parser.add_argument("--freq", required=True, choices=["day", "1min", "5min"])
    parser.add_argument("--codes", default="", help="逗号分隔代码；指数需带后缀")
    parser.add_argument("--limit", type=int, default=0, help="仅处理前 N 只")
    parser.add_argument("--start", default="", help="强制起始，覆盖库内最新时间")
    parser.add_argument("--end", default="", help="结束时间，默认当前")
    parser.add_argument("--chunk-days", type=int, default=0, help="分钟线切片天数")
    parser.add_argument("--pause", type=float, default=0.0, help="切片间隔秒")
    return parser.parse_args()


def _codes_from_args(asset: BarAsset, codes_arg: str, limit: int) -> list[str]:
    if codes_arg.strip():
        items = [item.strip() for item in codes_arg.split(",") if item.strip()]
        if asset.name == "index":
            codes = [item.upper() for item in items]
        else:
            codes = [item.split(".")[0] for item in items]
    elif asset.fixed_codes:
        codes = list(asset.fixed_codes)
    else:
        codes = codes_from_list(asset.list_table)
    if limit > 0:
        return codes[:limit]
    return codes


def _market_and_book(asset_name: str, code: str) -> tuple[str, str]:
    """指数返回 EQ 市场和完整合约代码；股票与 ETF 留空，沿用原推断。"""

    if asset_name != "index":
        return "", ""
    book = code.strip().upper()
    if book.endswith(".XSHG"):
        return "shkl", book
    if book.endswith(".XSHE"):
        return "szkl", book
    if book.endswith(".INDX"):
        return "cnsikl", book
    raise ValueError(f"指数代码需要带 .XSHG/.XSHE/.INDX 后缀: {code}")


def _plain_code(code: str) -> str:
    return code.strip().split(".")[0]


def _day_start(table: str, code: str, force_start: str) -> str:
    if force_start:
        return force_start[:10]
    latest = latest_bar(table, code, "date")
    if latest is None:
        return DAY_DEFAULT_START
    stamp = pd.Timestamp(latest) + datetime.timedelta(days=1)
    return stamp.strftime("%Y-%m-%d")


def _parse_end(force_end: str, *, date_only: bool) -> pd.Timestamp:
    if not force_end:
        now = datetime.datetime.now(CHINA_TZ).replace(tzinfo=None)
        if date_only:
            return pd.Timestamp(now.date())
        return pd.Timestamp(now)
    stamp = pd.Timestamp(force_end)
    if date_only:
        return pd.Timestamp(stamp.date())
    if len(force_end.strip()) <= 10:
        stamp = stamp + datetime.timedelta(hours=23, minutes=59, seconds=59)
    return stamp


def _min_start(table: str, code: str, force_start: str, step_minutes: int) -> pd.Timestamp:
    if force_start:
        return pd.Timestamp(force_start)
    latest = latest_bar(table, code, "datetime")
    if latest is None:
        return pd.Timestamp(MIN_DEFAULT_START)
    return pd.Timestamp(latest) + datetime.timedelta(minutes=step_minutes)


def _iter_chunks(
    start: pd.Timestamp,
    end: pd.Timestamp,
    chunk_days: int,
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    chunks: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    cursor = start
    step = datetime.timedelta(days=max(1, chunk_days))
    while cursor <= end:
        chunk_end = min(cursor + step - datetime.timedelta(seconds=1), end)
        chunks.append((cursor, chunk_end))
        cursor = cursor + step
    return chunks


def _write_rows(table: str, rows: list[tuple], columns: tuple[str, ...], batch_size: int) -> int:
    if not rows:
        return 0
    return insert_bar_rows(table, rows, columns, batch_size=batch_size)


def save_day(
    asset: BarAsset,
    codes: list[str],
    *,
    force_start: str,
    force_end: str,
) -> SaveSummary:
    """按日线增量写入。短窗口的股票和 ETF 按市场批量拉取。"""

    table = asset.tables["day"]
    end = _parse_end(force_end, date_only=True).strftime("%Y-%m-%d")
    ok = 0
    skipped = 0
    written_rows = 0
    failed: list[tuple[str, str]] = []
    pending_single: list[tuple[str, str, str, str]] = []
    pending_batch: dict[str, list[tuple[str, str]]] = defaultdict(list)

    for code in codes:
        start = _day_start(table, code, force_start)
        if start > end:
            skipped += 1
            continue
        market, book = _market_and_book(asset.name, code)
        span = (pd.Timestamp(end) - pd.Timestamp(start)).days
        if asset.name == "index" or span > BATCH_MAX_SPAN_DAYS:
            pending_single.append((code, start, market, book))
        else:
            pending_batch[eq.eq_market_for_code(code)].append((code, start))

    batch_index = 0
    for market, items in pending_batch.items():
        by_start: dict[str, list[str]] = defaultdict(list)
        for code, start in items:
            by_start[start].append(code)
        for start, group in by_start.items():
            for offset in range(0, len(group), BATCH_SIZE):
                batch = group[offset : offset + BATCH_SIZE]
                batch_index += 1
                try:
                    rows = eq.fetch_stock_day_rows(batch, start, end)
                    written = _write_rows(
                        table, rows, eq.STOCK_DAY_CH_COLUMNS, DAY_INSERT_BATCH
                    )
                    written_rows += written
                    got = {str(row[1]).split(".")[0] for row in rows}
                    for code in batch:
                        if code in got:
                            ok += 1
                        else:
                            skipped += 1
                    print(
                        f"[batch {batch_index}] {market} n={len(batch)} "
                        f"rows={len(rows)} written={written} range={start}..{end}"
                    )
                except Exception as exc:  # noqa: BLE001
                    for code in batch:
                        failed.append((code, str(exc)))
                    print(f"[batch {batch_index}] {market} 失败 n={len(batch)}: {exc}")

    for index, (code, start, market, book) in enumerate(pending_single, start=1):
        try:
            rows = eq.fetch_stock_day_rows(
                _plain_code(code),
                start,
                end,
                market=market,
                order_book_id=book,
            )
            if not rows:
                skipped += 1
                print(f"[single {index}/{len(pending_single)}] {code} 无新数据")
                continue
            written = _write_rows(table, rows, eq.STOCK_DAY_CH_COLUMNS, DAY_INSERT_BATCH)
            written_rows += written
            ok += 1
            print(
                f"[single {index}/{len(pending_single)}] {code} "
                f"rows={len(rows)} written={written} range={start}..{end}"
            )
        except Exception as exc:  # noqa: BLE001
            failed.append((code, str(exc)))
            print(f"[single {index}/{len(pending_single)}] {code} 失败: {exc}")

    return {
        "total": len(codes),
        "ok": ok,
        "skipped": skipped,
        "failed": failed,
        "clickhouse_rows": written_rows,
    }


def save_min(
    asset: BarAsset,
    codes: list[str],
    frequence: str,
    *,
    force_start: str,
    force_end: str,
    chunk_days: int,
    pause: float,
) -> SaveSummary:
    """按分钟线切片增量写入。"""

    table = asset.tables[frequence]
    step_minutes = 1 if frequence == "1min" else 5
    end = _parse_end(force_end, date_only=False)
    ok = 0
    skipped = 0
    written_rows = 0
    failed: list[tuple[str, str]] = []

    for index, code in enumerate(codes, start=1):
        market, book = _market_and_book(asset.name, code)
        try:
            start = _min_start(table, code, force_start, step_minutes)
        except Exception as exc:  # noqa: BLE001
            failed.append((code, str(exc)))
            print(f"[{index}/{len(codes)}] {code} 失败: {exc}")
            continue
        if start > end:
            skipped += 1
            print(f"[{index}/{len(codes)}] {code} 已是最新，跳过")
            continue
        code_rows = 0
        code_failed = False
        chunks = _iter_chunks(start, end, chunk_days)
        for chunk_i, (chunk_start, chunk_end) in enumerate(chunks, start=1):
            start_text = chunk_start.strftime("%Y-%m-%d %H:%M:%S")
            end_text = chunk_end.strftime("%Y-%m-%d %H:%M:%S")
            try:
                rows = eq.fetch_stock_min_rows(
                    _plain_code(code),
                    start_text,
                    end_text,
                    frequence,
                    market=market,
                    order_book_id=book,
                )
            except Exception as exc:  # noqa: BLE001
                code_failed = True
                failed.append((code, f"{start_text}..{end_text}: {exc}"))
                print(
                    f"[{index}/{len(codes)}] {code} "
                    f"chunk {chunk_i}/{len(chunks)} 失败: {exc}"
                )
                if pause > 0:
                    time.sleep(pause)
                continue
            if rows:
                written = _write_rows(
                    table, rows, eq.STOCK_5MIN_CH_COLUMNS, MIN_INSERT_BATCH
                )
                code_rows += written
                written_rows += written
                print(
                    f"[{index}/{len(codes)}] {code} chunk {chunk_i}/{len(chunks)} "
                    f"rows={len(rows)} written={written} range={start_text}..{end_text}"
                )
            if pause > 0:
                time.sleep(pause)
        if code_rows == 0 and not code_failed:
            skipped += 1
            print(f"[{index}/{len(codes)}] {code} 无新数据")
        elif code_rows > 0:
            ok += 1
            print(f"[{index}/{len(codes)}] {code} 完成 written={code_rows}")

    return {
        "total": len(codes),
        "ok": ok,
        "skipped": skipped,
        "failed": failed,
        "clickhouse_rows": written_rows,
    }


def _print_summary(summary: SaveSummary) -> int:
    print("==== 汇总 ====")
    print(
        f"total={summary['total']} ok={summary['ok']} "
        f"skipped={summary['skipped']} failed={len(summary['failed'])} "
        f"clickhouse_rows={summary['clickhouse_rows']}"
    )
    for code, err in summary["failed"][:50]:
        print(f"  FAIL {code}: {err}")
    return 2 if summary["failed"] else 0


def main() -> int:
    """解析参数并下载一种证券的一个频率。"""

    args = _parse_args()
    asset = BAR_ASSETS[args.asset]
    try:
        codes = _codes_from_args(asset, args.codes, args.limit)
    except Exception as exc:  # noqa: BLE001
        print(f"读取代码失败: {exc}")
        return 1
    if not codes:
        print("没有待处理代码")
        return 1
    table = asset.tables[args.freq]
    preview = ", ".join(codes[:10]) + (" ..." if len(codes) > 10 else "")
    print(f"准备处理 {len(codes)} 只 {asset.name} {args.freq} → {table}: {preview}")
    ensure_bar_table(table)
    print(f"ensured quantaxis.{table}")
    try:
        if args.freq == "day":
            summary = save_day(
                asset,
                codes,
                force_start=args.start.strip(),
                force_end=args.end.strip(),
            )
        else:
            chunk_days = args.chunk_days if args.chunk_days > 0 else CHUNK_DAYS[args.freq]
            summary = save_min(
                asset,
                codes,
                args.freq,
                force_start=args.start.strip(),
                force_end=args.end.strip(),
                chunk_days=chunk_days,
                pause=args.pause,
            )
    finally:
        eq.close_client()
    return _print_summary(summary)


if __name__ == "__main__":
    raise SystemExit(main())
