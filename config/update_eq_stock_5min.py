#!/usr/bin/env python
# coding: utf-8
"""从 EQ 拉取 A 股 5 分钟线，写入 ClickHouse quantaxis.stock_cn_5min。

设计要点:
- 只写 ClickHouse
- 无库内数据时从 DEFAULT_START；有数据则按 latest 增量
- 按年切片 + 区间一次 getSync（不再按日 RPC）
- 精简行元组直写，跳过 DataFrame / 入库前 SELECT 去重

前置:
- config/eq.ini 已配置
- qaenv / ClickHouse 可用
- 未指定 --codes 时，股票清单来自 quantaxis.stock_cn_codelist

用法:
  .venv/bin/python config/update_eq_stock_5min.py --codes 000001
  .venv/bin/python config/update_eq_stock_5min.py --codes 000001,600000 --start 2024-01-01 --end 2024-01-31
  .venv/bin/python config/update_eq_stock_5min.py --limit 5
"""

from __future__ import annotations

import argparse
import datetime
import sys
import time
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from QUANTAXIS.QAFetch import QAEq as eq
from QUANTAXIS.QASU.save_ths_clickhouse import ClickHouseStorage

CHINA_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_START = "2000-01-01"
DATASET = "stock_5min"
CHUNK_DAYS = 365
REQUEST_PAUSE_SEC = 0.0
INSERT_BATCH = 20000


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="从 EQ 拉取 stock 5min 并写入 ClickHouse",
    )
    parser.add_argument(
        "--codes",
        default="",
        help="逗号分隔六位代码；为空则使用 CH stock_cn_codelist",
    )
    parser.add_argument("--limit", type=int, default=0, help="仅处理前 N 只")
    parser.add_argument("--start", default="", help="强制起始")
    parser.add_argument("--end", default="", help="结束时间，默认当前")
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=CHUNK_DAYS,
        help=f"外层切片跨度天数（默认 {CHUNK_DAYS}）",
    )
    parser.add_argument(
        "--pause",
        type=float,
        default=REQUEST_PAUSE_SEC,
        help="切片间隔秒（默认 0）",
    )
    return parser.parse_args()


def _codes_from_args(codes_arg: str, limit: int) -> list[str]:
    if codes_arg.strip():
        return [
            item.strip().split(".")[0]
            for item in codes_arg.split(",")
            if item.strip()
        ]
    codes = _codes_from_stock_list()
    if limit > 0:
        return codes[:limit]
    return codes


def _codes_from_stock_list() -> list[str]:
    """从 ClickHouse ``quantaxis.stock_cn_codelist`` 读取股票代码。"""

    try:
        client = ClickHouseStorage(None)
        rows = client.client.execute(
            "SELECT DISTINCT order_book_id "
            "FROM quantaxis.stock_cn_codelist "
            "WHERE order_book_id != '' "
            "ORDER BY order_book_id"
        )
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            f"读取 CH stock_cn_codelist 失败: {exc}；请使用 --codes"
        ) from exc
    codes = sorted(
        {
            str(row[0]).split(".")[0]
            for row in rows
            if row and row[0]
        }
    )
    if not codes:
        raise RuntimeError(
            "quantaxis.stock_cn_codelist 为空，请先写入码表或使用 --codes"
        )
    return codes


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


def _resolve_start(
    clickhouse: ClickHouseStorage,
    code: str,
    force_start: str,
) -> pd.Timestamp:
    if force_start:
        return pd.Timestamp(force_start)
    latest = clickhouse.latest(DATASET, code, "datetime")
    if latest is None:
        return pd.Timestamp(DEFAULT_START)
    return pd.Timestamp(latest) + datetime.timedelta(minutes=5)


def _parse_end(force_end: str) -> pd.Timestamp:
    if not force_end:
        return pd.Timestamp(
            datetime.datetime.now(CHINA_TZ).replace(tzinfo=None)
        )
    stamp = pd.Timestamp(force_end)
    if len(force_end.strip()) <= 10:
        stamp = stamp + datetime.timedelta(hours=23, minutes=59, seconds=59)
    return stamp


def save_stock_5min(
    codes: list[str],
    clickhouse: ClickHouseStorage,
    *,
    force_start: str = "",
    force_end: str = "",
    chunk_days: int = CHUNK_DAYS,
    pause: float = REQUEST_PAUSE_SEC,
) -> dict[str, object]:
    end = _parse_end(force_end)
    total = len(codes)
    ok = 0
    skipped = 0
    ch_rows = 0
    failed: list[tuple[str, str]] = []
    columns = eq.STOCK_5MIN_CH_COLUMNS

    for index, code in enumerate(codes, start=1):
        try:
            start = _resolve_start(clickhouse, code, force_start)
            if start > end:
                skipped += 1
                print(f"[{index}/{total}] {code} 已是最新，跳过")
                continue

            code_rows = 0
            chunks = _iter_chunks(start, end, chunk_days)
            for chunk_i, (c_start, c_end) in enumerate(chunks, start=1):
                start_s = c_start.strftime("%Y-%m-%d %H:%M:%S")
                end_s = c_end.strftime("%Y-%m-%d %H:%M:%S")
                try:
                    rows = eq.fetch_stock_min_rows(
                        code,
                        start_s,
                        end_s,
                        frequence="5min",
                    )
                except Exception as exc:  # noqa: BLE001
                    failed.append((code, f"{start_s}..{end_s}: {exc}"))
                    print(
                        f"[{index}/{total}] {code} chunk {chunk_i}/{len(chunks)} "
                        f"失败: {exc}"
                    )
                    if pause > 0:
                        time.sleep(pause)
                    continue

                if not rows:
                    if pause > 0:
                        time.sleep(pause)
                    continue

                written = clickhouse.insert_canonical_rows(
                    DATASET,
                    rows,
                    columns,
                    batch_size=INSERT_BATCH,
                )
                code_rows += written
                ch_rows += written
                print(
                    f"[{index}/{total}] {code} chunk {chunk_i}/{len(chunks)} "
                    f"rows={len(rows)} written={written} "
                    f"range={start_s}..{end_s}"
                )
                if pause > 0:
                    time.sleep(pause)

            if code_rows == 0 and not any(c == code for c, _ in failed):
                skipped += 1
                print(f"[{index}/{total}] {code} 无新数据 ({start}..{end})")
            elif code_rows > 0:
                ok += 1
                print(f"[{index}/{total}] {code} 完成 written={code_rows}")
        except Exception as exc:  # noqa: BLE001
            failed.append((code, str(exc)))
            print(f"[{index}/{total}] {code} 失败: {exc}")

    return {
        "total": total,
        "ok": ok,
        "skipped": skipped,
        "failed": failed,
        "clickhouse_rows": ch_rows,
    }


def main() -> int:
    args = _parse_args()
    codes = _codes_from_args(args.codes, args.limit)
    if not codes:
        print("没有待处理股票代码")
        return 1

    print(
        f"准备处理 {len(codes)} 只股票 5min → ClickHouse: "
        + ", ".join(codes[:10])
        + (" ..." if len(codes) > 10 else "")
    )

    clickhouse = ClickHouseStorage(None)
    clickhouse.ensure_stock_cn_5min()
    print("ensured quantaxis.stock_cn_5min")

    try:
        summary = save_stock_5min(
            codes,
            clickhouse,
            force_start=args.start.strip(),
            force_end=args.end.strip(),
            chunk_days=args.chunk_days,
            pause=args.pause,
        )
    finally:
        eq.close_client()

    print("==== 汇总 ====")
    print(
        f"total={summary['total']} ok={summary['ok']} "
        f"skipped={summary['skipped']} failed={len(summary['failed'])} "
        f"clickhouse_rows={summary['clickhouse_rows']}"
    )
    if summary["failed"]:
        for code, err in summary["failed"][:50]:
            print(f"  FAIL {code}: {err}")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
