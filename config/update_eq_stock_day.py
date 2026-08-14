#!/usr/bin/env python
# coding: utf-8
"""从 EQ 增量拉取 A 股日线，写入 ClickHouse quantaxis.stock_cn_day。

前置:
- config/eq.ini 已配置 ip/port/user/passwd
- 已安装 eqapi + zeroc-ice + python-snappy + psutil
- ClickHouse 已有 quantaxis.stock_cn_day / stock_cn_codelist
- 未指定 --codes 时，股票清单来自 quantaxis.stock_cn_codelist
- 短窗增量：同市场多代码批量 getSync；长窗单票区间拉取

用法:
  .venv/bin/python config/update_eq_stock_day.py --codes 000001
  .venv/bin/python config/update_eq_stock_day.py --codes 000001,600000
  .venv/bin/python config/update_eq_stock_day.py --limit 5
"""

from __future__ import annotations

import argparse
import datetime
import sys
from collections import defaultdict
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from QUANTAXIS.QAFetch import QAEq as eq
from QUANTAXIS.QASU.save_ths_clickhouse import ClickHouseStorage

CHINA_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_START = "1990-01-01"
DATASET = "stock_day"
INSERT_BATCH = 5000
# 增量日期跨度不超过该天数时，同市场批量拉取
BATCH_MAX_SPAN_DAYS = 40
BATCH_SIZE = 30


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="从 EQ 拉取 stock_day 并写入 ClickHouse",
    )
    parser.add_argument(
        "--codes",
        default="",
        help="逗号分隔的六位股票代码；为空则使用 CH stock_cn_codelist",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="仅处理清单前 N 只；0 表示不限制",
    )
    parser.add_argument(
        "--start",
        default="",
        help="强制起始（覆盖库内 latest）；空则自动增量",
    )
    parser.add_argument(
        "--end",
        default="",
        help="结束日期，默认今天（上海）",
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


def _start_from_latest(latest: object, force_start: str) -> str:
    if force_start:
        return force_start[:10]
    if latest is None:
        return DEFAULT_START
    stamp = pd.Timestamp(latest) + datetime.timedelta(days=1)
    return stamp.strftime("%Y-%m-%d")


def _write_rows(clickhouse: ClickHouseStorage, rows: list[tuple]) -> int:
    if not rows:
        return 0
    return clickhouse.insert_canonical_rows(
        DATASET,
        rows,
        eq.STOCK_DAY_CH_COLUMNS,
        batch_size=INSERT_BATCH,
    )


def save_stock_day(
    codes: list[str],
    clickhouse: ClickHouseStorage,
    *,
    force_start: str = "",
    force_end: str = "",
) -> dict[str, object]:
    end = force_end[:10] if force_end else datetime.datetime.now(CHINA_TZ).date().isoformat()
    total = len(codes)
    ok = 0
    skipped = 0
    ch_rows = 0
    failed: list[tuple[str, str]] = []

    # (code, start) 待处理；短窗按市场批量，长窗单票
    pending_single: list[tuple[str, str]] = []
    pending_batch: dict[str, list[tuple[str, str]]] = defaultdict(list)

    for code in codes:
        start = _start_from_latest(
            clickhouse.latest(DATASET, code, "date"),
            force_start,
        )
        if start[:10] > end:
            skipped += 1
            continue
        span = (pd.Timestamp(end) - pd.Timestamp(start)).days
        if span <= BATCH_MAX_SPAN_DAYS:
            market = eq.eq_market_for_code(code)
            pending_batch[market].append((code, start))
        else:
            pending_single.append((code, start))
    already_latest = skipped

    # 先处理批量增量
    batch_index = 0
    for market, items in pending_batch.items():
        # 按起始日分组，同 start 的一批一起拉
        by_start: dict[str, list[str]] = defaultdict(list)
        for code, start in items:
            by_start[start].append(code)
        for start, group in by_start.items():
            for offset in range(0, len(group), BATCH_SIZE):
                batch = group[offset : offset + BATCH_SIZE]
                batch_index += 1
                try:
                    rows = eq.fetch_stock_day_rows(batch, start, end)
                    written = _write_rows(clickhouse, rows)
                    ch_rows += written
                    # 按代码统计是否写到数据（粗略：有返回则 ok）
                    codes_in_rows = {
                        str(row[1]).split(".")[0] for row in rows
                    }
                    for code in batch:
                        if code in codes_in_rows:
                            ok += 1
                        else:
                            skipped += 1
                    print(
                        f"[batch {batch_index}] {market} "
                        f"n={len(batch)} rows={len(rows)} written={written} "
                        f"range={start}..{end}"
                    )
                except Exception as exc:  # noqa: BLE001
                    for code in batch:
                        failed.append((code, str(exc)))
                    print(
                        f"[batch {batch_index}] {market} "
                        f"失败 n={len(batch)}: {exc}"
                    )

    # 长历史单票
    for index, (code, start) in enumerate(pending_single, start=1):
        try:
            rows = eq.fetch_stock_day_rows(code, start, end)
            if not rows:
                skipped += 1
                print(
                    f"[single {index}/{len(pending_single)}] {code} "
                    f"无新数据 ({start}..{end})"
                )
                continue
            written = _write_rows(clickhouse, rows)
            ch_rows += written
            ok += 1
            print(
                f"[single {index}/{len(pending_single)}] {code} "
                f"rows={len(rows)} written={written} range={start}..{end}"
            )
        except Exception as exc:  # noqa: BLE001
            failed.append((code, str(exc)))
            print(
                f"[single {index}/{len(pending_single)}] {code} 失败: {exc}"
            )

    if already_latest > 0:
        print(f"已是最新跳过: {already_latest}")

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
        f"准备处理 {len(codes)} 只股票 day → ClickHouse: "
        + ", ".join(codes[:10])
        + (" ..." if len(codes) > 10 else "")
    )

    clickhouse = ClickHouseStorage(None)
    try:
        summary = save_stock_day(
            codes,
            clickhouse,
            force_start=args.start.strip(),
            force_end=args.end.strip(),
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
