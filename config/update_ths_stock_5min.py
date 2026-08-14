#!/usr/bin/env python
# coding: utf-8
"""从 ths_http 拉取 A 股 5 分钟线，写入 ClickHouse quantaxis.stock_cn_5min。

设计要点:
- 只写 ClickHouse（不写 Mongo）
- 起始：库内无数据时从 DEFAULT_START（2000-01-01）；有数据则按 latest 增量
- 按年切片请求，规避 THS high_frequency 约 200 万点上限
- 建表：按月分区、Float64 OHLC

前置:
- config/ths_http.ini 已配置 refresh_token
- qaenv / CLICKHOUSE_* 已指向本机 ClickHouse

用法:
  .venv/bin/python config/update_ths_stock_5min.py --codes 688813
  .venv/bin/python config/update_ths_stock_5min.py --codes 000001,600000
  .venv/bin/python config/update_ths_stock_5min.py --limit 5
  .venv/bin/python config/update_ths_stock_5min.py --start 2024-01-01 --end 2024-06-30
  .venv/bin/python config/update_ths_stock_5min.py
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

from QUANTAXIS.QAFetch import QAThs_http as ths
from QUANTAXIS.QASU.save_ths_clickhouse import ClickHouseStorage
from QUANTAXIS.QAUtil import DATABASE

CHINA_TZ = ZoneInfo("Asia/Shanghai")
# 无库内数据时的回补起点；有 latest 则从其下一根 5min 增量
DEFAULT_START = "2000-01-01"
DATASET = "stock_5min"
UNIQUE_FIELDS = ("code", "datetime", "type")
CHUNK_DAYS = 365
REQUEST_PAUSE_SEC = 0.15
# 同花顺行情周用量超限（如 -4302）时立即停跑，避免空耗请求
QUOTA_EXIT_CODE = 3


class THSQuotaExceededError(RuntimeError):
    """同花顺行情数据用量超限，应终止全市场任务。"""


def _is_quota_exceeded(exc: BaseException) -> bool:
    text = str(exc)
    text_l = text.lower()
    if "-4302" in text_l:
        return True
    if "usage of quote data" in text_l and "exceeded" in text_l:
        return True
    if "quote data" in text_l and "exceeded" in text_l:
        return True
    if "用量" in text and ("超限" in text or "用完" in text or "超过" in text):
        return True
    return False


def _raise_if_quota(exc: BaseException) -> None:
    if _is_quota_exceeded(exc):
        raise THSQuotaExceededError(str(exc)) from exc


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="从 ths_http 拉取 stock 5min 并写入 ClickHouse",
    )
    parser.add_argument(
        "--codes",
        default="",
        help="逗号分隔六位代码；为空则用清单全市场",
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
        help="强制起始（覆盖库内 latest）；空则自动增量/最早",
    )
    parser.add_argument(
        "--end",
        default="",
        help="结束时间，默认当前（上海）",
    )
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=CHUNK_DAYS,
        help=f"单次请求最大跨度天数（默认 {CHUNK_DAYS}）",
    )
    parser.add_argument(
        "--pause",
        type=float,
        default=REQUEST_PAUSE_SEC,
        help="请求间隔秒（限流）",
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
    try:
        cursor = DATABASE.stock_list.find({}, {"code": 1, "_id": 0})
        codes = [
            str(doc["code"]).split(".")[0]
            for doc in cursor
            if doc.get("code")
        ]
        if codes:
            return sorted(set(codes))
    except Exception as exc:  # noqa: BLE001
        print(f"读取 Mongo stock_list 失败，改用 ths_http: {exc}")
    try:
        client = ClickHouseStorage(None)
        rows = client.client.execute(
            "SELECT order_book_id FROM quantaxis.stock_cn_codelist"
        )
        if rows:
            return sorted(
                {
                    str(row[0]).split(".")[0]
                    for row in rows
                    if row and row[0]
                }
            )
    except Exception as exc:  # noqa: BLE001
        print(f"读取 CH stock_cn_codelist 失败，改用 ths_http: {exc}")
    frame = ths.QA_fetch_get_stock_list()
    if "code" not in frame.columns or frame.empty:
        raise RuntimeError("无法获取股票清单")
    return sorted(
        {
            str(code).split(".")[0]
            for code in frame["code"].tolist()
            if str(code).strip()
        }
    )


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
    # 增量：从下一根 5 分钟 bar 开始
    return pd.Timestamp(latest) + datetime.timedelta(minutes=5)


def _parse_end(force_end: str) -> pd.Timestamp:
    if not force_end:
        return pd.Timestamp(datetime.datetime.now(CHINA_TZ).replace(tzinfo=None))
    stamp = pd.Timestamp(force_end)
    # 仅日期时扩到当日 23:59:59，避免漏掉收盘 bar
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
                    frame = ths.QA_fetch_get_stock_min(
                        code,
                        start_s,
                        end_s,
                        frequence="5min",
                    )
                except THSQuotaExceededError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    _raise_if_quota(exc)
                    # 单窗口失败不丢整只股票：记录后继续后续窗口
                    failed.append((code, f"{start_s}..{end_s}: {exc}"))
                    print(
                        f"[{index}/{total}] {code} chunk {chunk_i}/{len(chunks)} "
                        f"失败: {exc}"
                    )
                    if pause > 0:
                        time.sleep(pause)
                    continue

                if frame is None or getattr(frame, "empty", True):
                    if pause > 0:
                        time.sleep(pause)
                    continue

                payload = frame.reset_index(drop=True)
                written = clickhouse.save(DATASET, payload, UNIQUE_FIELDS)
                code_rows += written
                ch_rows += written
                print(
                    f"[{index}/{total}] {code} chunk {chunk_i}/{len(chunks)} "
                    f"rows={len(payload)} written={written} "
                    f"range={start_s}..{end_s}"
                )
                if pause > 0:
                    time.sleep(pause)

            if code_rows == 0 and not any(c == code for c, _ in failed):
                skipped += 1
                print(
                    f"[{index}/{total}] {code} 无新数据 "
                    f"({start}..{end})"
                )
            elif code_rows > 0:
                ok += 1
                print(f"[{index}/{total}] {code} 完成 written={code_rows}")
        except THSQuotaExceededError:
            raise
        except Exception as exc:  # noqa: BLE001
            _raise_if_quota(exc)
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
    except THSQuotaExceededError as exc:
        print("==== 用量超限，任务退出 ====")
        print(f"THS quote usage exceeded: {exc}")
        print("已写入数据保留在 ClickHouse，额度恢复后可直接重跑续传。")
        return QUOTA_EXIT_CODE

    print("==== 汇总 ====")
    print(
        f"total={summary['total']} ok={summary['ok']} "
        f"skipped={summary['skipped']} failed={len(summary['failed'])} "
        f"clickhouse_rows={summary['clickhouse_rows']}"
    )
    if summary["failed"]:
        for code, err in summary["failed"][:50]:
            print(f"  FAIL {code}: {err}")
        if len(summary["failed"]) > 50:
            print(f"  ... 另有 {len(summary['failed']) - 50} 条失败")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
