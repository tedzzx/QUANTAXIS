#!/usr/bin/env python
# coding: utf-8
"""从 ths_http 增量拉取 A 股日线，写入 MongoDB 与/或 ClickHouse。

设计要点（相对「调用两次 QA_SU_save_stock_day」）:
- 每只股票只请求一次 ths_http，再双写，避免全市场双倍 API 消耗
- 起始日取两库 latest 的更早一侧；缺省从 1990-01-01
- Mongo: upsert（含 vol/volume 双字段，兼容 QA_fetch_stock_day）
- ClickHouse: 写入 quantaxis.stock_cn_day（无效 OHLC 行会在存储层过滤）

前置条件:
- config/ths_http.ini 已配置 refresh_token
- ClickHouse 已有 quantaxis.stock_cn_day
- qaenv/环境变量已对齐本机 Docker（建议 CLICKHOUSE_PORT=9000, USER=default）
- 建议已有股票清单（Mongo stock_list）

用法:
  .venv/bin/python config/update_ths_stock_day.py --codes 000002
  .venv/bin/python config/update_ths_stock_day.py --codes 000001,600000
  .venv/bin/python config/update_ths_stock_day.py --limit 5
  .venv/bin/python config/update_ths_stock_day.py --backends mongodb
  .venv/bin/python config/update_ths_stock_day.py
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

from QUANTAXIS.QAFetch import QAThs_http as ths
from QUANTAXIS.QASU.save_ths_clickhouse import ClickHouseStorage
from QUANTAXIS.QASU.save_ths_mongodb import MongoStorage
from QUANTAXIS.QAUtil import DATABASE

CHINA_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_START = "1990-01-01"
DATASET = "stock_day"
UNIQUE_FIELDS = ("code", "date")
SUPPORTED_BACKENDS = ("mongodb", "clickhouse")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="从 ths_http 拉取 stock_day 并写入 MongoDB / ClickHouse",
    )
    parser.add_argument(
        "--codes",
        default="",
        help="逗号分隔的六位股票代码；为空则使用清单全市场",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="仅处理清单前 N 只（联调冒烟）；0 表示不限制",
    )
    parser.add_argument(
        "--backends",
        default="both",
        choices=("both", "mongodb", "clickhouse"),
        help="写入目标：both / mongodb / clickhouse（默认 both）",
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


def _start_from_latest(latest: object) -> str:
    if latest is None:
        return DEFAULT_START
    stamp = pd.Timestamp(latest) + datetime.timedelta(days=1)
    return stamp.strftime("%Y-%m-%d")


def _enabled_backends(choice: str) -> set[str]:
    if choice == "both":
        return set(SUPPORTED_BACKENDS)
    return {choice}


def save_stock_day_dual(
    codes: list[str],
    mongo: MongoStorage | None,
    clickhouse: ClickHouseStorage | None,
    backends: set[str],
) -> dict[str, object]:
    end = datetime.datetime.now(CHINA_TZ).date().isoformat()
    mongo_rows = 0
    ch_rows = 0
    ok = 0
    skipped = 0
    failed: list[tuple[str, str]] = []

    total = len(codes)
    for index, code in enumerate(codes, start=1):
        try:
            starts: list[str] = []
            if "mongodb" in backends and mongo is not None:
                starts.append(
                    _start_from_latest(mongo.latest(DATASET, code, "date"))
                )
            if "clickhouse" in backends and clickhouse is not None:
                starts.append(
                    _start_from_latest(
                        clickhouse.latest(DATASET, code, "date")
                    )
                )
            start = min(starts) if starts else DEFAULT_START
            if start[:10] > end:
                skipped += 1
                print(f"[{index}/{total}] {code} 已是最新，跳过")
                continue

            frame = ths.QA_fetch_get_stock_day(code, start, end)
            if frame is None or getattr(frame, "empty", True):
                skipped += 1
                print(f"[{index}/{total}] {code} 无新数据 ({start}..{end})")
                continue

            payload = frame.reset_index(drop=True)
            m_count = 0
            c_count = 0
            errors: list[str] = []

            # 两库独立写入：一侧失败不回滚另一侧，便于增量续跑。
            if "mongodb" in backends and mongo is not None:
                try:
                    m_count = mongo.save(DATASET, payload, UNIQUE_FIELDS)
                    mongo_rows += m_count
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"MongoDB: {exc}")
            if "clickhouse" in backends and clickhouse is not None:
                try:
                    c_count = clickhouse.save(DATASET, payload, UNIQUE_FIELDS)
                    ch_rows += c_count
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"ClickHouse: {exc}")

            if errors:
                failed.append((code, " | ".join(errors)))
                print(
                    f"[{index}/{total}] {code} 部分失败 "
                    f"mongo={m_count} clickhouse={c_count} "
                    f"range={start}..{end}; {errors}"
                )
                continue

            ok += 1
            print(
                f"[{index}/{total}] {code} "
                f"rows={len(payload)} mongo={m_count} clickhouse={c_count} "
                f"range={start}..{end}"
            )
        except Exception as exc:  # noqa: BLE001
            failed.append((code, str(exc)))
            print(f"[{index}/{total}] {code} 失败: {exc}")

    return {
        "total": total,
        "ok": ok,
        "skipped": skipped,
        "failed": failed,
        "mongo_rows": mongo_rows,
        "clickhouse_rows": ch_rows,
        "backends": sorted(backends),
    }


def main() -> int:
    args = _parse_args()
    backends = _enabled_backends(args.backends)
    codes = _codes_from_args(args.codes, args.limit)
    if not codes:
        print("没有待处理股票代码")
        return 1

    print(
        f"backends={sorted(backends)} 准备处理 {len(codes)} 只股票: "
        + ", ".join(codes[:10])
        + (" ..." if len(codes) > 10 else "")
    )

    mongo = MongoStorage(DATABASE) if "mongodb" in backends else None
    # None -> 走 QAClickHouseResourceManager / qaenv 环境变量
    clickhouse = (
        ClickHouseStorage(None) if "clickhouse" in backends else None
    )
    summary = save_stock_day_dual(codes, mongo, clickhouse, backends)

    print("==== 汇总 ====")
    print(
        f"backends={summary['backends']} total={summary['total']} "
        f"ok={summary['ok']} skipped={summary['skipped']} "
        f"failed={len(summary['failed'])}"
    )
    print(
        f"mongo_rows={summary['mongo_rows']} "
        f"clickhouse_rows={summary['clickhouse_rows']}"
    )
    if summary["failed"]:
        for code, err in summary["failed"]:
            print(f"  FAIL {code}: {err}")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
