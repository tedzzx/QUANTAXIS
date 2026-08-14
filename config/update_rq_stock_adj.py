#!/usr/bin/env python
# coding: utf-8
"""从 RQData HTTP 拉取复权因子，展开为日频 adj，写入 ClickHouse stock_adj。

目标表结构与 QAClickhouse.get_stock_adj / get_stock_day_qfq 一致:
  quantaxis.stock_adj(date, adj, order_book_id)

用法:
  .venv/bin/python config/update_rq_stock_adj.py --codes 000001,600000
  .venv/bin/python config/update_rq_stock_adj.py --limit 20
  .venv/bin/python config/update_rq_stock_adj.py   # 全市场（stock_cn_codelist / all_instruments）
"""

from __future__ import annotations

import argparse
import datetime
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from QUANTAXIS.QAFetch.QAClickhouse import stock_format
from QUANTAXIS.QAFetch.QARqdata_http import (
    all_instruments,
    get_client,
    get_ex_factor,
    init_rqdatac_http,
)

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS quantaxis.stock_adj (
    date Date DEFAULT '1970-01-01',
    adj Float32,
    order_book_id String
)
ENGINE = ReplacingMergeTree()
ORDER BY (date, order_book_id)
SETTINGS index_granularity=8192
"""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RQData → ClickHouse stock_adj")
    parser.add_argument("--codes", default="", help="逗号分隔代码；空则全市场")
    parser.add_argument("--limit", type=int, default=0, help="仅处理前 N 只")
    parser.add_argument("--batch-size", type=int, default=100, help="每次请求代码数")
    parser.add_argument(
        "--start",
        default="1990-01-01",
        help="get_ex_factor 起始日",
    )
    parser.add_argument(
        "--end",
        default="",
        help="结束日，默认今天",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="写入前清空 stock_adj（全量重建建议开启）",
    )
    return parser.parse_args()


def _clickhouse_client():
    import importlib.util

    path = Path(__file__).resolve().parent / "data_init.py"
    spec = importlib.util.spec_from_file_location("qa_data_init", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("无法加载 data_init.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod._clickhouse_client()


def _normalize_codes(codes_arg: str) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for item in codes_arg.split(","):
        raw = item.strip()
        if not raw:
            continue
        code = stock_format(raw)
        if code not in seen:
            seen.add(code)
            ordered.append(code)
    return ordered


def _codes_from_clickhouse(client) -> list[str]:
    if not client.execute("EXISTS TABLE quantaxis.stock_cn_codelist")[0][0]:
        return []
    rows = client.execute(
        "SELECT order_book_id FROM quantaxis.stock_cn_codelist "
        "WHERE order_book_id != '' ORDER BY order_book_id"
    )
    return [str(row[0]) for row in rows]


def _codes_from_rqdata() -> list[str]:
    frame = all_instruments(type="CS", market="cn")
    if frame is None or frame.empty:
        return []
    return sorted(frame["order_book_id"].astype(str).unique().tolist())


def _trading_calendar(end: str) -> pd.DatetimeIndex:
    client = get_client()
    frame = client.call(
        "get_trading_dates",
        start_date="1990-01-01",
        end_date=end,
        market="cn",
    )
    dates = pd.to_datetime(frame["trading_date"], errors="coerce").dropna()
    return pd.DatetimeIndex(sorted(dates.unique()))


def _expand_adj_daily(
    fac: pd.DataFrame,
    calendar: pd.DatetimeIndex,
    end: pd.Timestamp,
) -> pd.DataFrame:
    """把除权事件展开为交易日 adj 序列（对齐 data_init.save_adj_factor 语义）。"""

    if fac is None or fac.empty:
        return pd.DataFrame(columns=["order_book_id", "date", "adj"])

    work = fac.copy()
    work["ex_date"] = pd.to_datetime(work["ex_date"], errors="coerce")
    work["ex_factor"] = pd.to_numeric(work["ex_factor"], errors="coerce")
    work = work.dropna(subset=["ex_date", "ex_factor", "order_book_id"])
    if work.empty:
        return pd.DataFrame(columns=["order_book_id", "date", "adj"])

    cal = calendar[calendar <= end]
    cal_df = pd.DataFrame({"ex_date": cal})
    pieces: list[pd.DataFrame] = []

    for code, group in work.groupby("order_book_id", sort=False):
        u1 = group.sort_values("ex_date").reset_index(drop=True)
        u1 = u1.assign(adj=(1.0 / u1.ex_factor.values[::-1].cumprod())[::-1])
        eu = (
            pd.concat([u1.loc[:, ["ex_date", "adj"]], cal_df], ignore_index=True)
            .sort_values("ex_date")
            .bfill()
            .set_index("ex_date")
            .loc[:end]
            .fillna(1.0)
        )
        series = eu["adj"].shift(-1).fillna(1.0)
        piece = series.rename("adj").rename_axis("date").reset_index()
        piece.insert(0, "order_book_id", str(code))
        pieces.append(piece)

    if not pieces:
        return pd.DataFrame(columns=["order_book_id", "date", "adj"])
    return pd.concat(pieces, ignore_index=True)


def _ensure_table(client, truncate: bool) -> None:
    client.execute(CREATE_SQL)
    if truncate:
        client.execute("TRUNCATE TABLE IF EXISTS quantaxis.stock_adj")
        print("已清空 quantaxis.stock_adj")


def _insert_adj(client, frame: pd.DataFrame) -> int:
    if frame is None or frame.empty:
        return 0
    payload = frame.loc[:, ["date", "adj", "order_book_id"]].copy()
    payload["date"] = pd.to_datetime(payload["date"], errors="coerce").dt.date
    payload["adj"] = pd.to_numeric(payload["adj"], errors="coerce").fillna(1.0)
    payload["order_book_id"] = payload["order_book_id"].astype(str)
    payload = payload.dropna(subset=["date", "order_book_id"])
    values = [
        (row.date, float(row.adj), row.order_book_id)
        for row in payload.itertuples(index=False)
    ]
    if not values:
        return 0
    batch = 20000
    sql = "INSERT INTO quantaxis.stock_adj (date, adj, order_book_id) VALUES"
    for offset in range(0, len(values), batch):
        client.execute(sql, values[offset : offset + batch])
    return len(values)


def main() -> int:
    args = _parse_args()
    end = args.end or datetime.date.today().isoformat()

    print("初始化 RQData HTTP ...")
    init_rqdatac_http()
    ch = _clickhouse_client()
    _ensure_table(ch, truncate=args.truncate)

    if args.codes.strip():
        codes = _normalize_codes(args.codes)
    else:
        codes = _codes_from_clickhouse(ch)
        if not codes:
            print("ClickHouse 无 stock_cn_codelist，改用 RQData all_instruments(CS)")
            codes = _codes_from_rqdata()
    if args.limit > 0:
        codes = codes[: args.limit]
    if not codes:
        print("没有待处理代码")
        return 1

    print(f"codes={len(codes)} range={args.start}..{end} batch={args.batch_size}")
    calendar = _trading_calendar(end)
    print(f"trading_days={len(calendar)}")
    end_ts = pd.Timestamp(end)

    total_event_rows = 0
    total_adj_rows = 0
    failed: list[tuple[str, str]] = []
    batches = [
        codes[i : i + args.batch_size]
        for i in range(0, len(codes), args.batch_size)
    ]

    no_event_codes: list[str] = []
    for index, batch_codes in enumerate(batches, start=1):
        try:
            # 北交所等部分代码 RQData get_ex_factor 不认，先过滤明显无效后缀重试
            fac = get_ex_factor(
                batch_codes,
                start_date=args.start,
                end_date=end,
                market="cn",
            )
            event_n = 0 if fac is None or fac.empty else len(fac)
            total_event_rows += event_n
            if event_n == 0:
                no_event_codes.extend(batch_codes)
                print(
                    f"[{index}/{len(batches)}] codes={len(batch_codes)} "
                    f"events=0 (稍后补 adj=1)"
                )
                continue
            daily = _expand_adj_daily(fac, calendar, end_ts)
            written = _insert_adj(ch, daily)
            total_adj_rows += written
            have_event = set(fac["order_book_id"].astype(str))
            no_event_codes.extend(
                [code for code in batch_codes if code not in have_event]
            )
            print(
                f"[{index}/{len(batches)}] codes={len(batch_codes)} "
                f"events={event_n} adj_rows={written}"
            )
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            # 整批无效时降级为逐个请求，避免北交所拖垮整批
            if "valid instrument" in msg.lower() or "400" in msg:
                recovered = 0
                for code in batch_codes:
                    try:
                        fac_one = get_ex_factor(
                            [code],
                            start_date=args.start,
                            end_date=end,
                            market="cn",
                        )
                        if fac_one is None or fac_one.empty:
                            no_event_codes.append(code)
                            continue
                        daily = _expand_adj_daily(fac_one, calendar, end_ts)
                        recovered += _insert_adj(ch, daily)
                        total_event_rows += len(fac_one)
                    except Exception:  # noqa: BLE001
                        no_event_codes.append(code)
                total_adj_rows += recovered
                print(
                    f"[{index}/{len(batches)}] 批次降级逐个处理 "
                    f"recovered_rows={recovered} fallback_ones="
                    f"{len(batch_codes)}"
                )
                continue
            failed.append((batch_codes[0], msg))
            print(f"[{index}/{len(batches)}] 失败: {msg[:300]}")

    if no_event_codes:
        uniq = sorted(set(no_event_codes))
        print(f"为无除权事件/无效代码补 adj=1: {len(uniq)} 只")
        dates = [ts.date() for ts in calendar if ts <= end_ts]
        sql = "INSERT INTO quantaxis.stock_adj (date, adj, order_book_id) VALUES"
        ones_rows = 0
        for offset in range(0, len(uniq), 20):
            part = uniq[offset : offset + 20]
            values = [(d, 1.0, code) for code in part for d in dates]
            for pos in range(0, len(values), 20000):
                ch.execute(sql, values[pos : pos + 20000])
            ones_rows += len(values)
        total_adj_rows += ones_rows
        print(f"adj=1 rows={ones_rows}")

    try:
        ch.execute("OPTIMIZE TABLE quantaxis.stock_adj FINAL")
    except Exception as exc:  # noqa: BLE001
        print(f"OPTIMIZE 跳过: {exc}")

    stats = ch.execute(
        "SELECT count(), uniqExact(order_book_id), min(date), max(date) "
        "FROM quantaxis.stock_adj"
    )[0]
    print("==== 汇总 ====")
    print(
        f"batches_ok={len(batches) - len(failed)}/{len(batches)} "
        f"event_rows={total_event_rows} inserted_adj_rows={total_adj_rows}"
    )
    print(
        f"stock_adj: rows={stats[0]} codes={stats[1]} "
        f"date=[{stats[2]} .. {stats[3]}]"
    )
    if failed:
        for code, err in failed[:10]:
            print(f"  FAIL near {code}: {err}")
        return 2

    # 用 QAClickhouse 读回抽样验证
    from QUANTAXIS.QAFetch.QAClickhouse import QACKClient

    sample = codes[0]
    ck = QACKClient()
    adj = ck.get_stock_adj(sample, "2024-01-01", "2024-01-31")
    print(f"QAClickhouse.get_stock_adj({sample}) sample rows={len(adj)}")
    if len(adj):
        print(adj.tail(3))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
