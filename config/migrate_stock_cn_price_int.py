#!/usr/bin/env python3
"""将 stock_cn_day / stock_cn_5min / stock_cn_1min 从 Float64 迁移为 Int 定点。

价格（OHLC / limit_*）→ Int32 × 10000
volume / num_trades / total_turnover → Int64（成交额为元整数，不 ×10000）

用法:
  .venv/bin/python config/migrate_stock_cn_price_int.py --table day --dry-run
  .venv/bin/python config/migrate_stock_cn_price_int.py --table all
  .venv/bin/python config/migrate_stock_cn_price_int.py --table 5min --swap
"""

from __future__ import annotations

import argparse
import time

from clickhouse_driver import Client
from qaenv import (
    clickhouse_ip,
    clickhouse_password,
    clickhouse_port,
    clickhouse_user,
)

PRICE_SCALE = 10000

# 与 QUANTAXIS.QASU.save_ths_clickhouse 中 DDL 保持一致（避免 import QUANTAXIS 写日志）
STOCK_CN_DAY_DDL = """
CREATE TABLE IF NOT EXISTS quantaxis.stock_cn_day (
    date Date,
    order_book_id LowCardinality(String),
    num_trades Int64 CODEC(ZSTD(3)),
    limit_up Int32 CODEC(ZSTD(3)),
    limit_down Int32 CODEC(ZSTD(3)),
    open Int32 CODEC(ZSTD(3)),
    high Int32 CODEC(ZSTD(3)),
    low Int32 CODEC(ZSTD(3)),
    close Int32 CODEC(ZSTD(3)),
    volume Int64 CODEC(ZSTD(3)),
    total_turnover Int64 CODEC(ZSTD(3))
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, order_book_id)
SETTINGS index_granularity = 8192
"""

STOCK_CN_5MIN_DDL = """
CREATE TABLE IF NOT EXISTS quantaxis.stock_cn_5min (
    datetime DateTime CODEC(DoubleDelta, ZSTD(3)),
    order_book_id LowCardinality(String),
    open Int32 CODEC(ZSTD(3)),
    high Int32 CODEC(ZSTD(3)),
    low Int32 CODEC(ZSTD(3)),
    close Int32 CODEC(ZSTD(3)),
    volume Int64 CODEC(ZSTD(3)),
    total_turnover Int64 CODEC(ZSTD(3))
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(datetime)
ORDER BY (order_book_id, datetime)
SETTINGS index_granularity = 8192
"""

STOCK_CN_1MIN_DDL = """
CREATE TABLE IF NOT EXISTS quantaxis.stock_cn_1min (
    datetime DateTime CODEC(DoubleDelta, ZSTD(3)),
    order_book_id LowCardinality(String),
    open Int32 CODEC(ZSTD(3)),
    high Int32 CODEC(ZSTD(3)),
    low Int32 CODEC(ZSTD(3)),
    close Int32 CODEC(ZSTD(3)),
    volume Int64 CODEC(ZSTD(3)),
    total_turnover Int64 CODEC(ZSTD(3))
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(datetime)
ORDER BY (order_book_id, datetime)
SETTINGS index_granularity = 8192
"""

TABLES = {
    "day": {
        "src": "quantaxis.stock_cn_day",
        "new": "quantaxis.stock_cn_day_new",
        "bak": "quantaxis.stock_cn_day_f64_bak",
        "ddl": STOCK_CN_DAY_DDL.replace(
            "quantaxis.stock_cn_day",
            "quantaxis.stock_cn_day_new",
            1,
        ),
        "time_col": "date",
        "partition_expr": "toYYYYMM(date)",
        "select": f"""
            date,
            order_book_id,
            toInt64(round(num_trades)),
            toInt32(round(limit_up * {PRICE_SCALE})),
            toInt32(round(limit_down * {PRICE_SCALE})),
            toInt32(round(open * {PRICE_SCALE})),
            toInt32(round(high * {PRICE_SCALE})),
            toInt32(round(low * {PRICE_SCALE})),
            toInt32(round(close * {PRICE_SCALE})),
            toInt64(round(volume)),
            toInt64(round(total_turnover))
        """,
        "sample_check": f"""
            SELECT
                count() AS cnt,
                max(abs(o.open - n.open / {PRICE_SCALE})) AS max_open_diff,
                max(abs(o.total_turnover - n.total_turnover)) AS max_amt_diff
            FROM {{src}} AS o
            INNER JOIN {{new}} AS n
                USING (date, order_book_id)
            WHERE o.order_book_id = '000001.XSHE'
              AND o.date >= today() - 30
        """,
    },
    "5min": {
        "src": "quantaxis.stock_cn_5min",
        "new": "quantaxis.stock_cn_5min_new",
        "bak": "quantaxis.stock_cn_5min_f64_bak",
        "ddl": STOCK_CN_5MIN_DDL.replace(
            "quantaxis.stock_cn_5min",
            "quantaxis.stock_cn_5min_new",
            1,
        ),
        "time_col": "datetime",
        "partition_expr": "toYYYYMM(datetime)",
        "select": f"""
            datetime,
            order_book_id,
            toInt32(round(open * {PRICE_SCALE})),
            toInt32(round(high * {PRICE_SCALE})),
            toInt32(round(low * {PRICE_SCALE})),
            toInt32(round(close * {PRICE_SCALE})),
            toInt64(round(volume)),
            toInt64(round(total_turnover))
        """,
        "sample_check": f"""
            SELECT
                count() AS cnt,
                max(abs(o.open - n.open / {PRICE_SCALE})) AS max_open_diff,
                max(abs(o.total_turnover - n.total_turnover)) AS max_amt_diff
            FROM {{src}} AS o
            INNER JOIN {{new}} AS n
                USING (datetime, order_book_id)
            WHERE o.order_book_id = '000001.XSHE'
              AND o.datetime >= now() - INTERVAL 7 DAY
        """,
    },
    "1min": {
        "src": "quantaxis.stock_cn_1min",
        "new": "quantaxis.stock_cn_1min_new",
        "bak": "quantaxis.stock_cn_1min_f64_bak",
        "ddl": STOCK_CN_1MIN_DDL.replace(
            "quantaxis.stock_cn_1min",
            "quantaxis.stock_cn_1min_new",
            1,
        ),
        "time_col": "datetime",
        # 旧表按日分区；拷贝时按源分区 ID 过滤，新表按月分区
        "partition_expr": "toYYYYMMDD(datetime)",
        "select": f"""
            datetime,
            order_book_id,
            toInt32(round(open * {PRICE_SCALE})),
            toInt32(round(high * {PRICE_SCALE})),
            toInt32(round(low * {PRICE_SCALE})),
            toInt32(round(close * {PRICE_SCALE})),
            toInt64(round(volume)),
            toInt64(round(total_turnover))
        """,
        "sample_check": f"""
            SELECT
                count() AS cnt,
                max(abs(o.open - n.open / {PRICE_SCALE})) AS max_open_diff,
                max(abs(o.total_turnover - n.total_turnover)) AS max_amt_diff
            FROM {{src}} AS o
            INNER JOIN {{new}} AS n
                USING (datetime, order_book_id)
        """,
    },
}


def _client() -> Client:
    return Client(
        host=clickhouse_ip,
        port=int(clickhouse_port),
        user=clickhouse_user,
        password=clickhouse_password or "",
        database="quantaxis",
        connect_timeout=60,
        send_receive_timeout=6 * 3600,
        settings={
            "max_execution_time": 0,
            "receive_timeout": 6 * 3600,
            "send_timeout": 6 * 3600,
        },
    )


def _execute_with_retry(
    client: Client,
    sql: str,
    params: dict,
    *,
    retries: int = 5,
    label: str = "",
) -> Client:
    """执行 SQL；遇连接断开则重建 client 并重试。"""

    delay = 2.0
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            client.execute(sql, params)
            return client
        except (EOFError, OSError, Exception) as exc:  # noqa: BLE001
            last_exc = exc
            print(
                f"  retry {attempt}/{retries} {label}: {type(exc).__name__}: {exc}"
            )
            time.sleep(delay)
            delay = min(delay * 2, 60)
            try:
                client.disconnect()
            except Exception:  # noqa: BLE001
                pass
            client = _client()
    raise RuntimeError(f"执行失败 {label}: {last_exc}")


def _table_exists(client: Client, full_name: str) -> bool:
    database, table = full_name.split(".", 1)
    rows = client.execute(
        "SELECT count() FROM system.tables "
        "WHERE database = %(d)s AND name = %(t)s",
        {"d": database, "t": table},
    )
    return bool(rows and rows[0][0])


def _partitions(client: Client, full_name: str) -> list[str]:
    database, table = full_name.split(".", 1)
    rows = client.execute(
        "SELECT DISTINCT partition FROM system.parts "
        "WHERE database = %(d)s AND table = %(t)s AND active "
        "ORDER BY partition",
        {"d": database, "t": table},
    )
    return [str(r[0]) for r in rows]


def _count(client: Client, full_name: str, *, final: bool = False) -> int:
    suffix = " FINAL" if final else ""
    return int(client.execute(f"SELECT count() FROM {full_name}{suffix}")[0][0])


def migrate_one(
    client: Client,
    key: str,
    *,
    dry_run: bool,
    swap: bool,
    recreate: bool,
) -> None:
    cfg = TABLES[key]
    src, new, bak = cfg["src"], cfg["new"], cfg["bak"]
    print(f"\n=== migrate {key}: {src} -> {new} ===")
    if not _table_exists(client, src):
        print(f"跳过：源表不存在 {src}")
        return

    src_count = _count(client, src)
    print(f"源表行数: {src_count}")
    parts = _partitions(client, src)
    print(f"源分区数: {len(parts)}")

    if dry_run:
        print("dry-run：仅打印，不建表/拷贝/切换")
        print("DDL:\n", cfg["ddl"])
        return

    if recreate and _table_exists(client, new):
        print(f"DROP {new}")
        client.execute(f"DROP TABLE IF EXISTS {new}")

    if not _table_exists(client, new):
        print(f"CREATE {new}")
        client.execute(cfg["ddl"])
    else:
        print(f"复用已有 {new}（可用 --recreate 重建）")

    time_col = cfg["time_col"]
    part_expr = cfg["partition_expr"]
    select_cols = cfg["select"].strip()

    for i, part in enumerate(parts, 1):
        # 已写入该分区则跳过（按源分区过滤后比对行数）
        src_part_cnt = client.execute(
            f"SELECT count() FROM {src} WHERE {part_expr} = %(p)s",
            {"p": int(part) if part.isdigit() else part},
        )[0][0]
        new_part_cnt = client.execute(
            f"SELECT count() FROM {new} WHERE {part_expr} = %(p)s",
            {"p": int(part) if part.isdigit() else part},
        )[0][0]
        # 允许 FINAL 去重导致 new 略少；仅当明显偏少（<95%）时重拷
        if src_part_cnt > 0 and new_part_cnt >= src_part_cnt * 0.95:
            print(
                f"[{i}/{len(parts)}] skip partition {part} "
                f"(src={src_part_cnt}, new={new_part_cnt})"
            )
            continue
        if new_part_cnt > 0:
            # 新表分区 ID 与源一致时（day/5min 均为 YYYYMM）直接 DROP PARTITION
            print(
                f"[{i}/{len(parts)}] 分区 {part} 不完整，DROP 后重拷 "
                f"(new={new_part_cnt} < src={src_part_cnt})"
            )
            try:
                client.execute(
                    f"ALTER TABLE {new} DROP PARTITION %(p)s",
                    {"p": part},
                )
            except Exception as exc:  # noqa: BLE001
                print(f"  DROP PARTITION 失败，改用 DELETE: {exc}")
                client.execute(
                    f"ALTER TABLE {new} DELETE WHERE {part_expr} = %(p)s",
                    {"p": int(part) if part.isdigit() else part},
                )
                time.sleep(1.0)

        t0 = time.time()
        sql = (
            f"INSERT INTO {new} "
            f"SELECT {select_cols} FROM {src} "
            f"WHERE {part_expr} = %(p)s"
        )
        params = {"p": int(part) if part.isdigit() else part}
        client = _execute_with_retry(
            client,
            sql,
            params,
            label=f"partition {part}",
        )
        elapsed = time.time() - t0
        print(
            f"[{i}/{len(parts)}] partition {part}: "
            f"src_rows={src_part_cnt} ({elapsed:.1f}s)"
        )

    new_count = _count(client, new)
    src_final = _count(client, src, final=True)
    new_final = _count(client, new, final=True)
    print(
        f"新表行数: {new_count} (源 {src_count}; "
        f"FINAL src={src_final} new={new_final})"
    )
    # ReplacingMergeTree 源表可能含未合并重复行；以 FINAL 对齐为准
    if new_final != src_final:
        print("WARNING: FINAL 行数不一致，请检查后再 --swap")
    elif new_count != src_count:
        print(
            "INFO: 原始 count 不一致但 FINAL 一致"
            "（源表存在可替换重复行），允许 swap"
        )

    check_sql = cfg["sample_check"].format(src=src, new=new)
    try:
        check = client.execute(check_sql)
        print(f"抽样校验 (cnt, max_open_diff, max_amt_diff): {check}")
    except Exception as exc:  # noqa: BLE001
        print(f"抽样校验失败（可忽略）: {exc}")

    if not swap:
        print("未切换。确认无误后加 --swap 执行 RENAME。")
        return

    if new_final != src_final:
        raise SystemExit("FINAL 行数不一致，拒绝 swap")

    # 备份名冲突则先丢掉旧备份
    if _table_exists(client, bak):
        print(f"DROP 旧备份 {bak}")
        client.execute(f"DROP TABLE IF EXISTS {bak}")

    # RENAME: src -> bak, new -> src
    src_db, src_tbl = src.split(".", 1)
    new_db, new_tbl = new.split(".", 1)
    bak_db, bak_tbl = bak.split(".", 1)
    rename_sql = (
        f"RENAME TABLE {src_db}.{src_tbl} TO {bak_db}.{bak_tbl}, "
        f"{new_db}.{new_tbl} TO {src_db}.{src_tbl}"
    )
    print(rename_sql)
    client.execute(rename_sql)
    print(f"已切换：{src} 现在为 Int 表；旧 Float64 在 {bak}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--table",
        choices=["day", "5min", "1min", "all"],
        default="all",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--swap",
        action="store_true",
        help="拷贝并校验后 RENAME 切换（旧表改名为 *_f64_bak）",
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="若 *_new 已存在则 DROP 重建",
    )
    args = parser.parse_args()

    keys = list(TABLES) if args.table == "all" else [args.table]
    client = _client()
    for key in keys:
        migrate_one(
            client,
            key,
            dry_run=args.dry_run,
            swap=args.swap,
            recreate=args.recreate,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
