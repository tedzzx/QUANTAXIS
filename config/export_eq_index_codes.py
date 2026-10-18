#!/usr/bin/env python
# coding: utf-8
"""从 EQ 静态码表导出指数代码，写入 CSV。不修改 ClickHouse。

历史行情端口用 HqApplication.getSync(market, "code")。
本账号的 13011 不提供 RqApplication.rebuildSync（连接报 Invalid proxy）。
证券类别字段 ``800``，名称字段 ``801``，代码字段 ``0``。
指数类别为 0–6（指数、交易所指数、亚洲、国际、期货指数、现货指数、国证）。

用法（在仓库根目录）:
  .venv/bin/python config/export_eq_index_codes.py
"""

from __future__ import annotations

import csv
import json
import os
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from QUANTAXIS.QAFetch.QAEq import close_client, get_client

# quotesdbtype.h SecurityType::index .. index_china
_INDEX_TYPES = frozenset(range(0, 7))
_MARKETS = (
    ("shl2", "XSHG"),
    ("szl2", "XSHE"),
    ("cnsi", "INDX"),
)
_OUT = ROOT / "config" / "eq_index_codelist.csv"


def _decode(item: object) -> str | None:
    if item is None or item == "":
        return None
    if isinstance(item, (bytes, bytearray)):
        try:
            item = item.decode("utf-8")
        except UnicodeDecodeError:
            item = item.decode("gbk", errors="ignore")
    if not isinstance(item, str):
        return None
    text = item.strip()
    return text or None


def _as_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        try:
            return int(float(value))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None


def _rows_from_quotes(market: str, suffix: str, quotes: list) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    types: Counter[str] = Counter()
    printed = False
    for item in quotes:
        text = _decode(item)
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        if not printed:
            print(f"sample {market} {text[:500]}", flush=True)
            printed = True
        type_raw = payload.get("800")
        types[str(type_raw)] += 1
        security_type = _as_int(type_raw)
        # cnsi 本身是中证指数市场，记录里可能没有 800
        if market != "cnsi" and security_type not in _INDEX_TYPES:
            continue
        if market == "cnsi" and security_type is not None and security_type not in _INDEX_TYPES:
            continue
        code = str(payload.get("0") or "").strip()
        if not code:
            continue
        name = str(payload.get("801") or "").strip()
        rows.append(
            {
                "code": code,
                "name": name,
                "security_type": "" if security_type is None else str(security_type),
                "market": market,
                "order_book_id": f"{code}.{suffix}",
            }
        )
    print(f"{market} raw={sum(types.values())} index={len(rows)} types={types.most_common(8)}", flush=True)
    return rows


def main() -> int:
    client = get_client()
    collected: list[dict[str, str]] = []
    try:
        for market, suffix in _MARKETS:
            try:
                quotes = client.get_sync(market, "code", [""], 0, 0, 0, 0, 0)
            except Exception as exc:  # noqa: BLE001
                print(f"{market} 失败，跳过: {exc}", flush=True)
                continue
            quotes = [item for item in list(quotes or []) if _decode(item)]
            if not quotes:
                print(f"{market} 无数据，跳过", flush=True)
                continue
            collected.extend(_rows_from_quotes(market, suffix, quotes))
    finally:
        close_client()

    deduped: dict[str, dict[str, str]] = {}
    for row in collected:
        deduped[row["order_book_id"]] = row
    rows = sorted(deduped.values(), key=lambda item: item["order_book_id"])
    _OUT.parent.mkdir(parents=True, exist_ok=True)
    with _OUT.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["code", "name", "security_type", "market", "order_book_id"],
        )
        writer.writeheader()
        writer.writerows(rows)
    print(f"合计 {len(rows)} 只指数 → {_OUT}", flush=True)
    return 0 if rows else 2


if __name__ == "__main__":
    raise SystemExit(main())
