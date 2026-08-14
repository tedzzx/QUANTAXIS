#!/usr/bin/env python
# coding: utf-8
"""将 MongoDB stock_min_five 合并到 stock_min（与 QA_SU_save_stock_min 一致）。

步骤:
1. 确保 stock_min 有 code/type/datetime 与 time_stamp 索引
2. $merge 拷贝 stock_min_five → stock_min（去掉 _id，按新文档插入）
3. 校验 5min 条数后，将 stock_min_five 重命名为 stock_min_five_bak
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pymongo

from QUANTAXIS.QAUtil import DATABASE


def main() -> int:
    db = DATABASE
    src = db.stock_min_five
    dst = db.stock_min

    src_n = src.estimated_document_count()
    dst_before = dst.estimated_document_count()
    five_before = dst.count_documents({"type": "5min"})
    print(
        f"before: stock_min_five≈{src_n} stock_min≈{dst_before} "
        f"stock_min(type=5min)={five_before}",
        flush=True,
    )
    if src_n == 0:
        print("stock_min_five 为空，跳过合并", flush=True)
        return 1

    print("ensure indexes on stock_min ...", flush=True)
    dst.create_index(
        [("code", 1), ("datetime", 1), ("type", 1)],
        unique=True,
        background=True,
        name="code_datetime_type",
    )
    dst.create_index(
        [("code", 1), ("time_stamp", 1), ("type", 1)],
        background=True,
        name="code_time_stamp_type",
    )
    dst.create_index([("code", 1), ("date", 1)], background=True)

    t0 = time.time()
    print("merging via aggregation $merge ...", flush=True)
    # 去掉 _id，全部作为新文档插入，避免与目标 _id 冲突
    list(
        src.aggregate(
            [
                {"$project": {"_id": 0}},
                {
                    "$merge": {
                        "into": "stock_min",
                        "whenMatched": "keepExisting",
                        "whenNotMatched": "insert",
                    }
                },
            ],
            allowDiskUse=True,
        )
    )
    print(f"merge done in {time.time() - t0:.1f}s", flush=True)

    five_after = dst.count_documents({"type": "5min"})
    dst_after = dst.estimated_document_count()
    print(
        f"after: stock_min≈{dst_after} stock_min(type=5min)={five_after}",
        flush=True,
    )
    if five_after < src_n * 0.99:
        print(
            f"WARN: merged 5min count {five_after} << source {src_n}, "
            "不重命名源表",
            flush=True,
        )
        return 2

    # 备份源表名，避免误删
    bak = "stock_min_five_bak"
    if bak in db.list_collection_names():
        print(f"drop existing {bak}", flush=True)
        db.drop_collection(bak)
    print(f"rename stock_min_five -> {bak}", flush=True)
    src.rename(bak)
    print("OK", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
