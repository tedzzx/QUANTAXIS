#!/usr/bin/env python
# coding: utf-8
"""全市场通达信本地分钟线 → MongoDB。

路径（WSL）:
  /mnt/c/iTendx/vipdoc/{sh,sz}/fzline  → stock_min (type=5min)
  /mnt/c/iTendx/vipdoc/{sh,sz}/minline → stock_min (type=1min)

与 QA_SU_save_stock_min / QA_fetch_stock_min 一致：各周期共用 stock_min。
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from QUANTAXIS.QASU.save_tdx_file import QA_save_tdx_to_mongo

JOBS = [
    ("/mnt/c/iTendx/vipdoc/sh/fzline", "5min"),
    ("/mnt/c/iTendx/vipdoc/sz/fzline", "5min"),
    ("/mnt/c/iTendx/vipdoc/sh/minline", "1min"),
    ("/mnt/c/iTendx/vipdoc/sz/minline", "1min"),
]


def main() -> int:
    summaries = []
    for path, bar_type in JOBS:
        print(f"==== START {bar_type} {path} ====", flush=True)
        if not os.path.isdir(path):
            print(f"MISSING {path}", flush=True)
            continue
        summary = QA_save_tdx_to_mongo(path, bar_type=bar_type)
        print(f"==== DONE {summary} ====", flush=True)
        summaries.append(summary)

    print("==== ALL DONE ====", flush=True)
    for item in summaries:
        print(item, flush=True)
    failed = sum(len(item.get("failed") or []) for item in summaries)
    return 2 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
