# coding:utf-8
#
# The MIT License (MIT)
#
# Copyright (c) 2016-2021 yutiansut/QUANTAXIS
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""通达信本地 fzline/lc5（5 分钟）写入 MongoDB。

本地 ``.lc5`` 记录布局（32 字节）与 pytdx ``TdxLCMinBarReader`` 一致::

    <HHfffffII
    date(u16), time(u16),
    open/high/low/close/amount(float32),
    volume(u32), reserved(u32)

日期编码：``year = n//2048 + 2004``；时间：``hour = n//60, minute = n%60``。

协议侧 OHLC 亦多为 float32（参见 gotdx
https://github.com/bensema/gotdx 中 MAC/K 线字段的 ``Float32`` 解码）。
**不要**使用 ``TdxMinBarReader``（``<HHIIIIfII`` 整型/100），否则价格会变成千万级错值。
"""

from __future__ import annotations

import json
import os
import struct
from collections.abc import Iterable
from typing import Optional

import pandas as pd
from pytdx.reader import TdxLCMinBarReader, TdxMinBarReader

from QUANTAXIS.QAUtil import (
    DATABASE,
    QA_util_date_stamp,
    QA_util_log_info,
    QA_util_time_stamp,
)

# 与 pytdx.reader.lc_min_bar_reader.TdxLCMinBarReader 相同
_LC5_FLOAT_FMT = "<HHfffffII"
_LC5_REC_SIZE = 32


def _looks_like_stock_price(frame: pd.DataFrame) -> bool:
    """粗判 OHLC 是否像 A 股价格（排除 MinBarReader 误读）。"""

    if frame is None or frame.empty:
        return False
    sample = frame["close"].dropna()
    if sample.empty:
        return False
    # 误用整型格式时 close 常在 1e6~1e8
    return bool((sample > 0.01).all() and (sample < 100000).all())


def read_tdx_lc5(fname: str) -> pd.DataFrame:
    """读取通达信 vipdoc ``.lc5`` / ``.lc1`` 分钟线。

    优先 ``TdxLCMinBarReader``（float32 OHLC）；若结果不像股价，
    再回退 ``TdxMinBarReader``（旧整型格式）。
    """

    if not os.path.isfile(fname):
        raise FileNotFoundError(fname)

    # 空文件 / 非 32 字节对齐仍交给 reader，由底层报错或返回空
    reader_lc = TdxLCMinBarReader()
    frame = reader_lc.get_df(fname)
    if _looks_like_stock_price(frame):
        return frame

    # 极少数旧文件可能仍是整型布局
    with open(fname, "rb") as handle:
        head = handle.read(_LC5_REC_SIZE)
    if len(head) == _LC5_REC_SIZE:
        probe = struct.unpack(_LC5_FLOAT_FMT, head)
        # float 布局下 OHLC 应已合理；若仍不合理再试 MinBar
        ohlc = probe[2:6]
        if not all(0.01 < float(v) < 100000 for v in ohlc):
            frame_min = TdxMinBarReader().get_df(fname)
            if _looks_like_stock_price(frame_min):
                QA_util_log_info(
                    f"read_tdx_lc5 fallback TdxMinBarReader: {fname}"
                )
                return frame_min

    if frame is None or frame.empty:
        return pd.DataFrame(
            columns=["open", "high", "low", "close", "amount", "volume"]
        )
    # LC 读出但不像股价：仍返回 LC 结果并告警，避免静默写错库
    QA_util_log_info(
        f"WARN read_tdx_lc5 price range suspicious: {fname} "
        f"close=[{frame['close'].min()}, {frame['close'].max()}]"
    )
    return frame


def _bar_type_from_filename(filename: str) -> Optional[str]:
    """由后缀推断周期：``.lc5``→``5min``，``.lc1``→``1min``。"""

    name = os.path.basename(filename).lower()
    if name.endswith(".lc5"):
        return "5min"
    if name.endswith(".lc1"):
        return "1min"
    return None


def _is_a_share_lc_file(filename: str, bar_type: Optional[str] = None) -> bool:
    """是否为沪深 A 股 lc1/lc5（sh60/68、sz00/30）。"""

    detected = _bar_type_from_filename(filename)
    if detected is None:
        return False
    if bar_type is not None and detected != bar_type:
        return False
    name = os.path.basename(filename).lower()
    if len(name) < 10:
        return False
    market = name[:2]
    code = name[2:8]
    if not code.isdigit():
        return False
    if market == "sh":
        return code.startswith(("60", "68"))
    if market == "sz":
        return code.startswith(("00", "30"))
    return False


def _default_coll_name(bar_type: str) -> str:
    # 与 QA_SU_save_stock_min / QA_fetch_stock_min 一致：1min/5min 同表 stock_min
    if bar_type in ("1min", "5min"):
        return "stock_min"
    raise ValueError(f"不支持的 bar_type: {bar_type}")


def _ensure_indexes(coll) -> None:
    coll.create_index(
        [("code", 1), ("datetime", 1), ("type", 1)],
        unique=True,
        name="code_datetime_type",
        background=True,
    )
    coll.create_index([("code", 1), ("date", 1)], background=True)


def _is_a_share_lc5(filename: str) -> bool:
    """兼容旧名：仅匹配 ``.lc5`` A 股文件。"""

    return _is_a_share_lc_file(filename, "5min")


read_tdx_lc1 = read_tdx_lc5


def QA_save_tdx_to_mongo(
    file_dir: str,
    client=DATABASE,
    *,
    codes: Optional[Iterable[str]] = None,
    coll_name: Optional[str] = None,
    bar_type: Optional[str] = None,
    replace: bool = True,
    limit_files: int = 0,
) -> dict[str, object]:
    """把通达信 minline/fzline 目录下的分钟线写入 MongoDB。

    Arguments:
        file_dir: ``minline``（.lc1）或 ``fzline``（.lc5）目录，
            如 ``C:\\iTendx\\vipdoc\\sh\\minline`` /
            WSL ``/mnt/c/iTendx/vipdoc/sh/minline``。
        client: Mongo Database（默认 ``DATABASE``）。

    Keyword Arguments:
        codes: 仅保存指定六位代码；``None`` 表示目录内全部 A 股。
        coll_name: 集合名；默认 ``1min→stock_min``，``5min→stock_min_five``。
        bar_type: ``1min`` / ``5min``；``None`` 时按文件后缀自动识别。
        replace: 写入前按 code+type 删除旧数据，避免重复。
        limit_files: >0 时最多处理 N 个文件（小范围测试）。

    Returns:
        汇总字典：ok / failed / rows / files。
    """

    code_set = (
        {str(code).split(".")[0].zfill(6) for code in codes}
        if codes is not None
        else None
    )

    ok = 0
    rows_total = 0
    failed: list[tuple[str, str]] = []
    processed = 0
    used_coll: Optional[str] = None
    used_type: Optional[str] = None

    for root, _dirs, files in os.walk(file_dir):
        for file in sorted(files):
            detected = _bar_type_from_filename(file)
            if detected is None:
                continue
            if bar_type is not None and detected != bar_type:
                continue
            if not _is_a_share_lc_file(file, detected):
                continue
            code = file[2:8]
            if code_set is not None and code not in code_set:
                continue
            if limit_files > 0 and processed >= limit_files:
                break

            target_coll = coll_name or _default_coll_name(detected)
            coll = client[target_coll]
            if used_coll is None:
                _ensure_indexes(coll)
                used_coll = target_coll
                used_type = detected

            processed += 1
            fname = os.path.join(root, file)
            try:
                QA_util_log_info(
                    f"Now_saving {code}'s {detected} from {fname}"
                )
                df = read_tdx_lc5(fname)
                if df is None or df.empty:
                    failed.append((code, "empty frame"))
                    continue
                if not _looks_like_stock_price(df):
                    failed.append(
                        (
                            code,
                            f"invalid price range close="
                            f"[{df['close'].min()}, {df['close'].max()}]",
                        )
                    )
                    continue

                out = df.copy()
                out["code"] = code
                out["market"] = file[:2].lower()
                out["type"] = detected
                out["datetime"] = [str(x) for x in list(out.index)]
                out["date"] = [str(x)[0:10] for x in list(out.index)]
                out["time_stamp"] = out["datetime"].apply(QA_util_time_stamp)
                out["date_stamp"] = out["date"].apply(QA_util_date_stamp)
                if "vol" not in out.columns and "volume" in out.columns:
                    out["vol"] = out["volume"]

                for col in ("open", "high", "low", "close"):
                    out[col] = pd.to_numeric(out[col], errors="coerce").round(2)

                records = json.loads(out.to_json(orient="records"))
                if replace:
                    coll.delete_many({"code": code, "type": detected})
                if records:
                    batch = 5000
                    for offset in range(0, len(records), batch):
                        coll.insert_many(
                            records[offset : offset + batch],
                            ordered=False,
                        )
                ok += 1
                rows_total += len(records)
                QA_util_log_info(f"saved {code} rows={len(records)} type={detected}")
            except Exception as exc:  # noqa: BLE001
                failed.append((code, str(exc)))
                QA_util_log_info(f"FAIL {code}: {exc}")

        if limit_files > 0 and processed >= limit_files:
            break

    return {
        "files": processed,
        "ok": ok,
        "failed": failed,
        "rows": rows_total,
        "collection": used_coll or coll_name or "",
        "type": used_type or bar_type or "",
        "dir": file_dir,
    }


if __name__ == "__main__":
    file_dirs = [
        r"/mnt/c/iTendx/vipdoc/sh/fzline",
        r"/mnt/c/iTendx/vipdoc/sz/fzline",
        r"/mnt/c/iTendx/vipdoc/sh/minline",
        r"/mnt/c/iTendx/vipdoc/sz/minline",
    ]
    for item in file_dirs:
        if os.path.isdir(item):
            print(QA_save_tdx_to_mongo(item))
        else:
            print(f"skip missing dir: {item}")
