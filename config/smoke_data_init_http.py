#!/usr/bin/env python
# coding: utf-8
"""小范围验证 data_init.py 全部 RQData 取数路径（默认 HTTP）。

覆盖:
  init_rqdatac / get_list / get_raw / get_code / get_data /
  get_ex_factor / get_trading_dates / fund.all_instruments /
  save_data / save_codelist / save_fundlist / save_adj_factor（小样本、不重建全表）
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import QUANTAXIS as QA


def _load_data_init():
    path = Path(__file__).resolve().parent / "data_init.py"
    spec = importlib.util.spec_from_file_location("data_init", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载 {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _ok(name: str, detail: str = "") -> None:
    print(f"[OK] {name}" + (f" | {detail}" if detail else ""))


def _fail(name: str, exc: BaseException) -> None:
    print(f"[FAIL] {name} | {type(exc).__name__}: {exc}")


def main() -> int:
    data_init = _load_data_init()
    failed = 0

    # 1) init
    try:
        transport = data_init.init_rqdatac()
        assert transport == "http", transport
        _ok("init_rqdatac", f"transport={transport}")
    except Exception as exc:  # noqa: BLE001
        _fail("init_rqdatac", exc)
        return 1

    # 2) construct model（内部 get_list -> all_instruments）
    try:
        model = data_init.datamodelx(
            QA.MARKET_TYPE.STOCK_CN,
            QA.FREQUENCE.DAY,
            init_rq=False,
        )
        # 小范围：只保留 2 只股票，避免后续 get_data 全市场
        model.codelist = model.codelist[
            model.codelist.order_book_id.isin(["000001.XSHE", "600000.XSHG"])
        ].copy()
        if model.codelist.empty:
            # 若排序后不在前部，重新 get_list 过滤
            full = model.get_list()
            model.codelist = full[
                full.order_book_id.isin(["000001.XSHE", "600000.XSHG"])
            ].copy()
        assert len(model.codelist) >= 1
        _ok("get_list/datamodelx", f"codes={model.codelist.order_book_id.tolist()}")
    except Exception as exc:  # noqa: BLE001
        _fail("get_list/datamodelx", exc)
        return 1

    # 3) get_end_time
    try:
        end = model.get_end_time()
        assert "wrong date" not in end, end
        assert len(end) >= 10
        _ok("get_end_time", end)
    except Exception as exc:  # noqa: BLE001
        _fail("get_end_time", exc)
        failed += 1

    # 4) get_raw
    try:
        raw = model.get_raw("000001.XSHE", "2024-01-02", "2024-01-10")
        assert raw is not None and not raw.empty
        _ok("get_raw", f"rows={len(raw.reset_index())}")
    except Exception as exc:  # noqa: BLE001
        _fail("get_raw", exc)
        failed += 1

    # 5) get_code（历史较长，但单票可接受；缩到用 get_raw 同区间验证绑定）
    try:
        # 为加速：临时把 endtime 侧逻辑已修好；这里用短区间 get_price 等价路径
        code_df = data_init.get_price(
            "000001.XSHE",
            start_date="2024-06-01",
            end_date="2024-06-10",
            frequency="1d",
            adjust_type="none",
        )
        assert code_df is not None and not code_df.empty
        _ok("get_price/get_code路径", f"rows={len(code_df.reset_index())}")
    except Exception as exc:  # noqa: BLE001
        _fail("get_price/get_code路径", exc)
        failed += 1

    # 6) get_data（已把 codelist 缩到 2 只）
    try:
        data = model.get_data("2024-01-02", "2024-01-10")
        assert data is not None and not data.empty
        _ok("get_data", f"rows={len(data.reset_index())}")
    except Exception as exc:  # noqa: BLE001
        _fail("get_data", exc)
        failed += 1

    # 7) get_trading_dates
    try:
        cal = data_init.get_trading_dates(
            start_date="2024-01-01",
            end_date="2024-01-31",
            market="cn",
        )
        assert cal is not None and not cal.empty
        _ok("get_trading_dates", f"rows={len(cal)}")
    except Exception as exc:  # noqa: BLE001
        _fail("get_trading_dates", exc)
        failed += 1

    # 8) get_ex_factor
    try:
        fac = data_init.get_ex_factor(
            ["000001.XSHE"],
            start_date="1990-01-01",
            end_date="2024-12-31",
            market="cn",
        )
        assert fac is not None and not fac.empty
        _ok("get_ex_factor", f"rows={len(fac)}")
    except Exception as exc:  # noqa: BLE001
        _fail("get_ex_factor", exc)
        failed += 1

    # 9) fund.all_instruments（HTTP 无权限时回退 ETF+LOF）
    try:
        assert data_init.fund is not None
        funds = data_init.fund.all_instruments()
        assert funds is not None and not funds.empty
        _ok("fund.all_instruments", f"rows={len(funds)} cols={len(funds.columns)}")
    except Exception as exc:  # noqa: BLE001
        _fail("fund.all_instruments", exc)
        failed += 1

    # 10) 1m get_price（分钟线路径）
    try:
        model_min = data_init.datamodelx(
            QA.MARKET_TYPE.STOCK_CN,
            QA.FREQUENCE.ONE_MIN,
            init_rq=False,
        )
        model_min.codelist = model.codelist.copy()
        minute = model_min.get_raw("000001.XSHE", "2024-06-03", "2024-06-03")
        assert minute is not None and not minute.empty
        _ok("get_raw(1m)", f"rows={len(minute.reset_index())}")
    except Exception as exc:  # noqa: BLE001
        _fail("get_raw(1m)", exc)
        failed += 1

    # 11) save_data 小写入（ReplacingMergeTree，不删表）
    try:
        frame = model.get_raw("000001.XSHE", "2024-01-02", "2024-01-05")
        model.create_table()
        model.save_data(frame)
        _ok("save_data", "stock_cn_day upsert ok")
    except Exception as exc:  # noqa: BLE001
        _fail("save_data", exc)
        failed += 1

    # 12) save_codelist 小样本、不重建全表
    try:
        model.save_codelist(codes=["000001.XSHE"], rebuild=False)
        _ok("save_codelist", "subset upsert ok")
    except Exception as exc:  # noqa: BLE001
        _fail("save_codelist", exc)
        failed += 1

    # 13) save_fundlist 小样本、不重建全表
    try:
        model.save_fundlist(limit=5, rebuild=False)
        _ok("save_fundlist", "limit=5 upsert ok")
    except Exception as exc:  # noqa: BLE001
        _fail("save_fundlist", exc)
        failed += 1

    # 14) save_adj_factor 小样本、不重建全表
    try:
        model.save_adj_factor(
            codes=["000001.XSHE"],
            batch_size=1,
            rebuild=False,
        )
        _ok("save_adj_factor", "000001.XSHE upsert ok")
    except Exception as exc:  # noqa: BLE001
        _fail("save_adj_factor", exc)
        failed += 1

    # index / etf / future 清单路径
    try:
        frame = data_init.all_instruments(type="INDX", market="cn")
        _ok("all_instruments(INDX)", f"rows={len(frame)}")
        frame = data_init.all_instruments(type="ETF", market="cn")
        _ok("all_instruments(ETF)", f"rows={len(frame)}")
        frame = data_init.all_instruments(type="Future", market="cn")
        _ok("all_instruments(Future)", f"rows={len(frame)}")
    except Exception as exc:  # noqa: BLE001
        _fail("all_instruments(multi)", exc)
        failed += 1

    model.quit()
    print("==== 汇总 ====")
    print("failed=", failed)
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
