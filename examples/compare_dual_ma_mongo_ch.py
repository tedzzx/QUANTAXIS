#!/usr/bin/env python3
"""同一套 QAStrategyStockBase + MA5/MA20，分别用 mongo / clickhouse 行情回测。

``source`` 只切换行情；账户仍是 QIFI_Account，settle 写入 Mongo ``quantaxis.history``。
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import random
import time
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from typing import Any

from QUANTAXIS.QAFetch.QAClickhouse import QACKClient, stock_format
from QUANTAXIS.QAStrategy.qamultibase import QAStrategyStockBase

MA_FAST: int = 5
MA_SLOW: int = 20
TRADE_SHARES: int = 1000
DEFAULT_INIT_CASH: float = 20_000_000.0
DEFAULT_START: str = "2015-01-01"
DEFAULT_END: str = "2025-12-31"
DEFAULT_N: int = 100
DEFAULT_SEED: int = 42
DEFAULT_MIN_BARS: int = 2000
SMOKE_START: str = "2024-01-01"
SMOKE_END: str = "2024-06-30"


@dataclass
class RunMetrics:
    """单次回测指标。"""

    source: str
    actual_source: str
    n_codes: int
    n_bars: int
    load_seconds: float
    loop_seconds: float
    total_seconds: float
    init_cash: float
    equity: float
    available: float
    return_pct: float
    n_buy: int
    n_sell: int
    n_positions: int


class DualMAStrategy(QAStrategyStockBase):
    """按标的维护收盘价：MA5 上穿 MA20 买 1000 股，下穿卖出昨仓。"""

    def user_init(self) -> None:
        """初始化均线缓存与成交计数。"""

        self._closes: dict[str, list[float]] = {}
        self.n_buy: int = 0
        self.n_sell: int = 0
        self.load_seconds: float = 0.0
        self.loop_seconds: float = 0.0
        self.n_bars: int = 0

    def debug(self) -> None:
        """回测：分别统计取数与循环耗时。"""

        self.running_mode = "backtest"
        t0 = time.perf_counter()
        self._init_backtest_account()
        data = self._load_backtest_bars()
        self.load_seconds = time.perf_counter() - t0
        self.n_bars = len(data.data)
        print(
            f"market source: {self.source} ({self._market_source_actual}) bars={self.n_bars}"
        )
        t1 = time.perf_counter()
        data.data.apply(self.x1, axis=1)
        self.loop_seconds = time.perf_counter() - t1

    def on_bar(self, bar: Any) -> None:
        """单根 K 线：金叉买入、死叉卖出。"""

        code = str(bar.name[1]).split(".", maxsplit=1)[0]
        close = float(bar["close"])
        hist = self._closes.setdefault(code, [])
        hist.append(close)
        if len(hist) < MA_SLOW + 1:
            return
        ma_fast = sum(hist[-MA_FAST:]) / float(MA_FAST)
        ma_slow = sum(hist[-MA_SLOW:]) / float(MA_SLOW)
        prev_fast = sum(hist[-MA_FAST - 1 : -1]) / float(MA_FAST)
        prev_slow = sum(hist[-MA_SLOW - 1 : -1]) / float(MA_SLOW)
        pos = self.get_positions(code)
        volume_his = int(getattr(pos, "volume_long_his", 0) or 0)
        volume_today = int(getattr(pos, "volume_long_today", 0) or 0)
        golden = prev_fast <= prev_slow and ma_fast > ma_slow
        death = prev_fast >= prev_slow and ma_fast < ma_slow
        if golden and volume_his + volume_today <= 0:
            self.send_order(
                direction="BUY",
                offset="OPEN",
                code=code,
                price=close,
                volume=TRADE_SHARES,
            )
            self.n_buy += 1
        elif death and volume_his > 0:
            self.send_order(
                direction="SELL",
                offset="CLOSE",
                code=code,
                price=close,
                volume=volume_his,
            )
            self.n_sell += 1

    def on_tick(self, tick: Any) -> None:
        """日线回测不使用 tick。"""

        return

    def on_1min_bar(self) -> None:
        """日线回测不使用分钟回调。"""

        return


def _to_mongo_code(order_book_id: str) -> str:
    """ClickHouse ``000001.XSHE`` → Mongo 六位码。"""

    return str(order_book_id).split(".", maxsplit=1)[0]


def pick_universe(
    client: QACKClient,
    start: str,
    end: str,
    n: int,
    seed: int,
    min_bars: int,
) -> list[str]:
    """从 CH 日线中筛选区间内根数足够的股票，再固定种子抽样。"""

    sql = (
        "SELECT order_book_id, count() AS n FROM quantaxis.stock_cn_day "
        f"WHERE date >= '{start}' AND date <= '{end}' "
        f"GROUP BY order_book_id HAVING n >= {int(min_bars)} "
        "ORDER BY order_book_id"
    )
    frame = client.query_dataframe(sql)
    if frame is None or frame.empty:
        raise RuntimeError("ClickHouse 没有满足条件的日线标的")
    codes = [str(x) for x in frame["order_book_id"].tolist()]
    if client.table_exists("quantaxis.stock_cn_codelist"):
        listed = client.get_stock_list()
        if listed is not None and not listed.empty:
            active_col = "order_book_id" if "order_book_id" in listed.columns else None
            if active_col is None and "code" in listed.columns:
                active_col = "code"
            if active_col is not None:
                active = {
                    stock_format(str(x)) for x in listed[active_col].tolist()
                }
                codes = [c for c in codes if c in active]
    if len(codes) < n:
        raise RuntimeError(
            f"可用标的只有 {len(codes)} 只，少于请求的 {n}"
        )
    rng = random.Random(seed)
    return sorted(rng.sample(codes, n))


def _mark_to_market(strategy: DualMAStrategy) -> None:
    """用最新收盘价刷新持仓，便于读取期末权益。"""

    for code, hist in strategy._closes.items():
        if not hist:
            continue
        strategy.acc.on_price_change(code, hist[-1], strategy.running_time)


def run_one(
    source: str,
    codes: list[str],
    start: str,
    end: str,
    init_cash: float,
    quiet: bool,
) -> RunMetrics:
    """跑一次指定 source 的双均线回测。"""

    strategy_id = f"dualma_{source}_{int(time.time())}"
    strategy = DualMAStrategy(
        code=codes,
        frequence="day",
        strategy_id=strategy_id,
        start=start,
        end=end,
        init_cash=init_cash,
        source=source,
    )
    t0 = time.perf_counter()
    if quiet:
        with (
            open(os.devnull, "w", encoding="utf-8") as devnull,
            contextlib.redirect_stdout(devnull),
        ):
            strategy.debug()
    else:
        strategy.debug()
    total = time.perf_counter() - t0
    _mark_to_market(strategy)
    equity = float(getattr(strategy.acc, "balance", init_cash))
    available = float(getattr(strategy.acc, "available", 0.0))
    return RunMetrics(
        source=source,
        actual_source=str(strategy._market_source_actual),
        n_codes=len(codes),
        n_bars=int(strategy.n_bars),
        load_seconds=float(strategy.load_seconds),
        loop_seconds=float(strategy.loop_seconds),
        total_seconds=float(total),
        init_cash=float(init_cash),
        equity=equity,
        available=available,
        return_pct=(equity / init_cash - 1.0) * 100.0,
        n_buy=int(strategy.n_buy),
        n_sell=int(strategy.n_sell),
        n_positions=len(getattr(strategy.acc, "positions", {}) or {}),
    )


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    """命令行参数。"""

    parser = argparse.ArgumentParser(description="mongo vs clickhouse 双均线对比")
    parser.add_argument("--n", type=int, default=DEFAULT_N, help="抽样股票数")
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", default=DEFAULT_END)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--min-bars", type=int, default=DEFAULT_MIN_BARS)
    parser.add_argument("--init-cash", type=float, default=DEFAULT_INIT_CASH)
    parser.add_argument(
        "--only",
        choices=("both", "mongo", "clickhouse"),
        default="both",
    )
    parser.add_argument("--smoke", action="store_true", help="1 只股票短区间冒烟")
    parser.add_argument("--quiet", action="store_true", help="屏蔽回测过程打印")
    parser.add_argument(
        "--json-out",
        default="",
        help="把对比结果写成 JSON",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _print_metrics(metrics: RunMetrics) -> None:
    """打印单次结果。"""

    print(f"==== {metrics.source} ====")
    print(
        f"实际行情={metrics.actual_source} 标的={metrics.n_codes} K线={metrics.n_bars}"
    )
    print(
        f"加载={metrics.load_seconds:.2f}s 循环={metrics.loop_seconds:.2f}s 总计={metrics.total_seconds:.2f}s"
    )
    print(
        f"权益={metrics.equity:.2f} 可用={metrics.available:.2f} 收益={metrics.return_pct:.2f}% 买={metrics.n_buy} 卖={metrics.n_sell} 持仓标的={metrics.n_positions}"
    )


def main(argv: Iterable[str] | None = None) -> int:
    """入口。"""

    args = _parse_args(argv)
    client = QACKClient()
    if args.smoke:
        codes_ch = [stock_format("000001")]
        start, end, n = SMOKE_START, SMOKE_END, 1
        quiet = False
    else:
        codes_ch = pick_universe(
            client, args.start, args.end, args.n, args.seed, args.min_bars
        )
        start, end, n = args.start, args.end, args.n
        quiet = bool(args.quiet)
    codes_mongo = [_to_mongo_code(c) for c in codes_ch]
    print(
        f"区间={start}..{end} 标的={n} seed={args.seed} 资金={args.init_cash:.0f}"
    )
    print(f"CH 代码示例: {codes_ch[:5]}")
    print(f"Mongo 代码示例: {codes_mongo[:5]}")

    results: list[RunMetrics] = []
    if args.only in ("both", "mongo"):
        print("开始 mongo ...")
        results.append(
            run_one("mongo", codes_mongo, start, end, args.init_cash, quiet)
        )
        _print_metrics(results[-1])
    if args.only in ("both", "clickhouse"):
        print("开始 clickhouse ...")
        results.append(
            run_one("clickhouse", codes_ch, start, end, args.init_cash, quiet)
        )
        _print_metrics(results[-1])

    payload = {
        "start": start,
        "end": end,
        "n": n,
        "seed": args.seed,
        "init_cash": args.init_cash,
        "codes_ch": codes_ch,
        "codes_mongo": codes_mongo,
        "results": [asdict(item) for item in results],
    }
    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        print(f"已写入 {args.json_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
