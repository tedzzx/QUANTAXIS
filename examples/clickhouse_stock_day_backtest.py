#!/usr/bin/env python3
"""用 ClickHouse 日线 + QARSBacktest（qa-rs）跑均线股票回测。

行情来自 ``quantaxis.stock_cn_day``，写成 qa-rs parquet 后交给
``QARSBacktest.run``。策略在 ``on_calc`` 做 MA20 金叉死叉，下单走
``QAMarket.target_amount``。不依赖 Mongo / RabbitMQ。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from dataclasses import dataclass
from typing import Any

import pandas as pd  # type: ignore[import-untyped]
from qaenv import (  # type: ignore[import-untyped]
    clickhouse_ip,
    clickhouse_password,
    clickhouse_port,
    clickhouse_user,
)

from QUANTAXIS.QAFetch.QAClickhouse import QACKClient, stock_format
from QUANTAXIS.QARSBridge import QARSBacktest, QARSStrategy, has_qars_support
from QUANTAXIS.QARSBridge.qars_backtest import write_qars_day_parquet

MA_PERIOD: int = 20
TRADE_SHARES: int = 1000
DEFAULT_INIT_CASH: float = 1_000_000.0
DEFAULT_CODE: str = "000001"
DEFAULT_START: str = "2025-01-01"
DEFAULT_END: str = "2026-08-07"
ACCOUNT_NAME: str = "ma_cross"


@dataclass(frozen=True)
class BacktestConfig:
    """回测运行参数。"""

    code: str
    start: str
    end: str
    init_cash: float
    ma_period: int
    shares: int


class MACrossStrategy(QARSStrategy):
    """收盘价上穿 MA 买入、下穿卖出。"""

    def __init__(
        self,
        data_path: str,
        order_book_id: str,
        init_cash: float,
        ma_period: int,
        shares: int,
        account_name: str = ACCOUNT_NAME,
    ) -> None:
        """绑定行情文件与交易参数。

        Args:
            data_path: qa-rs parquet 路径。
            order_book_id: ``000001.XSHE`` 形式代码。
            init_cash: 初始资金。
            ma_period: 均线周期。
            shares: 每次买入股数。
            account_name: qa-rs 账户名。
        """

        super().__init__(
            data_path,
            portfolio_name="clickhouse_day",
            account_name=account_name,
            init_cash=init_cash,
        )
        self.order_book_id = order_book_id
        self.ma_period = ma_period
        self.shares = shares
        self.closes: list[float] = []

    def on_calc(self) -> None:
        """按当日截面更新均线并调仓。"""

        raw = self.market.get_mkt_slice()
        slice_map = json.loads(raw) if raw else {}
        bar = slice_map.get(self.order_book_id)
        if not isinstance(bar, dict):
            return
        close = float(bar.get("close", 0.0))
        if close <= 0:
            return
        self.closes.append(close)
        if len(self.closes) <= self.ma_period:
            return

        current_ma = sum(self.closes[-self.ma_period :]) / self.ma_period
        prev_ma = sum(self.closes[-self.ma_period - 1 : -1]) / self.ma_period
        prev_close = self.closes[-2]
        cross_up = prev_close <= prev_ma and close > current_ma
        cross_down = prev_close >= prev_ma and close < current_ma
        today = str(getattr(self.market, "today", ""))

        if cross_up and not self._holding():
            self.market.target_amount(
                {self.account_name: {self.order_book_id: float(self.shares)}},
                "close",
            )
            self.buy_count += 1
            print(
                f"{today} BUY  {self.order_book_id} "
                f"price={close:.2f} ma={current_ma:.2f} shares={self.shares}"
            )
        elif cross_down and self._holding():
            self.market.target_amount(
                {self.account_name: {self.order_book_id: 0.0}},
                "close",
            )
            self.sell_count += 1
            print(
                f"{today} SELL {self.order_book_id} "
                f"price={close:.2f} ma={current_ma:.2f}"
            )

    def _holding(self) -> bool:
        """当前是否持有标的。"""

        try:
            positions = json.loads(self.market.get_positions_qifi(self.account_name))
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        if not isinstance(positions, dict):
            return False
        pos = positions.get(self.order_book_id)
        if not isinstance(pos, dict):
            return False
        volume = float(pos.get("volume_long", 0.0) or 0.0)
        if volume <= 0.0:
            volume = float(pos.get("volume_long_his", 0.0) or 0.0) + float(
                pos.get("volume_long_today", 0.0) or 0.0
            )
        return volume > 0.0


def _parse_args() -> BacktestConfig:
    """解析命令行参数。"""

    parser = argparse.ArgumentParser(description="ClickHouse 日线 + QARSBacktest 均线回测")
    parser.add_argument("--code", default=DEFAULT_CODE, help="六位股票代码")
    parser.add_argument("--start", default=DEFAULT_START, help="起始日期")
    parser.add_argument("--end", default=DEFAULT_END, help="结束日期")
    parser.add_argument(
        "--init-cash",
        type=float,
        default=DEFAULT_INIT_CASH,
        help="初始资金",
    )
    parser.add_argument(
        "--ma-period",
        type=int,
        default=MA_PERIOD,
        help="均线周期",
    )
    parser.add_argument(
        "--shares",
        type=int,
        default=TRADE_SHARES,
        help="每次买入股数",
    )
    args = parser.parse_args()
    return BacktestConfig(
        code=str(args.code).strip().split(".")[0],
        start=args.start,
        end=args.end,
        init_cash=args.init_cash,
        ma_period=args.ma_period,
        shares=args.shares,
    )


def _normalize_day_frame(data: object) -> pd.DataFrame:
    """把 QACKClient 返回值整理成 qa-rs 日线表。"""

    if hasattr(data, "data"):
        frame = pd.DataFrame(data.data)
    else:
        frame = pd.DataFrame(data)
    if frame.empty:
        return pd.DataFrame()
    frame = frame.reset_index()
    frame = frame.loc[:, ~frame.columns.duplicated()]
    if "order_book_id" not in frame.columns and "code" in frame.columns:
        frame["order_book_id"] = frame["code"].astype(str)
    if "date" not in frame.columns:
        return pd.DataFrame()
    return frame.drop_duplicates(subset=["date", "order_book_id"]).sort_values("date")


def load_stock_day(
    client: QACKClient,
    code: str,
    start: str,
    end: str,
) -> tuple[pd.DataFrame, str]:
    """读取日线；有复权因子则前复权，否则不复权。

    Args:
        client: ClickHouse 客户端。
        code: 六位代码。
        start: 起始日期。
        end: 结束日期。

    Returns:
        (日线 DataFrame, 复权说明)。
    """

    order_book_id: str = stock_format(code)
    use_qfq: bool = False
    if client.table_exists("quantaxis.stock_adj"):
        adj = client.get_stock_adj(code, start, end)
        use_qfq = adj is not None and not adj.empty
    if use_qfq:
        raw = client.get_stock_day_qfq(code, start, end)
        return _normalize_day_frame(raw), "前复权 (stock_adj)"
    raw = client.get_stock_day(code, start, end)
    return _normalize_day_frame(raw), f"不复权 ({order_book_id})"


def _print_report(
    config: BacktestConfig,
    bars: pd.DataFrame,
    adj_note: str,
    parquet_path: str,
    result: dict[str, Any],
) -> None:
    """打印回测摘要。"""

    balance = float(result.get("balance", config.init_cash))
    available = float(result.get("available", 0.0))
    ret = float(result.get("total_return", 0.0)) * 100.0
    print("==== 回测摘要 ====")
    print(f"代码={stock_format(config.code)} 区间={config.start}..{config.end} 行情={adj_note}")
    print(f"parquet={parquet_path}")
    print(f"K 线根数={len(bars)} MA={config.ma_period}")
    print(f"初始资金={config.init_cash:.2f}")
    print(f"期末权益={balance:.2f} 可用={available:.2f} 收益率={ret:.2f}%")
    print(f"买入次数={result.get('buy', 0)} 卖出次数={result.get('sell', 0)}")
    positions = result.get("positions") or {}
    if not positions:
        print("持仓=空")
    else:
        print(f"持仓={positions}")
    if int(result.get("buy", 0)) == 0 and int(result.get("sell", 0)) == 0:
        print("区间内无信号")


def main() -> int:
    """入口。"""

    if not has_qars_support():
        print(
            "本示例需要 qars3.Backtest。请执行: pip install -e /path/to/qa-rs",
            file=sys.stderr,
        )
        return 1

    config = _parse_args()
    client = QACKClient(
        host=clickhouse_ip,
        port=int(clickhouse_port),
        user=clickhouse_user,
        password=clickhouse_password or "",
    )
    bars, adj_note = load_stock_day(client, config.code, config.start, config.end)
    if bars.empty:
        print(
            f"ClickHouse 无日线: {config.code} {config.start}..{config.end}",
            file=sys.stderr,
        )
        return 1

    print(
        f"已加载 {len(bars)} 根日线 {stock_format(config.code)} "
        f"{pd.to_datetime(bars['date']).iloc[0].date()}.."
        f"{pd.to_datetime(bars['date']).iloc[-1].date()} ({adj_note})"
    )

    handle, parquet_tmp = tempfile.mkstemp(prefix="qars_ch_day_", suffix=".pq")
    os.close(handle)
    parquet_path = write_qars_day_parquet(bars, parquet_tmp)
    print(f"已写入 qa-rs parquet: {parquet_path}")

    backtest = QARSBacktest(config.start, config.end, parquet_path)
    strategy = MACrossStrategy(
        data_path=parquet_path,
        order_book_id=stock_format(config.code),
        init_cash=config.init_cash,
        ma_period=config.ma_period,
        shares=config.shares,
    )
    result = backtest.run(strategy)
    _print_report(config, bars, adj_note, parquet_path, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
