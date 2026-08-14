#!/usr/bin/env python3
"""用 ClickHouse 日线 + QARSAccount 跑均线股票回测。

行情来自 ``quantaxis.stock_cn_day``（``QACKClient``）。账户使用
``QARSAccount``（与 ``qarsbridge_example`` 相同 API）。不依赖 Mongo /
RabbitMQ。

策略对齐文档 SimpleMAStrategy：收盘价上穿 MA20 买入，下穿卖出；
每日 ``settle()`` 处理 A 股 T+1。
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from typing import Protocol

import pandas as pd  # type: ignore[import-untyped]
from qaenv import (  # type: ignore[import-untyped]
    clickhouse_ip,
    clickhouse_password,
    clickhouse_port,
    clickhouse_user,
)

from QUANTAXIS.QAFetch.QAClickhouse import QACKClient, stock_format
from QUANTAXIS.QARSBridge import QARSAccount, has_qars_support

MA_PERIOD: int = 20
TRADE_SHARES: int = 1000
DEFAULT_INIT_CASH: float = 1_000_000.0
DEFAULT_CODE: str = "000001"
DEFAULT_START: str = "2025-01-01"
DEFAULT_END: str = "2026-08-07"


class StockAccount(Protocol):
    """回测循环需要的账户接口。"""

    def buy(self, code: str, price: float, date: str, amount: int) -> bool:
        """买入。"""

    def sell(self, code: str, price: float, date: str, amount: int) -> bool:
        """卖出。"""

    def settle(self, date: str | None = None) -> None:
        """日终结算。"""

    def get_positions(self) -> pd.DataFrame:
        """当前持仓。"""

    def get_account_info(self) -> dict[str, object]:
        """账户资金摘要。"""


@dataclass(frozen=True)
class BacktestConfig:
    """回测运行参数。"""

    code: str
    start: str
    end: str
    init_cash: float
    ma_period: int
    shares: int


@dataclass
class TradeCounts:
    """成交计数。"""

    buy: int = 0
    sell: int = 0


def _parse_args() -> BacktestConfig:
    """解析命令行参数。"""

    parser = argparse.ArgumentParser(description="ClickHouse 日线均线回测")
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


def _bars_to_frame(data: object) -> pd.DataFrame:
    """把 QACKClient 返回值整理成按日期排序的日线表。"""

    if hasattr(data, "data"):
        frame = pd.DataFrame(data.data).reset_index()
    else:
        frame = pd.DataFrame(data).reset_index()
    if frame.empty:
        return pd.DataFrame(columns=["date", "close", "open", "high", "low"])
    frame["date"] = pd.to_datetime(frame["date"])
    keep = [col for col in ("date", "open", "high", "low", "close", "volume") if col in frame.columns]
    return frame.loc[:, keep].drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)


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
        return _bars_to_frame(raw), "前复权 (stock_adj)"
    raw = client.get_stock_day(code, start, end)
    return _bars_to_frame(raw), f"不复权 ({order_book_id})"


def _long_volume(account: StockAccount, code: str) -> int:
    """读取指定股票多头数量。"""

    positions = account.get_positions()
    if positions is None or positions.empty:
        return 0
    if "code" not in positions.columns:
        return 0
    matched = positions[positions["code"].astype(str) == code]
    if matched.empty:
        return 0
    volume_col = "volume_long" if "volume_long" in matched.columns else None
    if volume_col is None:
        return 0
    return int(matched.iloc[0][volume_col])


def run_ma_backtest(
    bars: pd.DataFrame,
    account: StockAccount,
    config: BacktestConfig,
) -> TradeCounts:
    """按日遍历 K 线，执行均线金叉死叉。

    Args:
        bars: 含 date/close 的日线。
        account: QARS 账户。
        config: 回测参数。

    Returns:
        买卖次数。
    """

    closes: pd.Series = bars["close"].astype(float)
    ma: pd.Series = closes.rolling(config.ma_period).mean()
    counts = TradeCounts()
    holding: bool = False

    for index in range(config.ma_period, len(bars)):
        row = bars.iloc[index]
        trade_date: str = pd.Timestamp(row["date"]).strftime("%Y-%m-%d")
        price: float = float(row["close"])
        current_ma: float = float(ma.iloc[index])
        prev_close: float = float(closes.iloc[index - 1])
        prev_ma: float = float(ma.iloc[index - 1])
        if pd.isna(current_ma) or pd.isna(prev_ma):
            account.settle(trade_date)
            continue

        cross_up: bool = prev_close <= prev_ma and price > current_ma
        cross_down: bool = prev_close >= prev_ma and price < current_ma

        if cross_up and not holding:
            if account.buy(config.code, price, trade_date, config.shares):
                counts.buy += 1
                holding = True
                print(
                    f"{trade_date} BUY  {config.code} "
                    f"price={price:.2f} ma={current_ma:.2f} shares={config.shares}"
                )
            else:
                print(f"{trade_date} BUY 失败 price={price:.2f}")
        elif cross_down and holding:
            volume: int = _long_volume(account, config.code)
            sell_shares: int = volume if volume > 0 else config.shares
            if account.sell(config.code, price, trade_date, sell_shares):
                counts.sell += 1
                holding = False
                print(
                    f"{trade_date} SELL {config.code} "
                    f"price={price:.2f} ma={current_ma:.2f} shares={sell_shares}"
                )
            else:
                print(f"{trade_date} SELL 失败 price={price:.2f}")

        account.settle(trade_date)
        holding = _long_volume(account, config.code) > 0

    return counts


def _as_float(value: object, default: float) -> float:
    """把账户字段转成 float，无法转换时用 default。"""

    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return default
    return default


def _print_report(
    config: BacktestConfig,
    bars: pd.DataFrame,
    adj_note: str,
    counts: TradeCounts,
    account: StockAccount,
) -> None:
    """打印回测摘要。"""

    info = account.get_account_info()
    balance = _as_float(info.get("balance"), config.init_cash)
    available = _as_float(info.get("available"), 0.0)
    ret = (balance / config.init_cash - 1.0) * 100.0
    print("==== 回测摘要 ====")
    print(f"代码={config.code} 区间={config.start}..{config.end} 行情={adj_note}")
    print(f"K 线根数={len(bars)} MA={config.ma_period}")
    print(f"初始资金={config.init_cash:.2f}")
    print(f"期末权益={balance:.2f} 可用={available:.2f} 收益率={ret:.2f}%")
    print(f"买入次数={counts.buy} 卖出次数={counts.sell}")
    positions = account.get_positions()
    if positions is None or positions.empty:
        print("持仓=空")
    else:
        cols = [c for c in ("code", "volume_long", "open_price_long") if c in positions.columns]
        print("持仓:")
        print(positions[cols] if cols else positions)
    if counts.buy == 0 and counts.sell == 0:
        print("区间内无信号")


def main() -> int:
    """入口。"""

    if not has_qars_support():
        print(
            "本示例需要 QARSAccount.buy/sell（qars3）。"
            "请执行: pip install quantaxis[rust]",
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
        f"已加载 {len(bars)} 根日线 {config.code} "
        f"{bars['date'].iloc[0].date()}..{bars['date'].iloc[-1].date()} ({adj_note})"
    )
    account: QARSAccount = QARSAccount(
        account_cookie=f"ch_day_{config.code}",
        portfolio="clickhouse_day",
        init_cash=config.init_cash,
        environment="backtest",
    )
    counts = run_ma_backtest(bars, account, config)
    _print_report(config, bars, adj_note, counts, account)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
