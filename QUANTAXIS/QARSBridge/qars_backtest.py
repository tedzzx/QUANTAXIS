"""QARS 回测引擎桥接：把 ClickHouse/DataFrame 日线交给 qa-rs。

``QARSBacktest`` 包装 ``qars3.Backtest``（Rust ``QABacktest``）。
行情必须是 qa-rs 约定的 parquet（见 ``write_qars_day_parquet``）。
策略必须带 ``.market``，逻辑写在 ``on_calc``；引擎不会调用 ``on_bar``。
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd
from qars3 import Backtest as _RustBacktest
from qars3 import Strategy as _RustStrategy

_PARQUET_COLUMNS: tuple[str, ...] = (
    "date",
    "order_book_id",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "total_turnover",
    "limit_up",
    "limit_down",
    "num_trades",
    "split_coefficient_to",
    "dividend_cash_before_tax",
)


def write_qars_day_parquet(frame: pd.DataFrame, path: str) -> str:
    """把日线表写成 qa-rs ``QAMarketCenter`` 可读的 parquet。

    Args:
        frame: 至少含 ``date``、``order_book_id``、OHLC 的日线。
        path: 输出 ``.pq`` / ``.parquet`` 路径。

    Returns:
        写入后的绝对路径。
    """

    data = frame.copy()
    if "order_book_id" not in data.columns and "code" in data.columns:
        data["order_book_id"] = data["code"].astype(str)
    if "date" not in data.columns:
        raise ValueError("日线缺少 date 列，无法写成 qa-rs parquet")
    if "order_book_id" not in data.columns:
        raise ValueError("日线缺少 order_book_id 列，无法写成 qa-rs parquet")

    data["date"] = pd.to_datetime(data["date"]).dt.strftime("%Y-%m-%d")
    data["order_book_id"] = data["order_book_id"].astype(str)
    if "total_turnover" not in data.columns and "amount" in data.columns:
        data["total_turnover"] = data["amount"]

    for column in (
        "open",
        "high",
        "low",
        "close",
        "volume",
        "total_turnover",
        "limit_up",
        "limit_down",
        "num_trades",
        "split_coefficient_to",
        "dividend_cash_before_tax",
    ):
        if column not in data.columns:
            data[column] = 0.0
        data[column] = pd.to_numeric(data[column], errors="coerce").fillna(0.0)

    close = data["close"].astype(float)
    missing_up = data["limit_up"] <= 0
    missing_down = data["limit_down"] <= 0
    data.loc[missing_up, "limit_up"] = close[missing_up] * 1.2
    data.loc[missing_down, "limit_down"] = close[missing_down] * 0.8

    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    data.loc[:, list(_PARQUET_COLUMNS)].to_parquet(out, index=False)
    return str(out.resolve())


def _as_float(value: object, default: float = 0.0) -> float:
    """把账户字段转成 float。"""

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


def _call_market(func: Callable[..., Any], default: Any, *args: Any) -> Any:
    """调用 qa-rs 市场方法；失败时返回 default。"""

    try:
        return func(*args)
    except (TypeError, ValueError, AttributeError, RuntimeError):
        return default


def _collect_result(strategy: Any) -> dict[str, Any]:
    """从策略 ``market`` 组装回测结果。"""

    account_name = str(getattr(strategy, "account_name", "default"))
    init_cash = _as_float(getattr(strategy, "init_cash", 0.0), 0.0)
    market = getattr(strategy, "market", None)
    result: dict[str, Any] = {
        "total_return": 0.0,
        "balance": init_cash,
        "available": 0.0,
        "close_profit": 0.0,
        "float_profit": 0.0,
        "init_cash": init_cash,
        "account_name": account_name,
        "positions": {},
        "buy": int(getattr(strategy, "buy_count", 0)),
        "sell": int(getattr(strategy, "sell_count", 0)),
    }
    if market is None:
        return result
    result["balance"] = _as_float(
        _call_market(market.get_balance, init_cash, account_name),
        init_cash,
    )
    result["close_profit"] = _as_float(
        _call_market(market.get_closeprofit, 0.0, account_name),
        0.0,
    )
    result["float_profit"] = _as_float(
        _call_market(market.get_floatprofit, 0.0, account_name),
        0.0,
    )
    qifi_raw = _call_market(market.get_account_qifi, "{}", account_name)
    try:
        qifi = json.loads(qifi_raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        qifi = {}
    if isinstance(qifi, dict):
        result["available"] = _as_float(qifi.get("available"), 0.0)
        if "balance" in qifi:
            result["balance"] = _as_float(qifi.get("balance"), result["balance"])
    pos_raw = _call_market(market.get_positions_qifi, "{}", account_name)
    try:
        positions = json.loads(pos_raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        positions = {}
    result["positions"] = positions if isinstance(positions, dict) else {}
    if init_cash > 0:
        result["total_return"] = result["balance"] / init_cash - 1.0
    return result


class QARSBacktest:
    """qa-rs ``QABacktest`` 的 Python 包装。

    Args:
        start: 回测开始日期 ``YYYY-MM-DD``。
        end: 回测结束日期 ``YYYY-MM-DD``。
        data_path: qa-rs parquet 路径（``mkt.pq``）。
    """

    def __init__(
        self,
        start: str,
        end: str,
        data_path: str | None = None,
    ) -> None:
        """初始化回测引擎。"""

        self.start = start
        self.end = end
        self.data_path = data_path or ""
        self._backtest = _RustBacktest(start, end, self.data_path)

    def load_dataframe(self, frame: pd.DataFrame, path: str | None = None) -> str:
        """把日线 DataFrame 写成 parquet，并更新 ``data_path``。

        策略应在调用本方法之后用返回路径构造，以便 ``QAMarket`` 读到文件。

        Args:
            frame: 日线表。
            path: 输出路径；缺省写到临时文件。

        Returns:
            parquet 绝对路径。
        """

        if path is None:
            handle, path = tempfile.mkstemp(prefix="qars_mkt_", suffix=".pq")
            os.close(handle)
        self.data_path = write_qars_day_parquet(frame, path)
        self._backtest = _RustBacktest(self.start, self.end, self.data_path)
        return self.data_path

    def run(self, strategy: Any) -> dict[str, Any]:
        """运行 Rust 回测循环，并从 ``strategy.market`` 收集结果。

        Args:
            strategy: 带 ``.market`` 的 ``QARSStrategy`` / ``qars3.Strategy``。

        Returns:
            含 ``total_return``、``balance``、``positions``、买卖次数的字典。
        """

        self._backtest.run(strategy)
        return _collect_result(strategy)

    def __repr__(self) -> str:
        """字符串表示。"""

        return (
            f"QARSBacktest(start='{self.start}', end='{self.end}', "
            f"data_path='{self.data_path}', backend='Rust')"
        )


class QARSStrategy(_RustStrategy):
    """对齐 qa-rs ``Strategy`` 的回测策略基类。

    构造时创建 ``QAMarket(portfolio_name, data_path)``。引擎按交易日回调
    ``on_init`` / ``on_session_begin`` / ``on_calc`` / ``on_session_end`` /
    ``on_finish``。日线逻辑写在 ``on_calc``。
    """

    def __init__(
        self,
        data_path: str,
        portfolio_name: str = "default",
        account_name: str = "default",
        init_cash: float = 1_000_000.0,
    ) -> None:
        """初始化策略并绑定行情文件。

        Args:
            data_path: qa-rs parquet 路径。
            portfolio_name: ``QAMarket`` 组合名。
            account_name: ``init_market`` 使用的账户名。
            init_cash: 初始资金。
        """

        super().__init__(data_path, portfolio_name)
        self.account_name = account_name
        self.init_cash = init_cash
        self.buy_count = 0
        self.sell_count = 0

    def on_init(self) -> None:
        """回测开始前注册账户。"""

        self.market.init_market({self.account_name: float(self.init_cash)})

    def on_session_begin(self) -> None:
        """每个交易日盘前回调。"""

    def on_calc(self) -> None:
        """当日截面就绪后的核心逻辑。"""

    def on_session_end(self) -> None:
        """每个交易日盘后回调。"""

    def on_finish(self) -> None:
        """全部交易日结束后的回调。"""


def create_backtest(
    start: str,
    end: str,
    data_path: str | None = None,
) -> QARSBacktest:
    """创建回测引擎。

    Args:
        start: 开始日期。
        end: 结束日期。
        data_path: parquet 路径。

    Returns:
        ``QARSBacktest`` 实例。
    """

    return QARSBacktest(start, end, data_path)
