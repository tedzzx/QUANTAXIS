# ruff: noqa: N999
"""EverQuotes (EQ) 历史行情数据源。

通过 ``eqapi.HqApplication`` 拉取 A 股日线 / 5 分钟 K 线，输出与
``QAThs_http`` 兼容的 DataFrame，便于复用 ``ClickHouseStorage``。

凭证优先级：
1. 函数入参 / 环境变量 ``EQ_IP`` ``EQ_PORT`` ``EQ_USER`` ``EQ_PASSWD``
2. ``config/eq.ini``（可用 ``EQ_CONFIG_FILE`` 覆盖路径）
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
from configparser import ConfigParser
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Optional, Sequence

import pandas as pd

from QUANTAXIS.QAFetch.QAClickhouse import PRICE_SCALE, stock_format
from QUANTAXIS.QAUtil import QA_util_date_stamp, QA_util_time_stamp

# ClickHouse 标准表列（快速入库路径）
STOCK_DAY_CH_COLUMNS = (
    "date",
    "order_book_id",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "total_turnover",
)
STOCK_5MIN_CH_COLUMNS = (
    "datetime",
    "order_book_id",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "total_turnover",
)

logger = logging.getLogger(__name__)

# EQ K 线 JSON 数字键（quotesdbtype.h / eq_download_data.reformat_data）
_KLINE_KEY_MAP = {
    "0": "ticker",
    "3": "date",
    "4": "time",
    "5": "quote_type",
    "20": "eq_trading_phase_code",
    "101": "open",
    "102": "high",
    "103": "low",
    "104": "last",
    "105": "close",
    "112": "num_trades",
    "113": "volume",
    "114": "amount",
}

_PRICE_COLS = ("open", "high", "low", "last", "close")
_PRICE_SCALE = float(PRICE_SCALE)

_FREQ_QUOTE_TYPE = {
    "day": "kl1D",
    "1min": "kl1m",
    "5min": "kl5m",
    "15min": "kl15m",
    "30min": "kl30m",
    "60min": "kl1h",
}


class EqError(RuntimeError):
    """EQ 连接或取数失败。"""


class EqConfigError(ValueError):
    """EQ 凭证或配置缺失。"""


@dataclass(frozen=True)
class EqServerSetting:
    ip: str
    port: str
    user: str
    passwd: str


@dataclass(frozen=True)
class EqHttpConfig:
    """历史客户端配置（命名沿用 ths 侧习惯，实际为 Ice/TCP）。"""

    primary: EqServerSetting
    secondary: EqServerSetting | None = None
    conf_path: str = ""
    client_type: int = 1


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _default_ini_path() -> Path:
    env = os.environ.get("EQ_CONFIG_FILE", "").strip()
    if env:
        return Path(env)
    return _project_root() / "config" / "eq.ini"


def _default_hq_client_conf() -> Path:
    env = os.environ.get("EQ_HQ_CLIENT_CONF", "").strip()
    if env:
        return Path(env)
    bundled = _project_root() / "config" / "eq_config.hqClient"
    if bundled.exists():
        return bundled
    # 回退到 EQAPI 包内官方样例
    return (
        Path.home()
        / "MyProject"
        / "EQAPI"
        / "eqapi_v1.3.6_20220711"
        / "api"
        / "python"
        / "config"
        / "config.hqClient"
    )


def _setting_from_section(section) -> EqServerSetting | None:
    ip = str(section.get("ip", "") or "").strip()
    port = str(section.get("port", "") or "").strip()
    user = str(section.get("user", "") or "").strip()
    passwd = str(section.get("passwd", "") or "").strip()
    if not (ip and port and user and passwd):
        return None
    return EqServerSetting(ip=ip, port=port, user=user, passwd=passwd)


def load_eq_config(
    ini_path: str | Path | None = None,
    *,
    ip: str | None = None,
    port: str | None = None,
    user: str | None = None,
    passwd: str | None = None,
) -> EqHttpConfig:
    """加载 EQ 连接配置。"""

    env_ip = (ip or os.environ.get("EQ_IP", "")).strip()
    env_port = (port or os.environ.get("EQ_PORT", "")).strip()
    env_user = (user or os.environ.get("EQ_USER", "")).strip()
    env_passwd = (passwd or os.environ.get("EQ_PASSWD", "")).strip()

    primary: EqServerSetting | None = None
    secondary: EqServerSetting | None = None
    client_type = 1
    conf_path = str(_default_hq_client_conf())

    path = Path(ini_path) if ini_path else _default_ini_path()
    if path.exists():
        parser = ConfigParser()
        parser.read(path, encoding="utf-8")
        if parser.has_section("EQ"):
            sec = parser["EQ"]
            primary = _setting_from_section(sec)
            client_type = int(sec.get("client_type", "1") or "1")
            conf_override = sec.get("hq_client_conf", "").strip()
            if conf_override:
                conf_path = conf_override
        if parser.has_section("EQ_SECONDARY"):
            secondary = _setting_from_section(parser["EQ_SECONDARY"])

    if env_ip and env_port and env_user and env_passwd:
        primary = EqServerSetting(
            ip=env_ip,
            port=env_port,
            user=env_user,
            passwd=env_passwd,
        )

    if primary is None:
        raise EqConfigError(
            f"缺少 EQ 凭证：请配置 {path} 或环境变量 "
            "EQ_IP/EQ_PORT/EQ_USER/EQ_PASSWD"
        )

    # 官方 demo 使用双服务器；未配置第二路时复用第一路
    if secondary is None:
        secondary = primary

    env_conf = os.environ.get("EQ_HQ_CLIENT_CONF", "").strip()
    if env_conf:
        conf_path = env_conf

    return EqHttpConfig(
        primary=primary,
        secondary=secondary,
        conf_path=conf_path,
        client_type=client_type,
    )


def eq_market_for_code(code: str) -> str:
    """六位代码 → EQ K 线市场 ID（shkl / szkl / bjkl）。"""

    order_book_id = stock_format(code)
    if order_book_id.endswith(".XSHG"):
        return "shkl"
    if order_book_id.endswith(".XBEI"):
        return "bjkl"
    return "szkl"


def _plain_code(code: str) -> str:
    return str(code).strip().split(".")[0]


def _to_yyyymmdd(value: object) -> int:
    stamp = pd.Timestamp(value)
    return int(stamp.strftime("%Y%m%d"))


def _hhmmssmmm_from_time(raw: object) -> str:
    text = str(int(raw) if str(raw).isdigit() else raw).zfill(9)
    return text


def _as_int(value: object, default: int = 0) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        try:
            return int(float(value))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return default


def _as_price(value: object) -> float:
    """EQ 价格（毫）转为元 float，供 DataFrame 对外接口。"""

    return round(_as_int(value) / _PRICE_SCALE, 2)


def _as_price_int(value: object) -> int:
    """EQ 价格保持毫（×10000）整数，供 ClickHouse Int32 写入。"""

    return _as_int(value)


def _eq_date(date_v: object) -> dt.date:
    d = _as_int(date_v)
    return dt.date(d // 10000, (d // 100) % 100, d % 100)


def _eq_datetime(date_v: object, time_v: object) -> dt.datetime | None:
    """解析 EQ date/time；非法时间（如 hour>=24 的脏数据）返回 None。"""

    d = _as_int(date_v)
    t = _as_int(time_v)
    year, month, day = d // 10000, (d // 100) % 100, d % 100
    # EQ time: HHMMSSmmm（9 位整数，如 93000000）
    hh = t // 10000000
    mm = (t // 100000) % 100
    ss = (t // 1000) % 100
    if not (
        1 <= month <= 12
        and 1 <= day <= 31
        and 0 <= hh <= 23
        and 0 <= mm <= 59
        and 0 <= ss <= 59
    ):
        return None
    try:
        return dt.datetime(year, month, day, hh, mm, ss)
    except ValueError:
        return None


def _iter_quote_dicts(quotes: list | None):
    """将 getSync 返回值迭代为 dict。"""

    if not quotes:
        return
    for item in quotes:
        if item is None or item == "":
            continue
        if isinstance(item, (bytes, bytearray)):
            try:
                item = item.decode("utf-8")
            except UnicodeDecodeError:
                item = item.decode("gbk", errors="ignore")
        if isinstance(item, str):
            text = item.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                logger.warning("跳过无法解析的 EQ quote: %s", text[:80])
                continue
        elif isinstance(item, dict):
            payload = item
        else:
            continue
        if isinstance(payload, dict):
            yield payload


def quotes_to_ch_rows(
    quotes: list | None,
    *,
    data_type: str,
    default_code: str = "",
    order_book_id: str = "",
) -> list[tuple]:
    """将 getSync JSON 转为 ClickHouse 行元组（无 DataFrame）。

    day 行: (date, order_book_id, open, high, low, close, volume, total_turnover)
    min 行: (datetime, order_book_id, open, high, low, close, volume, total_turnover)

    ``order_book_id`` 非空时整批行都写这个合约代码，供指数保留 .XSHG/.XSHE/.INDX。
    """

    rows: list[tuple] = []
    default_plain = _plain_code(default_code) if default_code else ""
    seen: set[tuple] = set()
    is_day = data_type == "day"
    skipped_bad_time = 0

    for payload in _iter_quote_dicts(quotes):
        ticker = str(payload.get("0", default_plain) or default_plain)
        plain = _plain_code(ticker) or default_plain
        if not plain:
            continue
        date_v = payload.get("3")
        if date_v is None:
            continue
        # CH 定点：价格 Int×10000；volume/成交额为元或股的整数（成交额不 ×10000）
        open_ = _as_price_int(payload.get("101", 0))
        high = _as_price_int(payload.get("102", 0))
        low = _as_price_int(payload.get("103", 0))
        last = payload.get("104")
        close_v = payload.get("105", last)
        close = _as_price_int(close_v if close_v is not None else 0)
        volume = _as_int(payload.get("113", 0))
        amount = _as_int(payload.get("114", 0))
        book_id = order_book_id or stock_format(plain)

        if is_day:
            try:
                moment = _eq_date(date_v)
            except ValueError:
                skipped_bad_time += 1
                continue
            key = (moment, book_id)
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                (moment, book_id, open_, high, low, close, volume, amount)
            )
        else:
            time_v = payload.get("4", 0)
            bar_time = _eq_datetime(date_v, time_v)
            if bar_time is None:
                skipped_bad_time += 1
                continue
            key = (bar_time, book_id)
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                (bar_time, book_id, open_, high, low, close, volume, amount)
            )

    if skipped_bad_time:
        logger.warning(
            "EQ quotes 跳过 %s 条非法时间戳 (data_type=%s code=%s)",
            skipped_bad_time,
            data_type,
            default_plain or "?",
        )
    rows.sort(key=lambda item: (item[1], item[0]))
    return rows


def ch_rows_to_frame(rows: list[tuple], data_type: str) -> pd.DataFrame:
    """将 CH 行元组转为兼容旧接口的轻量 DataFrame（仅在需要时使用）。

    CH 行价格为 ×10000 整数；对外 DataFrame 还原为元。
    """

    if not rows:
        return pd.DataFrame()
    if data_type == "day":
        frame = pd.DataFrame(rows, columns=list(STOCK_DAY_CH_COLUMNS))
        frame = frame.rename(columns={"order_book_id": "code", "total_turnover": "amount"})
        frame["code"] = frame["code"].astype(str).str.split(".").str[0]
        frame["datetime"] = pd.to_datetime(frame["date"])
        frame["date"] = frame["datetime"].dt.strftime("%Y-%m-%d")
    else:
        frame = pd.DataFrame(rows, columns=list(STOCK_5MIN_CH_COLUMNS))
        frame = frame.rename(columns={"order_book_id": "code", "total_turnover": "amount"})
        frame["code"] = frame["code"].astype(str).str.split(".").str[0]
        frame["datetime"] = pd.to_datetime(frame["datetime"])
        frame["date"] = frame["datetime"].dt.strftime("%Y-%m-%d")
    for col in ("open", "high", "low", "close"):
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce") / _PRICE_SCALE
    frame["date_stamp"] = frame["date"].map(QA_util_date_stamp)
    frame["time_stamp"] = frame["datetime"].map(QA_util_time_stamp)
    frame["type"] = data_type if data_type == "day" else data_type
    keep = [
        "code",
        "datetime",
        "date",
        "date_stamp",
        "time_stamp",
        "type",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
    ]
    return frame[keep].set_index("datetime", drop=False)


def quotes_to_frame(quotes: list | None, code: str, data_type: str) -> pd.DataFrame:
    """将 getSync 返回的 JSON 字符串列表转为标准行情 DataFrame。"""

    if not quotes:
        return pd.DataFrame()

    rows: list[dict] = []
    for item in quotes:
        if item is None or item == "":
            continue
        if isinstance(item, (bytes, bytearray)):
            try:
                item = item.decode("utf-8")
            except UnicodeDecodeError:
                item = item.decode("gbk", errors="ignore")
        if isinstance(item, str):
            text = item.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                logger.warning("跳过无法解析的 EQ quote: %s", text[:80])
                continue
        elif isinstance(item, dict):
            payload = item
        else:
            continue
        if not isinstance(payload, dict):
            continue
        rows.append({_KLINE_KEY_MAP.get(str(k), str(k)): v for k, v in payload.items()})

    if not rows:
        return pd.DataFrame()

    frame = pd.DataFrame(rows)
    for col in _PRICE_COLS:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce") / _PRICE_SCALE
    if "close" not in frame.columns and "last" in frame.columns:
        frame["close"] = frame["last"]
    for col in ("volume", "amount", "num_trades"):
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")

    plain = _plain_code(code)
    frame["code"] = plain
    if "date" not in frame.columns:
        raise EqError("EQ 行情缺少 date 字段")

    dates = pd.to_datetime(frame["date"].astype(str), format="%Y%m%d", errors="coerce")
    if "time" in frame.columns and data_type != "day":
        times = frame["time"].map(_hhmmssmmm_from_time)
        # EQ time: HHMMSSmmm（9 位）
        hh = times.str[0:2]
        mm = times.str[2:4]
        ss = times.str[4:6]
        frame["datetime"] = pd.to_datetime(
            dates.dt.strftime("%Y-%m-%d") + " " + hh + ":" + mm + ":" + ss,
            errors="coerce",
        )
    else:
        frame["datetime"] = dates

    frame = frame.dropna(subset=["datetime"])
    frame["date"] = frame["datetime"].dt.strftime("%Y-%m-%d")
    frame["date_stamp"] = frame["date"].map(QA_util_date_stamp)
    frame["time_stamp"] = frame["datetime"].map(QA_util_time_stamp)
    frame["type"] = data_type

    keep = [
        "code",
        "datetime",
        "date",
        "date_stamp",
        "time_stamp",
        "type",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
    ]
    for col in keep:
        if col not in frame.columns:
            frame[col] = pd.NA
    result = frame[keep].sort_values("datetime").drop_duplicates(
        subset=["code", "datetime"],
        keep="last",
    )
    return result.set_index("datetime", drop=False)


class _EqHqApp:
    """薄封装：连接生命周期 + getSync。"""

    def __init__(self, config: EqHttpConfig):
        try:
            import eqapi
        except ImportError as exc:
            raise EqError(
                "未安装 eqapi/zeroc-ice。请先安装 EQ Python 依赖后再使用。"
            ) from exc

        self._eqapi = eqapi
        self._config = config
        self._app = None
        self._errors: list[str] = []

    def _make_settings(self) -> list:
        settings = []
        for server in (self._config.primary, self._config.secondary):
            if server is None:
                continue
            setting = self._eqapi.EqSetting()
            setting.ip = server.ip
            setting.port = str(server.port)
            setting.user = server.user
            setting.passwd = server.passwd
            settings.append(setting)
        return settings

    def start(self) -> None:
        conf = self._config.conf_path
        if conf and not Path(conf).exists():
            raise EqConfigError(f"EQ Ice 配置文件不存在: {conf}")

        owner = self
        eqapi = self._eqapi

        class _App(eqapi.HqApplication):
            def onConnect(self, msg):
                logger.info("EQ connected: %s", msg)

            def onQuote(self, quotes):
                return None

            def onQuoteBin(self, quote):
                return None

            def onRebuild(self, quotes):
                return None

            def onRebuildBin(self, quote):
                return None

            def onLog(self, msg):
                logger.debug("EQ log: %s", msg)

            def onError(self, msg):
                text = str(msg)
                owner._errors.append(text)
                logger.error("EQ error: %s", text)

            def onDisconnect(self, msg):
                logger.warning("EQ disconnect: %s", msg)

        self._errors.clear()
        self._app = _App(self._make_settings(), conf or "")
        self._app.start()
        if self._app.state() != self._eqapi.EqState.EQ_STATE_CONNECT:
            detail = "; ".join(self._errors) if self._errors else "unknown"
            raise EqError(f"EQ 连接失败: {detail}")

    def stop(self) -> None:
        if self._app is not None:
            try:
                self._app.stop()
            except Exception:  # noqa: BLE001
                logger.exception("EQ stop 异常")
            self._app = None

    def get_sync(
        self,
        market: str,
        quote_type: str,
        codes: list[str],
        start_date: int,
        start_time: int,
        end_date: int,
        end_time: int,
        rate: int = 0,
    ) -> list:
        if self._app is None:
            raise EqError("EQ 未连接")
        self._errors.clear()
        quotes = self._app.getSync(
            market,
            quote_type,
            codes,
            start_date,
            start_time,
            end_date,
            end_time,
            rate,
        )
        if quotes is None:
            detail = "; ".join(self._errors) if self._errors else "empty result"
            raise EqError(f"EQ getSync 失败: {detail}")
        return quotes


_CLIENT_LOCK = Lock()
_CLIENT: Optional[_EqHqApp] = None


def get_client(config: EqHttpConfig | None = None, *, reconnect: bool = False) -> _EqHqApp:
    """获取全局 EQ 历史客户端（懒连接）。"""

    global _CLIENT
    with _CLIENT_LOCK:
        if _CLIENT is not None and not reconnect:
            return _CLIENT
        if _CLIENT is not None:
            _CLIENT.stop()
            _CLIENT = None
        cfg = config or load_eq_config()
        app = _EqHqApp(cfg)
        app.start()
        _CLIENT = app
        return _CLIENT


def close_client() -> None:
    global _CLIENT
    with _CLIENT_LOCK:
        if _CLIENT is not None:
            _CLIENT.stop()
            _CLIENT = None


def _range_times(start: object, end: object) -> tuple[int, int, int, int]:
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    start_date = int(start_ts.strftime("%Y%m%d"))
    end_date = int(end_ts.strftime("%Y%m%d"))
    start_time = int(start_ts.strftime("%H%M%S")) * 1000
    end_time = int(end_ts.strftime("%H%M%S")) * 1000
    if end_time == 0 and len(str(end).strip()) <= 10:
        end_time = 235959000
    if start_date == end_date and end_time < start_time:
        end_time = 235959000
    return start_date, start_time, end_date, end_time


def _get_sync_range(
    market: str,
    quote_type: str,
    codes: Sequence[str],
    start: object,
    end: object,
) -> list:
    client = get_client()
    start_date, start_time, end_date, end_time = _range_times(start, end)
    plain_codes = [_plain_code(code) for code in codes]
    return client.get_sync(
        market,
        quote_type,
        plain_codes,
        start_date,
        start_time,
        end_date,
        end_time,
        0,
    )


def fetch_stock_day_rows(
    codes: str | Sequence[str],
    start: str,
    end: str,
    *,
    market: str = "",
    order_book_id: str = "",
) -> list[tuple]:
    """拉取日线并返回 ClickHouse 行元组。

    多代码必须属于同一 K 线市场。指数传入 ``market`` 与 ``order_book_id``，
    避免六位代码被格式化成股票后缀。``order_book_id`` 只允许单代码。

    Args:
        codes: 一只或多只证券代码。
        start: 起始日期。
        end: 结束日期。
        market: EQ 市场 ID。空则按股票规则推断。
        order_book_id: 写入行的合约代码。空则按股票规则格式化。

    Returns:
        日线元组列表。
    """

    code_list = [codes] if isinstance(codes, str) else list(codes)
    if not code_list:
        return []
    if order_book_id and len(code_list) != 1:
        raise EqConfigError("指定 order_book_id 时只能拉一只代码")
    if market:
        market_id = market
    else:
        markets = {eq_market_for_code(code) for code in code_list}
        if len(markets) != 1:
            raise EqConfigError(
                f"fetch_stock_day_rows 要求同市场代码，收到: {sorted(markets)}"
            )
        market_id = next(iter(markets))
    quotes = _get_sync_range(market_id, "kl1D", code_list, start, end)
    return quotes_to_ch_rows(
        quotes,
        data_type="day",
        default_code=code_list[0],
        order_book_id=order_book_id,
    )


def fetch_stock_min_rows(
    code: str,
    start: str,
    end: str,
    frequence: str = "5min",
    *,
    market: str = "",
    order_book_id: str = "",
) -> list[tuple]:
    """区间一次 getSync 拉取分钟线，返回 ClickHouse 行元组。

    Args:
        code: 证券代码。
        start: 起始时间。
        end: 结束时间。
        frequence: ``1min`` 或 ``5min``。
        market: EQ 市场 ID。空则按股票规则推断。
        order_book_id: 写入行的合约代码。空则按股票规则格式化。

    Returns:
        分钟线元组列表。
    """

    quote_type = _FREQ_QUOTE_TYPE.get(frequence)
    if quote_type is None:
        raise EqConfigError(f"不支持的分钟频率: {frequence}")

    plain = _plain_code(code)
    market_id = market or eq_market_for_code(plain)
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)

    # 优先整段拉取；失败则按半年降级（应对单次响应过大）
    spans: list[tuple[pd.Timestamp, pd.Timestamp]] = [(start_ts, end_ts)]
    rows: list[tuple] = []
    while spans:
        c_start, c_end = spans.pop(0)
        try:
            quotes = _get_sync_range(market_id, quote_type, [plain], c_start, c_end)
            part = quotes_to_ch_rows(
                quotes,
                data_type=frequence,
                default_code=plain,
                order_book_id=order_book_id,
            )
            if part:
                rows.extend(part)
        except Exception as exc:  # noqa: BLE001
            days = (c_end.normalize() - c_start.normalize()).days
            if days <= 31:
                raise EqError(
                    f"EQ getSync 失败 {plain} {c_start}..{c_end}: {exc}"
                ) from exc
            # 取整到秒，避免 12:00:00.500000 等半秒中点污染后续区间
            mid = (c_start + (c_end - c_start) / 2).floor("s")
            if mid <= c_start:
                mid = c_start + pd.Timedelta(seconds=1)
            if mid >= c_end:
                raise EqError(
                    f"EQ getSync 失败 {plain} {c_start}..{c_end}: {exc}"
                ) from exc
            spans.insert(0, (mid + pd.Timedelta(seconds=1), c_end))
            spans.insert(0, (c_start, mid))
            logger.warning(
                "EQ 区间过大，拆分重试 %s %s..%s (%s)",
                plain,
                c_start,
                c_end,
                exc,
            )

    if not rows:
        return []
    # 裁剪到请求时间窗
    rows = [
        row
        for row in rows
        if start_ts.to_pydatetime() <= row[0] <= end_ts.to_pydatetime()
    ]
    rows.sort(key=lambda item: item[0])
    return rows


def QA_fetch_get_stock_day(
    code: str,
    start: str,
    end: str,
    if_fq: str = "00",
) -> pd.DataFrame:
    """拉取 A 股日线（不复权；EQ K 线本身无 CPS 参数）。"""

    del if_fq  # EQ 历史 K 线接口无复权开关
    rows = fetch_stock_day_rows(code, start, end)
    return ch_rows_to_frame(rows, "day")


def QA_fetch_get_stock_min(
    code: str,
    start: str,
    end: str,
    frequence: str = "5min",
) -> pd.DataFrame:
    """拉取 A 股分钟线（默认 5min；区间一次拉取）。"""

    rows = fetch_stock_min_rows(code, start, end, frequence=frequence)
    return ch_rows_to_frame(rows, frequence)
