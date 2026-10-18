from __future__ import annotations

import configparser
import datetime
import os
import pprint
from pathlib import Path
from typing import Optional

import clickhouse_driver
import numpy as np
import pandas as pd

import QUANTAXIS as QA

"""
RQData → ClickHouse 落地脚本。

传输通道（RQDATA_TRANSPORT / rqdatac.ini transport）:
- http（默认）: HTTPS://rqdata.ricequant.com ，不依赖 16011
- tcp: 官方 rqdatac 二进制协议（rqdatad-pro.ricequant.com:16011）

鉴权（任选其一）:
1. config/rqdatac.ini 的 license / username+password / uri
2. 环境变量 RQDATAC_USERNAME + RQDATAC_PASSWORD
3. 环境变量 RQDATAC_CONF / RQDATAC2_CONF / RQDATAC_URI（tcp）

ClickHouse 连接优先 qaenv，其次 CLICKHOUSE_* 环境变量。
"""

_CONFIG_DIR = Path(__file__).resolve().parent
_RQDATA_INI = _CONFIG_DIR / "rqdatac.ini"
fund = None  # tcp + rqdatac[fund] 时再赋值


def _clickhouse_client() -> clickhouse_driver.Client:
    """按环境变量 / qaenv 创建 ClickHouse 客户端。

    优先 ``CLICKHOUSE_*`` 环境变量（便于覆盖 qaenv 中错误的 9001 端口），
    其次 qaenv，最后本地默认 127.0.0.1:9000。
    """

    host = os.getenv("CLICKHOUSE_IP")
    port_raw = os.getenv("CLICKHOUSE_PORT")
    user = os.getenv("CLICKHOUSE_USER")
    password = os.getenv("CLICKHOUSE_PASSWORD")

    if not host or not port_raw or user is None:
        try:
            from qaenv import (
                clickhouse_ip,
                clickhouse_password,
                clickhouse_port,
                clickhouse_user,
            )

            host = host or clickhouse_ip
            port_raw = port_raw or str(clickhouse_port)
            user = clickhouse_user if user is None else user
            if password is None:
                password = clickhouse_password or ""
        except Exception:
            host = host or "127.0.0.1"
            port_raw = port_raw or "9000"
            user = "default" if user is None else user
            password = "" if password is None else password

    return clickhouse_driver.Client(
        host=host or "127.0.0.1",
        port=int(port_raw or "9000"),
        database="quantaxis",
        user=user or "default",
        password=password or "",
        settings={"insert_block_size": 100000000},
        compression=True,
    )


# 股票 / ETF / 指数 K 线。价格 Int32 = 元 × 10000，成交量与成交额 Int64（股 / 元）。
_BAR_TABLE_KIND = {
    "stock_cn_day": "day",
    "etf_cn_day": "day",
    "index_cn_day": "day",
    "stock_cn_1min": "min",
    "stock_cn_5min": "min",
    "etf_cn_1min": "min",
    "etf_cn_5min": "min",
    "index_cn_1min": "min",
    "index_cn_5min": "min",
}
_CODELIST_TABLES = frozenset({"stock_cn_codelist", "etf_cn_codelist"})
_DAY_INSERT_COLUMNS = frozenset(
    {
        "date",
        "order_book_id",
        "num_trades",
        "limit_up",
        "limit_down",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "total_turnover",
    }
)
_MIN_INSERT_COLUMNS = frozenset(
    {
        "datetime",
        "order_book_id",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "total_turnover",
    }
)
_BAR_CLIENT: Optional[clickhouse_driver.Client] = None

_DAY_BAR_DDL = """
CREATE TABLE IF NOT EXISTS quantaxis.{table} (
    date Date,
    order_book_id LowCardinality(String),
    num_trades Int64 CODEC(ZSTD(3)),
    limit_up Int32 CODEC(ZSTD(3)),
    limit_down Int32 CODEC(ZSTD(3)),
    open Int32 CODEC(ZSTD(3)),
    high Int32 CODEC(ZSTD(3)),
    low Int32 CODEC(ZSTD(3)),
    close Int32 CODEC(ZSTD(3)),
    volume Int64 CODEC(ZSTD(3)),
    total_turnover Int64 CODEC(ZSTD(3))
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, order_book_id)
SETTINGS index_granularity = 8192
"""

_MIN_BAR_DDL = """
CREATE TABLE IF NOT EXISTS quantaxis.{table} (
    datetime DateTime CODEC(DoubleDelta, ZSTD(3)),
    order_book_id LowCardinality(String),
    open Int32 CODEC(ZSTD(3)),
    high Int32 CODEC(ZSTD(3)),
    low Int32 CODEC(ZSTD(3)),
    close Int32 CODEC(ZSTD(3)),
    volume Int64 CODEC(ZSTD(3)),
    total_turnover Int64 CODEC(ZSTD(3))
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(datetime)
ORDER BY (order_book_id, datetime)
SETTINGS index_granularity = 8192
"""


def bar_client() -> clickhouse_driver.Client:
    """复用一条 ClickHouse 连接，供建表、增量水位和插入使用。"""

    global _BAR_CLIENT
    if _BAR_CLIENT is None:
        _BAR_CLIENT = _clickhouse_client()
    return _BAR_CLIENT


def bar_table_ddl(table: str) -> str:
    """返回股票、ETF 或指数 K 线表的建表语句。"""

    kind = _BAR_TABLE_KIND.get(table)
    if kind == "day":
        return _DAY_BAR_DDL.format(table=table)
    if kind == "min":
        return _MIN_BAR_DDL.format(table=table)
    raise ValueError(f"不支持的 K 线表: {table}")


def _bar_table_exists(table: str) -> bool:
    rows = bar_client().execute(
        "SELECT count() FROM system.tables "
        "WHERE database = 'quantaxis' AND name = %(table)s",
        {"table": table},
    )
    return bool(rows and rows[0][0])


def _open_column_type(table: str) -> str:
    rows = bar_client().execute(f"DESCRIBE TABLE quantaxis.{table}")
    for row in rows:
        if row and row[0] == "open":
            return str(row[1])
    return ""


def ensure_bar_table(table: str) -> None:
    """创建股票、ETF 或指数 K 线表。

    价格列已是整数的表保持不动。空的浮点价格表按定点语句重建。
    表里已有浮点行情时拒绝删除。
    """

    ddl = bar_table_ddl(table)
    if not _bar_table_exists(table):
        bar_client().execute(ddl)
        return
    open_type = _open_column_type(table)
    if open_type.startswith("Int"):
        return
    count = bar_client().execute(f"SELECT count() FROM quantaxis.{table}")[0][0]
    if count:
        raise RuntimeError(
            f"quantaxis.{table} 价格列为 {open_type} 且已有 {count} 行，拒绝重建"
        )
    bar_client().execute(f"DROP TABLE quantaxis.{table}")
    bar_client().execute(ddl)


def codes_from_list(table: str) -> list[str]:
    """从股票或 ETF 码表读取六位代码。"""

    if table not in _CODELIST_TABLES:
        raise ValueError(f"不支持的码表: {table}")
    rows = bar_client().execute(
        f"SELECT DISTINCT order_book_id FROM quantaxis.{table} "
        "WHERE order_book_id != '' ORDER BY order_book_id"
    )
    codes = sorted({str(row[0]).split(".")[0] for row in rows if row and row[0]})
    if not codes:
        raise RuntimeError(f"quantaxis.{table} 为空")
    return codes


def latest_bar(table: str, code: str, time_field: str) -> Optional[pd.Timestamp]:
    """查询一只证券在 K 线表中的最新时间；没有记录时返回 None。"""

    if table not in _BAR_TABLE_KIND:
        raise ValueError(f"不支持的 K 线表: {table}")
    if time_field not in {"date", "datetime"}:
        raise ValueError(f"不支持的时间字段: {time_field}")
    from QUANTAXIS.QAFetch.QAClickhouse import stock_format

    normalized = str(code).strip().upper()
    # stock_format 不保留 .INDX，中证指数必须原样查询。
    if normalized.endswith(".INDX"):
        order_book_id = normalized
    else:
        order_book_id = stock_format(code)
    rows = bar_client().execute(
        f"SELECT max({time_field}) FROM quantaxis.{table} "
        "WHERE order_book_id = %(order_book_id)s",
        {"order_book_id": order_book_id},
    )
    if not rows or rows[0][0] is None:
        return None
    stamp = pd.Timestamp(rows[0][0])
    if stamp.year < 1990:
        return None
    return stamp


def insert_bar_rows(
    table: str,
    rows: list[tuple],
    columns: list[str] | tuple[str, ...],
    *,
    batch_size: int = 20000,
) -> int:
    """把已经是 ×10000 整数的 K 线元组写入标准表。"""

    if table not in _BAR_TABLE_KIND:
        raise ValueError(f"不支持的 K 线表: {table}")
    if not rows:
        return 0
    allowed = (
        _DAY_INSERT_COLUMNS if _BAR_TABLE_KIND[table] == "day" else _MIN_INSERT_COLUMNS
    )
    column_list = [str(column) for column in columns]
    unknown = [column for column in column_list if column not in allowed]
    if unknown:
        raise ValueError(f"{table} 不接受列: {unknown}")
    sql = f"INSERT INTO quantaxis.{table} ({', '.join(column_list)}) VALUES"
    size = max(1, int(batch_size))
    client = bar_client()
    # 日线一次可能跨几十年。按月拆开，避免单次 INSERT 超过分区上限。
    buckets: dict[int, list[tuple]] = {}
    for row in rows:
        stamp = pd.Timestamp(row[0])
        buckets.setdefault(stamp.year * 100 + stamp.month, []).append(row)
    for key in sorted(buckets):
        part = buckets[key]
        for offset in range(0, len(part), size):
            client.execute(sql, part[offset : offset + size])
    return len(rows)


def _clean_secret(value: str) -> str:
    """去掉首尾空白与成对引号，避免 ini 里 license=\"...\" 带入引号。"""

    text = (value or "").strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        text = text[1:-1].strip()
    return text


def _load_rqdatac_ini() -> dict[str, str]:
    """读取 config/rqdatac.ini（若不存在返回空 dict）。"""

    if not _RQDATA_INI.is_file():
        return {}
    parser = configparser.ConfigParser()
    parser.read(_RQDATA_INI, encoding="utf-8")
    section = "RQDATA" if parser.has_section("RQDATA") else (
        parser.sections()[0] if parser.sections() else None
    )
    if section is None:
        return {}
    return {key: _clean_secret(value) for key, value in parser.items(section)}


def get_rqdata_transport() -> str:
    """返回 http 或 tcp。默认 http，避开 16011 防火墙问题。"""

    conf = _load_rqdatac_ini()
    value = (
        os.getenv("RQDATA_TRANSPORT", "")
        or conf.get("transport", "")
        or "http"
    ).strip().lower()
    if value not in {"http", "tcp"}:
        raise ValueError(f"不支持的 RQDATA_TRANSPORT={value!r}，可选 http/tcp")
    return value


def _bind_rqdata_http_apis() -> None:
    global get_price, all_instruments, get_ex_factor, get_trading_dates, fund, etf
    global index_weights, index_components
    global get_industry_mapping, get_industry, get_instrument_industry
    from QUANTAXIS.QAFetch import QARqdata_http as http_api

    get_price = http_api.get_price
    all_instruments = http_api.all_instruments
    get_ex_factor = http_api.get_ex_factor
    get_trading_dates = http_api.get_trading_dates
    fund = http_api.fund
    etf = http_api.etf
    index_weights = http_api.index_weights
    index_components = http_api.index_components
    get_industry_mapping = http_api.get_industry_mapping
    get_industry = http_api.get_industry
    get_instrument_industry = http_api.get_instrument_industry


def _bind_rqdata_tcp_apis() -> None:
    global get_price, all_instruments, get_ex_factor, get_trading_dates, fund, etf
    global index_weights, index_components
    global get_industry_mapping, get_industry, get_instrument_industry
    import rqdatac
    from rqdatac import (
        all_instruments as tcp_all_instruments,
        get_ex_factor as tcp_get_ex_factor,
        get_price as tcp_get_price,
    )

    get_price = tcp_get_price
    all_instruments = tcp_all_instruments
    get_ex_factor = tcp_get_ex_factor
    get_trading_dates = getattr(rqdatac, "get_trading_dates", None)
    index_weights = getattr(rqdatac, "index_weights", None)
    index_components = getattr(rqdatac, "index_components", None)
    get_industry_mapping = getattr(rqdatac, "get_industry_mapping", None)
    get_industry = getattr(rqdatac, "get_industry", None)
    get_instrument_industry = getattr(rqdatac, "get_instrument_industry", None)
    try:
        from rqdatac import fund as tcp_fund
    except ImportError:  # pragma: no cover
        tcp_fund = None
    fund = tcp_fund
    try:
        from rqdatac import etf as tcp_etf
    except ImportError:  # pragma: no cover
        tcp_etf = None
    etf = tcp_etf
    del rqdatac


# 占位，init_rqdatac / 首次绑定时替换
get_price = None  # type: ignore[assignment]
all_instruments = None  # type: ignore[assignment]
get_ex_factor = None  # type: ignore[assignment]
get_trading_dates = None  # type: ignore[assignment]
index_weights = None  # type: ignore[assignment]
index_components = None  # type: ignore[assignment]
get_industry_mapping = None  # type: ignore[assignment]
get_industry = None  # type: ignore[assignment]
get_instrument_industry = None  # type: ignore[assignment]
etf = None  # type: ignore[assignment]


def init_rqdatac(force: bool = False) -> str:
    """初始化 RQData（默认 HTTP）。返回实际使用的 transport。"""

    transport = get_rqdata_transport()
    if transport == "http":
        from QUANTAXIS.QAFetch.QARqdata_http import init_rqdatac_http

        _bind_rqdata_http_apis()
        init_rqdatac_http(force=force)
        return transport

    # ---- tcp / rqdatac 二进制协议 ----
    import rqdatac

    _bind_rqdata_tcp_apis()
    if rqdatac.initialized() and not force:
        return transport

    conf = _load_rqdatac_ini()
    uri = _clean_secret(
        os.getenv("RQDATAC_URI", "")
        or os.getenv("RQDATAC2_CONF", "")
        or os.getenv("RQDATAC_CONF", "")
        or conf.get("uri", "")
    )
    username = _clean_secret(
        os.getenv("RQDATAC_USERNAME", "") or conf.get("username", "")
    )
    password = _clean_secret(
        os.getenv("RQDATAC_PASSWORD", "")
        or conf.get("password", "")
        or conf.get("token", "")
        or conf.get("license", "")
    )
    if not username and (conf.get("license") or conf.get("token")):
        username = "license"

    init_kwargs = {
        "connect_timeout": int(os.getenv("RQDATAC_CONNECT_TIMEOUT", "30")),
        "timeout": int(os.getenv("RQDATAC_TIMEOUT", "120")),
        "lazy": False,
    }

    try:
        if uri:
            rqdatac.init(uri=uri, **init_kwargs)
        elif username and password:
            rqdatac.init(username, password, **init_kwargs)
        else:
            rqdatac.init(**init_kwargs)
    except ValueError as exc:
        raise RuntimeError(
            "RQData 未配置凭证。请设置环境变量 RQDATAC_CONF，"
            "或复制 config/rqdatac.example.ini 为 config/rqdatac.ini "
            "并填写 uri / username+password / license。"
        ) from exc
    except TimeoutError as exc:
        raise RuntimeError(
            "无法连接 RQData TCP 服务 rqdatad-pro.ricequant.com:16011。"
            "防火墙若不能放行，请改用 HTTP："
            "在 rqdatac.ini 设置 transport = http"
            "（或 export RQDATA_TRANSPORT=http）。"
        ) from exc

    if not rqdatac.initialized():
        raise RuntimeError("rqdatac.init() 后仍未初始化成功，请检查凭证")
    return transport


# 常用宽基/风格指数（与 index_cn_codelist 对齐）
INDEX_FOCUS_CODES = [
    "000001.XSHG",  # 上证指数
    "000300.XSHG",  # 沪深300
    "000016.XSHG",  # 上证50
    "000688.XSHG",  # 科创50
    "399673.XSHE",  # 创业板50
    "000905.XSHG",  # 中证500
    "000852.XSHG",  # 中证1000
    "932000.INDX",  # 中证2000
]


class datamodelx:

    def __init__(
        self,
        market,
        frequence,
        *,
        init_rq: bool = True,
        codes: Optional[list[str]] = None,
    ):
        """
        current support 

        ::::
        stock
        future
        index
        ::::

        ::::
        1min
        1day




        market:


        合约类型	说明
        CS	Common Stock, 即股票
        ETF	Exchange Traded Fund, 即交易所交易基金
        LOF	Listed Open-Ended Fund，即上市型开放式基金 （以下分级基金已并入）
        INDX	Index, 即指数
        Future	Futures，即期货，包含股指、国债和商品期货
        Spot	Spot，即现货，目前包括上海黄金交易所现货合约
        Option	期权，包括目前国内已上市的全部期权合约
        Convertible	沪深两市场内有交易的可转债合约
        Repo	沪深两市交易所交易的回购合约


        1m - 分钟线
        1d - 日线
        1w - 周线，只支持'1w'
        tick

        codes:
            非空时跳过全市场 all_instruments，仅使用给定 order_book_id 列表。
        """

        if init_rq:
            init_rqdatac()

        self.market_dict = {QA.MARKET_TYPE.STOCK_CN: 'CS', QA.MARKET_TYPE.FUTURE_CN: 'Future', QA.MARKET_TYPE.INDEX_CN: 'INDX',

                            QA.MARKET_TYPE.OPTION_CN: 'Option', 'etf_cn': 'ETF', 'lof_cn': 'LOF'}
        self.freq_dict = {QA.FREQUENCE.DAY: '1d', QA.FREQUENCE.ONE_MIN: '1m',
                          QA.FREQUENCE.FIVE_MIN: '5m', QA.FREQUENCE.FIFTEEN_MIN: '15m',
                          QA.FREQUENCE.TICK: 'tick'}

        self.market = market
        self.frequence = frequence

        self.table_name = self.market + '_' + self.frequence

        self.client = _clickhouse_client()

        if codes is not None:
            self.codelist = pd.DataFrame(
                {
                    "order_book_id": [str(c) for c in codes],
                    "listed_date": "2000-01-01",
                }
            )
        else:
            self.codelist = self.get_list().sort_values('listed_date', ascending=False)
        self.temp_data = []

    @property
    def today(self):
        return str(datetime.date.today())

    def __repr__(self):
        return self.market


    def create_table(self):

        #self.client.execute('DROP TABLE IF EXISTS {}'.format(self.table_name))
        if 'day' in self.frequence:

            if 'stock' in self.market:
                print('s')
                """
                    ['order_book_id', 'date', 'num_trades', 'close', 'limit_up', 'low',
               'open', 'high', 'limit_down', 'volume', 'total_turnover']
                """

                # 价格 Int32×10000（毫）；volume/成交额 Int64（股/元）
                query = bar_table_ddl(self.table_name)
            elif 'index' in self.market:
                # 与股票日线相同的定点列。价格 Int32 = 元 × 10000。
                query = bar_table_ddl(self.table_name)
            elif 'future' in self.market:

                """                
                ['order_book_id', 'date', 'limit_down', 'low', 'limit_up',
                'total_turnover', 'high', 'open_interest', 'prev_settlement', 'open',
                'volume', 'settlement', 'close']
                """
                query = "CREATE TABLE IF NOT EXISTS \
                `quantaxis`.`{}` (\
                    date Date DEFAULT '1970-01-01',\
                        order_book_id String,\
                        limit_up  Float64,\
                        limit_down  Float64,\
                        open_interest Float64,\
                        prev_settlement Float64,\
                        settlement Float64,\
                        open Float64,\
                        high Float64,\
                        low Float64,\
                        close Float64,\
                        volume Float64,\
                        total_turnover Float64\
                    )\
                    ENGINE = ReplacingMergeTree() \
                    PARTITION BY (toYYYYMM(`date`)) \
                    ORDER BY (date, order_book_id)\
                    SETTINGS index_granularity=8192".format(self.table_name)

            elif 'lof' in self.market:
                """
                ['order_book_id', 'date', 'limit_up', 'high', 'iopv', 'open', 'low', 'num_trades', 'close',
                'volume', 'total_turnover', 'limit_down'],
                """
                query = "CREATE TABLE IF NOT EXISTS \
                `quantaxis`.`{}` (\
                    date Date DEFAULT '1970-01-01',\
                        order_book_id String,\
                        limit_up  Float64,\
                        limit_down  Float64,\
                        iopv Float64,\
                        num_trades Float64,\
                        open Float64,\
                        high Float64,\
                        low Float64,\
                        close Float64,\
                        volume Float64,\
                        total_turnover Float64\
                    )\
                    ENGINE = ReplacingMergeTree() \
                    PARTITION BY (toYYYYMM(`date`)) \
                    ORDER BY (date, order_book_id)\
                    SETTINGS index_granularity=8192".format(self.table_name)
            elif 'etf' in self.market:
                """
                与股票日线相同的定点列。EQ K 线没有 iopv。
                """
                query = bar_table_ddl(self.table_name)
            elif 'option' in self.market:
                """
                ['order_book_id', 'date', 'limit_up', 'contract_multiplier',
                    'strike_price', 'open_interest', 'high', 'open', 'low', 'close',
                    'volume', 'settlement', 'prev_settlement', 'total_turnover',
                    'limit_down']
                """
                query = "CREATE TABLE IF NOT EXISTS \
                `quantaxis`.`{}` (\
                    date Date DEFAULT '1970-01-01',\
                        order_book_id String,\
                        limit_up  Float64,\
                        limit_down  Float64,\
                        strike_price Float64,\
                        contract_multiplier Float64,\
                        open_interest Float64,\
                        settlement Float64,\
                        prev_settlement Float64,\
                        open Float64,\
                        high Float64,\
                        low Float64,\
                        close Float64,\
                        volume Float64,\
                        total_turnover Float64\
                    )\
                    ENGINE = ReplacingMergeTree() \
                    PARTITION BY (toYYYYMM(`date`)) \
                    ORDER BY (date, order_book_id)\
                    SETTINGS index_granularity=8192".format(self.table_name)

        elif 'min' in self.frequence:

            if 'stock' in self.market:
                """
                   ['order_book_id', 'datetime', 'low', 'total_turnover', 'high', 'open',
                   'volume', 'close']
                """

                # 价格 Int32×10000；volume/成交额 Int64；按月分区。1min 与 5min 同结构。
                query = bar_table_ddl(self.table_name)
            elif 'index' in self.market:
                # 与股票分钟线相同的定点列，覆盖 index_cn_1min 与 index_cn_5min。
                query = bar_table_ddl(self.table_name)

            elif 'future' in self.market:

                """                
                ['order_book_id', 'datetime', 'low', 'total_turnover', 'high',
                       'open_interest', 'trading_date', 'open', 'volume', 'close']
                """
                query = "CREATE TABLE IF NOT EXISTS \
                `quantaxis`.`{}` (\
                    datetime Datetime DEFAULT '1970-01-01',\
                    trading_date Date DEFAULT '1970-01-01',\
                        order_book_id String,\
                        open_interest Float64,\
                        open Float64,\
                        high Float64,\
                        low Float64,\
                        close Float64,\
                        volume Float64,\
                        total_turnover Float64\
                    )\
                    ENGINE = ReplacingMergeTree() \
                    PARTITION BY (toYYYYMMDD(`datetime`)) \
                    ORDER BY (datetime, order_book_id)\
                    SETTINGS index_granularity=8192".format(self.table_name)
            elif 'option' in self.market:

                """                
                ['order_book_id', 'datetime', 'open_interest', 'high', 'open', 'low',
                'trading_date', 'close', 'volume', 'total_turnover'],
                """
                query = "CREATE TABLE IF NOT EXISTS \
                `quantaxis`.`{}` (\
                    datetime Datetime DEFAULT '1970-01-01',\
                    trading_date Date DEFAULT '1970-01-01',\
                        order_book_id String,\
                        open_interest Float64,\
                        open Float64,\
                        high Float64,\
                        low Float64,\
                        close Float64,\
                        volume Float64,\
                        total_turnover Float64\
                    )\
                    ENGINE = ReplacingMergeTree() \
                    PARTITION BY (toYYYYMMDD(`datetime`)) \
                    ORDER BY (datetime, order_book_id)\
                    SETTINGS index_granularity=8192".format(self.table_name)
            elif 'etf' in self.market:
                """
                与股票分钟线相同的定点列，覆盖 etf_cn_1min 与 etf_cn_5min。
                """
                query = bar_table_ddl(self.table_name)
            elif 'lof' in self.market:

                """                
                ['order_book_id', 'datetime', 'high', 'iopv', 'open', 'low', 'close',
                    'volume', 'total_turnover'],
                """
                query = "CREATE TABLE IF NOT EXISTS \
                `quantaxis`.`{}` (\
                    datetime Datetime DEFAULT '1970-01-01',\
                        order_book_id String,\
                        iopv Float64,\
                        open Float64,\
                        high Float64,\
                        low Float64,\
                        close Float64,\
                        volume Float64,\
                        total_turnover Float64\
                    )\
                    ENGINE = ReplacingMergeTree() \
                    PARTITION BY (toYYYYMMDD(`datetime`)) \
                    ORDER BY (datetime, order_book_id)\
                    SETTINGS index_granularity=8192".format(self.table_name)
        elif 'tick' in self.frequence:
            if 'stock' in self.market:
                query = "CREATE TABLE IF NOT EXISTS \
                    `quantaxis`.`{}` (\
                        datetime Datetime DEFAULT '1970-01-01',\
                        trading_date Date, \
                        order_book_id String,\
                        open Float64,\
                        last Float64,\
                        high Float64,\
                        low  Float64,\
                        prev_close Float64,\
                        volume   Float64,\
                        total_turnover Float64,\
                        limit_up  Float64,\
                        limit_down Float64,\
                        a1  Float64,\
                        a2  Float64,\
                        a3  Float64,\
                        a4  Float64,\
                        a5  Float64,\
                        b1  Float64,\
                        b2  Float64,\
                        b3  Float64,\
                        b4  Float64,\
                        b5  Float64,\
                        a1_v Float64,\
                        a2_v Float64,\
                        a3_v Float64,\
                        a4_v Float64,\
                        a5_v Float64,\
                        b1_v Float64,\
                        b2_v Float64,\
                        b3_v Float64,\
                        b4_v Float64,\
                        b5_v Float64,\
                        change_rate Float64\
                        )\
                        ENGINE = ReplacingMergeTree() \
                        PARTITION BY (toYYYYMMDD(`datetime`)) \
                        ORDER BY (datetime, order_book_id)\
                        SETTINGS index_granularity=8192".format(self.table_name)
            elif 'future' in self.market:
                query = "CREATE TABLE IF NOT EXISTS \
                    `quantaxis`.`{}` (\
                        datetime Datetime DEFAULT '1970-01-01',\
                        trading_date Date, \
                        order_book_id String,\
                        open Float64,\
                        last Float64,\
                        high Float64,\
                        low  Float64,\
                        prev_settlement Float64,\
                        prev_close Float64,\
                        open_interest Float64,\
                        volume   Float64,\
                        total_turnover Float64,\
                        limit_up  Float64,\
                        limit_down Float64,\
                        a1  Float64,\
                        a2  Float64,\
                        a3  Float64,\
                        a4  Float64,\
                        a5  Float64,\
                        b1  Float64,\
                        b2  Float64,\
                        b3  Float64,\
                        b4  Float64,\
                        b5  Float64,\
                        a1_v Float64,\
                        a2_v Float64,\
                        a3_v Float64,\
                        a4_v Float64,\
                        a5_v Float64,\
                        b1_v Float64,\
                        b2_v Float64,\
                        b3_v Float64,\
                        b4_v Float64,\
                        b5_v Float64,\
                        change_rate Float64\
                        )\
                        ENGINE = ReplacingMergeTree() \
                        PARTITION BY (toYYYYMMDD(`datetime`)) \
                        ORDER BY (datetime, order_book_id)\
                        SETTINGS index_granularity=8192".format(self.table_name)
            elif 'option' in self.market:
                """
                ['order_book_id', 'datetime', 'trading_date', 'open', 'last', 'high',
                'low', 'prev_settlement', 'prev_close', 'volume', 'open_interest',
                'total_turnover', 'limit_up', 'limit_down', 'a1', 'a2', 'a3', 'a4',
                'a5', 'b1', 'b2', 'b3', 'b4', 'b5', 'a1_v', 'a2_v', 'a3_v', 'a4_v',
                'a5_v', 'b1_v', 'b2_v', 'b3_v', 'b4_v', 'b5_v', 'change_rate'],
                """
                query = "CREATE TABLE IF NOT EXISTS \
                    `quantaxis`.`{}` (\
                        datetime Datetime DEFAULT '1970-01-01',\
                        trading_date Date, \
                        order_book_id String,\
                        open Float64,\
                        last Float64,\
                        high Float64,\
                        low  Float64,\
                        prev_settlement Float64,\
                        prev_close Float64,\
                        open_interest Float64,\
                        volume   Float64,\
                        total_turnover Float64,\
                        limit_up  Float64,\
                        limit_down Float64,\
                        a1  Float64,\
                        a2  Float64,\
                        a3  Float64,\
                        a4  Float64,\
                        a5  Float64,\
                        b1  Float64,\
                        b2  Float64,\
                        b3  Float64,\
                        b4  Float64,\
                        b5  Float64,\
                        a1_v Float64,\
                        a2_v Float64,\
                        a3_v Float64,\
                        a4_v Float64,\
                        a5_v Float64,\
                        b1_v Float64,\
                        b2_v Float64,\
                        b3_v Float64,\
                        b4_v Float64,\
                        b5_v Float64,\
                        change_rate Float64\
                        )\
                        ENGINE = ReplacingMergeTree() \
                        PARTITION BY (toYYYYMMDD(`datetime`)) \
                        ORDER BY (datetime, order_book_id)\
                        SETTINGS index_granularity=8192".format(self.table_name)

            elif 'index' in self.market:
                query = "CREATE TABLE IF NOT EXISTS \
                    `quantaxis`.`{}` (\
                        datetime Datetime DEFAULT '1970-01-01',\
                        trading_date Date, \
                        order_book_id String,\
                        open Float64,\
                        last Float64,\
                        high Float64,\
                        low  Float64,\
                        prev_close Float64,\
                        volume   Float64,\
                        total_turnover Float64,\
                        limit_up  Float64,\
                        limit_down Float64,\
                        a1  Float64,\
                        a2  Float64,\
                        a3  Float64,\
                        a4  Float64,\
                        a5  Float64,\
                        b1  Float64,\
                        b2  Float64,\
                        b3  Float64,\
                        b4  Float64,\
                        b5  Float64,\
                        a1_v Float64,\
                        a2_v Float64,\
                        a3_v Float64,\
                        a4_v Float64,\
                        a5_v Float64,\
                        b1_v Float64,\
                        b2_v Float64,\
                        b3_v Float64,\
                        b4_v Float64,\
                        b5_v Float64,\
                        change_rate Float64\
                        )\
                        ENGINE = ReplacingMergeTree() \
                        PARTITION BY (toYYYYMMDD(`datetime`)) \
                        ORDER BY (datetime, order_book_id)\
                        SETTINGS index_granularity=8192".format(self.table_name)
            elif 'lof' in self.market:
                """
                ['order_book_id', 'datetime', 'trading_date', 'open', 'last', 'high',
                'low', 'prev_close', 'volume', 'total_turnover', 'limit_up',
                'limit_down', 'a1', 'a2', 'a3', 'a4', 'a5', 'b1', 'b2', 'b3', 'b4',
                'b5', 'a1_v', 'a2_v', 'a3_v', 'a4_v', 'a5_v', 'b1_v', 'b2_v', 'b3_v',
                'b4_v', 'b5_v', 'change_rate', 'iopv', 'prev_iopv']
                """
                query = "CREATE TABLE IF NOT EXISTS \
                    `quantaxis`.`{}` (\
                        datetime Datetime DEFAULT '1970-01-01',\
                        trading_date Date, \
                        order_book_id String,\
                        open Float64,\
                        last Float64,\
                        high Float64,\
                        low  Float64,\
                        prev_close Float64,\
                        volume   Float64,\
                        total_turnover Float64,\
                        limit_up  Float64,\
                        limit_down Float64,\
                        a1  Float64,\
                        a2  Float64,\
                        a3  Float64,\
                        a4  Float64,\
                        a5  Float64,\
                        b1  Float64,\
                        b2  Float64,\
                        b3  Float64,\
                        b4  Float64,\
                        b5  Float64,\
                        a1_v Float64,\
                        a2_v Float64,\
                        a3_v Float64,\
                        a4_v Float64,\
                        a5_v Float64,\
                        b1_v Float64,\
                        b2_v Float64,\
                        b3_v Float64,\
                        b4_v Float64,\
                        b5_v Float64,\
                        change_rate Float64,\
                        iopv Float64,\
                        prev_iopv Float64\
                        )\
                        ENGINE = ReplacingMergeTree() \
                        PARTITION BY (toYYYYMMDD(`datetime`)) \
                        ORDER BY (datetime, order_book_id)\
                        SETTINGS index_granularity=8192".format(self.table_name)
        pprint.pprint(query)
        self.client.execute(query)

    def get_list(self, date=None):
        """
        order_book_id	str	证券代码，证券的独特的标识符。应以'.XSHG'或'.XSHE'或'.XHKG'结尾。 '.XSHG' - 上证，'.XSHE' - 深证， '.XHKG' - 港股
        symbol	str	证券的简称，例如'平安银行'
        abbrev_symbol	str	证券的名称缩写，在中国 A 股就是股票的拼音缩写。例如：'PAYH'就是平安银行股票的证券名缩写
        round_lot	int	一手对应多少股，中国 A 股一手是 100 股
        sector_code	str	板块缩写代码，全球通用标准定义
        sector_code_name	str	以当地语言为标准的板块代码名
        industry_code	str	国民经济行业分类代码，具体可参考下方“Industry 列表”
        industry_name	str	国民经济行业分类名称
        listed_date	str	该证券上市日期
        issue_price	float	该证券发行价 （元）
        de_listed_date	str	退市日期
        type	str	合约类型，目前支持的类型有: 'CS', 'INDX', 'LOF', 'ETF', 'Future'
        underlying_order_book_id已废弃	str	追踪基准的合约代码。目前仅限'ETF','LOF'
        underlying_name 已废弃	str	追踪基准的合约名称。目前仅限'ETF','LOF'
        concept_names 已废弃	str	概念股分类，例如：'铁路基建'，'基金重仓'等
        exchange	str	交易所，'XSHE' - 深交所, 'XSHG' - 上交所
        board_type	str	板块类别，'MainBoard' - 主板,'GEM' - 创业板,'SME' - 中小企业板,'KSH' - 科创板
        status	str	合约状态。'Active' - 正常上市, 'Delisted' - 终止上市, 'TemporarySuspended' - 暂停上市, 'PreIPO' - 发行配售期间, 'FailIPO' - 发行失败
        special_type	str	特别处理状态。'Normal' - 正常上市, 'ST' - ST 处理, 'StarST' - *ST 代表该股票正在接受退市警告, 'PT' - 代表该股票连续 3 年收入为负，将被暂停交易, 'Other' - 其他
        trading_hours	str	合约交易时间
        least_redeem	str	最低申赎份额，仅对 ETF 基金展示
        cross_market	str	沪深港通标识。True-支持，False-不支持。仅对港股生效
        eng_symbol	str	英文简称。仅对港股生效

        order_book_id	str	期货代码，期货的独特的标识符（郑商所期货合约数字部分进行了补齐。例如原有代码'ZC609'补齐之后变为'ZC1609'）。主力连续合约 UnderlyingSymbol+88，例如'IF88' ；指数连续合约命名规则为 UnderlyingSymbol+99
        symbol	str	期货的简称，例如'沪深 1005'
        margin_rate	float	期货合约的最低保证金率
        round_lot	float	期货全部为 1.0
        listed_date	str	期货的上市日期。主力连续合约与指数连续合约都为'0000-00-00'
        de_listed_date	str	期货的退市日期。
        industry_name	str	行业分类名称
        trading_code	str	交易代码
        market_tplus	str	交易制度。'0'表示 T+0，'1'表示 T+1，往后顺推
        type	str	合约类型，'Future'
        contract_multiplier	float	合约乘数，例如沪深 300 股指期货的乘数为 300.0
        underlying_order_book_id	str	合约标的代码，目前除股指期货(IH, IF, IC)之外的期货合约，这一字段全部为'null'
        underlying_symbol	str	合约标的名称，例如 IF1005 的合约标的名称为'IF'
        maturity_date	str	期货到期日。主力连续合约与指数连续合约都为'0000-00-00'
        exchange	str	交易所，'DCE' - 大连商品交易所, 'SHFE' - 上海期货交易所，'CFFEX' - 中国金融期货交易所, 'CZCE'- 郑州商品交易所
        trading_hours	str	合约交易时间
        product	str	合约种类，'Commodity'-商品期货，'Index'-股指期货，'Government'-国债期货
        """

        return all_instruments(type=self.market_dict[self.market], market='cn', date=date)

    def get_all_history(self):
        codelist = self.codelist.order_book_id.tolist()
        print(codelist)
        return get_price(codelist, start_date='2000-01-04', end_date=self.endtime,
                         frequency=self.freq_dict[self.frequence], adjust_type='none', expect_df=True)

    def get_code(self, code):
        return get_price(code, start_date='2000-01-04', end_date=self.endtime,
                         frequency=self.freq_dict[self.frequence], adjust_type='none', expect_df=True)

    @property
    def endtime(self):
        return self.get_end_time()

    def get_end_time(self):
        """返回行情拉取结束时间（含 17:00:00）。

        周末/节假日时 QA_util_get_last_day 可能返回 ``wrong date``；
        优先用 RQData 交易日历，再回退到工作日推算。
        """

        today = datetime.date.today()
        now = datetime.datetime.now()
        anchor = today if now.hour >= 15 else today - datetime.timedelta(days=1)

        last: Optional[str] = None
        if get_trading_dates is not None:
            try:
                cal = get_trading_dates(
                    start_date=(anchor - datetime.timedelta(days=30)).isoformat(),
                    end_date=anchor.isoformat(),
                    market="cn",
                )
                if cal is not None and not cal.empty:
                    col = "trading_date" if "trading_date" in cal.columns else cal.columns[0]
                    last = str(pd.to_datetime(cal[col]).max().date())
            except Exception:
                last = None

        if not last:
            try:
                candidate = QA.QA_util_get_last_day(today)
            except Exception:
                candidate = None
            if (
                candidate
                and str(candidate) != "wrong date"
                and len(str(candidate)) == 10
            ):
                last = str(candidate)

        if not last:
            day = anchor
            while day.weekday() >= 5:  # 六日
                day -= datetime.timedelta(days=1)
            last = day.isoformat()

        return f"{last} 17:00:00"

    def get_data(self, start, end=None):

        codelist = self.codelist.order_book_id.tolist()

        if end is None:
            end = self.endtime
        print('get data', start, end)
        return get_price(codelist, start_date=start, end_date=end,
                         frequency=self.freq_dict[self.frequence], adjust_type='none', expect_df=True)

    def get_raw(self, code, start, end):
        return get_price(code, start_date=start, end_date=end,
                         frequency=self.freq_dict[self.frequence], adjust_type='none', expect_df=True)

    def save_history(self, data=None):
        """ 
        """

        self.create_table()
        print(self.market)
        if 'day' in self.frequence:
            if data is None:
                self.temp_data = self.get_all_history()

            self.save_data(self.temp_data)
        elif 'min' in self.frequence:
            if data is None:
                codelist = self.codelist.order_book_id.tolist()
                for code in codelist:
                    print(code, codelist.index(code), len(codelist))
                    self.temp_data = self.get_code(code)
                    if self.temp_data is None:
                        pass
                    else:
                        self.save_data(self.temp_data)
        elif 'tick' in self.frequence:

            if data is None:

                datelist = QA.QA_util_get_trade_range(
                    '2000-01-01',  self.endtime[0:10])[::-1]
                codelist = self.codelist.order_book_id.tolist()
                for date in datelist:
                    print(date, datelist.index(date), len(datelist))

                    if 'stock' in self.market:

                        for ic in range(0, len(codelist), 120):

                            codes = codelist[ic:ic+120]
                            #print(codes, date)
                            self.temp_data = self.get_raw(
                                codes, date+' 00:00:00', date+' 23:59:59')

                            if self.temp_data is None:
                                pass
                            else:
                                self.save_data(self.temp_data)
                    elif 'future' in self.market:

                        codelist = self.get_list(date).order_book_id.tolist()

                        for ic in range(0, len(codelist), 20):

                            codes = codelist[ic:ic+20]
                            print(ic, codes)
                            self.temp_data = self.get_raw(
                                codes, date+' 00:00:00', date+' 23:59:59.999')
                            if self.temp_data is None:
                                pass
                            else:
                                self.save_data(self.temp_data)

    def resave_history(self, start_id=0):
        """
        集中下载时候的
        """

        if 'day' in self.frequence:
            pass
        elif 'min' in self.frequence:

            codelist = self.codelist.order_book_id.tolist()
            for code in codelist[start_id:]:
                print(code, codelist.index(code), len(codelist))
                data = self.get_code(code)
                if data is None:
                    pass
                else:
                    self.save_data(data)
        elif 'tick' in self.frequence:

            codelist = self.codelist.order_book_id.tolist()
            datelist = QA.QA_util_get_trade_range(
                '2000-01-01', self.endtime[0:10])[::-1]

            for date in datelist[start_id:]:
                print(date, datelist.index(date), len(datelist))
                codelist = self.codelist.order_book_id.tolist()
                self.temp_data = self.get_raw(
                    codelist, date+' 00:00:00', date+' 23:59:59')

                if self.temp_data is None:
                    pass
                else:
                    self.save_data(self.temp_data)

    def save_data(self, data):
        """
        save data
        """
        print(data)
        frame = data.reset_index()
        # HTTP CSV 常把 date/datetime 解析成字符串，写入 ClickHouse Date/Datetime 前需转换
        if "date" not in frame.columns and "datetime" in frame.columns and "day" in self.frequence:
            frame = frame.rename(columns={"datetime": "date"})
        if "date" in frame.columns:
            frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
        if "datetime" in frame.columns:
            frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce")
        if "trading_date" in frame.columns:
            frame["trading_date"] = pd.to_datetime(
                frame["trading_date"], errors="coerce"
            ).dt.date

        # 按目标表字段裁剪/补齐，避免 HTTP 多返回 prev_close 等列导致 INSERT 失败
        try:
            table_cols = [
                str(r[0])
                for r in self.client.execute(f"DESCRIBE TABLE quantaxis.{self.table_name}")
            ]
        except Exception:
            table_cols = list(frame.columns)
        for col in table_cols:
            if col not in frame.columns:
                if col in {"num_trades", "volume", "total_turnover", "open", "high", "low", "close"}:
                    frame[col] = 0.0
                else:
                    frame[col] = ""
        frame = frame.loc[:, table_cols].copy()

        if 'day' in self.frequence:
            data = frame.sort_values('date').to_dict('records')
            chunks = [data[i:i + 100] for i in range(0, len(data), 100)]
        elif 'min' in self.frequence:
            # 分钟表按 toYYYYMMDD 分区；单次 INSERT 跨太多交易日会触发
            # max_partitions_per_insert_block(100)。按自然月分批写入。
            frame = frame.sort_values('datetime')
            frame['_ym'] = pd.to_datetime(frame['datetime']).dt.strftime('%Y%m')
            chunks = [
                g.drop(columns=['_ym']).to_dict('records')
                for _, g in frame.groupby('_ym', sort=True)
            ]
        elif 'tick' in self.frequence:
            # tick 同样按日分区时也按月切，避免超分区限制
            if 'datetime' in frame.columns:
                frame = frame.sort_values('datetime')
                frame['_ym'] = pd.to_datetime(frame['datetime']).dt.strftime('%Y%m')
                chunks = [
                    g.drop(columns=['_ym']).to_dict('records')
                    for _, g in frame.groupby('_ym', sort=True)
                ]
            else:
                data = frame.to_dict('records')
                chunks = [data[i:i + 800000] for i in range(0, len(data), 800000)]
        else:
            chunks = [frame.to_dict('records')]

        total_rows = sum(len(ch) for ch in chunks)
        print('finish process data', total_rows, 'chunks', len(chunks))

        for i, chunk in enumerate(chunks, 1):
            if not chunk:
                continue
            self.client.execute(
                'INSERT INTO {} VALUES'.format(self.table_name), chunk
            )
            if i == 1 or i % 12 == 0 or i == len(chunks):
                print(f'insert chunk [{i}/{len(chunks)}] rows={len(chunk)}')
        self.client.execute('optimize table {}'.format(self.table_name))

    def save_bars(
        self,
        start_date: str,
        end_date: Optional[str] = None,
        codes: Optional[list[str]] = None,
        *,
        rebuild: bool = False,
        chunk_years: int = 1,
    ) -> dict:
        """从 RQData get_price 拉取当前 frequence 行情并写入 ``quantaxis.{table_name}``。

        分钟线按年分块请求，避免单次 HTTP 过大。
        """

        if codes is not None:
            codelist = [str(c) for c in codes]
        else:
            codelist = self.codelist.order_book_id.astype(str).tolist()
        if not codelist:
            raise RuntimeError("codelist 为空，无法拉取行情")

        if end_date is None:
            end_date = self.endtime

        if rebuild:
            self.drop_table()
        self.create_table()

        freq = self.freq_dict[self.frequence]
        total = 0
        print(
            f"save_bars table={self.table_name} freq={freq} "
            f"codes={len(codelist)} range=[{start_date} .. {end_date}]"
        )

        if "day" in self.frequence:
            ranges = [(start_date, end_date)]
        else:
            # 分钟：按年切片；end_date 可能带 " HH:MM:SS"
            start_ts = pd.Timestamp(str(start_date)[:10])
            end_ts = pd.Timestamp(str(end_date)[:10])
            ranges = []
            cur = pd.Timestamp(year=start_ts.year, month=1, day=1)
            if cur < start_ts:
                cur = start_ts
            while cur <= end_ts:
                year_end = pd.Timestamp(year=cur.year + chunk_years - 1, month=12, day=31)
                seg_end = min(year_end, end_ts)
                if "min" in self.frequence:
                    ranges.append(
                        (
                            f"{cur.date()} 00:00:00",
                            f"{seg_end.date()} 17:00:00",
                        )
                    )
                else:
                    ranges.append((str(cur.date()), str(seg_end.date())))
                cur = seg_end + pd.Timedelta(days=1)

        for i, (seg_start, seg_end) in enumerate(ranges, 1):
            try:
                part = get_price(
                    codelist,
                    start_date=seg_start,
                    end_date=seg_end,
                    frequency=freq,
                    adjust_type="none",
                    expect_df=True,
                )
            except Exception as exc:  # noqa: BLE001
                print(f"get_price fail [{i}/{len(ranges)}] {seg_start}~{seg_end}: {exc}")
                continue
            if part is None or getattr(part, "empty", True):
                print(f"get_price empty [{i}/{len(ranges)}] {seg_start}~{seg_end}")
                continue
            before = total
            self.save_data(part)
            # save_data 不返回行数，这里用 part 估算
            total += len(part.reset_index())
            print(
                f"save_bars [{i}/{len(ranges)}] {seg_start}~{seg_end} "
                f"rows+={total - before} total~={total}"
            )

        stats_sql = (
            f"SELECT count(), uniqExact(order_book_id), min(date), max(date) "
            f"FROM quantaxis.{self.table_name}"
            if "day" in self.frequence
            else (
                f"SELECT count(), uniqExact(order_book_id), min(datetime), max(datetime) "
                f"FROM quantaxis.{self.table_name}"
            )
        )
        stats = self.client.execute(stats_sql)[0]
        info = {
            "table": self.table_name,
            "rows": stats[0],
            "codes": stats[1],
            "min": stats[2],
            "max": stats[3],
        }
        print(f"save_bars done {info}")
        return info

    def drop_table(self):

        self.client.execute('DROP TABLE IF EXISTS {}'.format(self.table_name))

    def query_dblastdate(self,):
        if 'min' in self.frequence:
            return self.client.execute('SELECT datetime from quantaxis.{} order by datetime desc limit 1'.format(self.table_name))[0][0]
        elif 'day' in self.frequence:
            return self.client.execute('SELECT date from quantaxis.{} order by date desc limit 1'.format(self.table_name))[0][0]

    def update_all(self):
        lastdate = self.query_dblastdate()
        print('last datetime in database', lastdate)
        data = self.get_data(lastdate)
        self.save_data(data)

    def update_today(self):
        now = datetime.datetime.now()
        if now.hour > 15 and now.hour < 21:
            date = str(datetime.date.today())
            yes_night = QA.QA_util_get_last_day(date) + ' 20:50:00'
            end = date + ' 17:00:00'
            print(yes_night, end)
            self.temp_data = self.get_data(start=yes_night, end=end)
            self.save_data(self.temp_data)

    def update_from(self, start):
        data = self.get_data(start)
        self.save_data(data)

    def save_codelist(
        self,
        codes: Optional[list[str]] = None,
        *,
        rebuild: bool = True,
    ):
        """写入 stock_cn_codelist。codes 非空时仅写入子集（便于小范围测试）。"""

        if 'stock' not in self.market:
            return

        columns = [
            'order_book_id', 'industry_code', 'market_tplus', 'symbol',
            'special_type', 'exchange', 'status', 'type', 'de_listed_date',
            'listed_date', 'sector_code_name', 'abbrev_symbol', 'sector_code',
            'round_lot', 'trading_hours', 'board_type', 'industry_name',
            'issue_price', 'trading_code', 'purchasedate',
        ]
        if rebuild:
            self.client.execute('DROP TABLE IF EXISTS quantaxis.stock_cn_codelist')
        self.client.execute("CREATE TABLE IF NOT EXISTS \
            `quantaxis`.`stock_cn_codelist` (\
                order_book_id String,\
                industry_code String,\
                market_tplus  Int32,\
                symbol   String,\
                special_type  String,\
                exchange    String,\
                status      String,\
                type       String,\
                de_listed_date   String,\
                listed_date   String,\
                sector_code_name String,\
                abbrev_symbol String,\
                sector_code  String,\
                round_lot Float64,\
                trading_hours String,\
                board_type  String,\
                industry_name  String,\
                issue_price  Float64,\
                trading_code String,\
                purchasedate   String\
                )\
                ENGINE = ReplacingMergeTree() \
                ORDER BY (listed_date, order_book_id)\
                SETTINGS index_granularity=8192")

        frame = self.codelist.copy()
        if codes:
            wanted = set(str(c) for c in codes)
            frame = frame[frame.order_book_id.astype(str).isin(wanted)]
        for column in columns:
            if column not in frame.columns:
                frame[column] = 0.0 if column in {'issue_price', 'round_lot', 'market_tplus'} else 'nan'
        frame['issue_price'] = pd.to_numeric(frame['issue_price'], errors='coerce').fillna(0.0)
        frame['market_tplus'] = pd.to_numeric(frame['market_tplus'], errors='coerce').fillna(1).astype(int)
        frame['round_lot'] = pd.to_numeric(frame['round_lot'], errors='coerce').fillna(100.0)
        frame = frame.loc[:, columns].copy()
        string_cols = [
            c for c in columns
            if c not in {'market_tplus', 'round_lot', 'issue_price'}
        ]
        for column in string_cols:
            frame[column] = frame[column].fillna('nan').astype(str)
        records = frame.to_dict('records')
        rangex = 1000
        sql = (
            'INSERT INTO quantaxis.stock_cn_codelist ('
            + ', '.join(columns)
            + ') VALUES'
        )
        for i in range(0, len(records), rangex):
            print(i)
            chunk = [tuple(row[c] for c in columns) for row in records[i:i + rangex]]
            self.client.execute(sql, chunk)
        self.client.execute('OPTIMIZE TABLE quantaxis.stock_cn_codelist')
        print(f'save_codelist done rows={len(records)}')

    def save_fundlist(self, limit: int = 0, *, rebuild: bool = True):
        """写入 fund_cn_codelist。HTTP 下 fund.all_instruments 无权限时回退 ETF+LOF。"""

        columns = [
            'order_book_id', 'establishment_date', 'listed_date', 'transition_time',
            'amc', 'symbol', 'fund_type', 'fund_manager', 'latest_size', 'benchmark',
            'accrued_daily', 'de_listed_date', 'stop_date', 'exchange', 'round_lot',
        ]
        if rebuild:
            self.client.execute('DROP TABLE IF EXISTS quantaxis.fund_cn_codelist')
        self.client.execute("CREATE TABLE IF NOT EXISTS \
            `quantaxis`.`fund_cn_codelist` (\
                order_book_id String,\
                establishment_date String,\
                listed_date  String,\
                transition_time   Int64,\
                amc  String,\
                symbol    String,\
                fund_type      String,\
                fund_manager       String,\
                latest_size   Float64,\
                benchmark   String,\
                accrued_daily Boolean,\
                de_listed_date String,\
                stop_date  String,\
                exchange String,\
                round_lot Float64\
                )\
                ENGINE = ReplacingMergeTree() \
                ORDER BY (order_book_id)\
                SETTINGS index_granularity=8192")

        if fund is None:
            raise RuntimeError(
                "fund API 未绑定。HTTP 请确认 init_rqdatac()；"
                "TCP 需 pip install 'rqdatac[fund]'"
            )
        data = fund.all_instruments()
        if data is None or data.empty:
            raise RuntimeError('fund.all_instruments 无数据')
        if limit > 0:
            data = data.head(limit).copy()
        for column in columns:
            if column not in data.columns:
                if column in {'latest_size', 'round_lot'}:
                    data[column] = 0.0
                elif column == 'transition_time':
                    data[column] = 0
                elif column == 'accrued_daily':
                    data[column] = False
                else:
                    data[column] = ''
        data['latest_size'] = pd.to_numeric(data['latest_size'], errors='coerce').fillna(0.0)
        data['round_lot'] = pd.to_numeric(data['round_lot'], errors='coerce').fillna(1.0)
        data['transition_time'] = (
            pd.to_numeric(data['transition_time'], errors='coerce').fillna(0).astype('int64')
        )
        data['accrued_daily'] = data['accrued_daily'].fillna(False).astype(bool)
        string_cols = [
            c for c in columns
            if c not in {'transition_time', 'latest_size', 'round_lot', 'accrued_daily'}
        ]
        for column in string_cols:
            data[column] = data[column].fillna('').astype(str)
        records = data.loc[:, columns].to_dict('records')
        rangex = 1000
        sql = (
            'INSERT INTO quantaxis.fund_cn_codelist ('
            + ', '.join(columns)
            + ') VALUES'
        )
        for i in range(0, len(records), rangex):
            print(i)
            chunk = [tuple(row[c] for c in columns) for row in records[i:i + rangex]]
            self.client.execute(sql, chunk)
        self.client.execute('OPTIMIZE TABLE quantaxis.fund_cn_codelist')
        print(f'save_fundlist done rows={len(records)}')




    def save_adj_factor(
        self,
        batch_size: int = 80,
        codes: Optional[list[str]] = None,
        *,
        rebuild: bool = True,
    ):
        """从 RQData 拉取除权因子，展开为日频 adj，写入 quantaxis.stock_adj。

        HTTP 通道下会对代码分批请求；交易日历优先用 RQData get_trading_dates
        （避免本地 trade_date_sse 坏数据）。
        """

        if 'stock' not in self.market:
            return

        if codes:
            codelist = [str(c) for c in codes]
        else:
            codelist = self.codelist.order_book_id.astype(str).tolist()
        # 复权因子按自然日即可；勿用 self.endtime（含 "17:00:00"，
        # 且周末时 QA_util_get_last_day 可能返回 "wrong date"）
        end = str(datetime.date.today())
        print(f'save_adj_factor codes={len(codelist)} end={end} batch={batch_size}')

        # 1) 分批拉取 get_ex_factor（全市场一次请求在 HTTP 下易失败/过大）
        frames: list[pd.DataFrame] = []
        for offset in range(0, len(codelist), batch_size):
            batch = codelist[offset: offset + batch_size]
            try:
                part = get_ex_factor(
                    batch,
                    start_date='1990-01-01',
                    end_date=end,
                    market='cn',
                )
            except Exception as exc:  # noqa: BLE001
                # 北交所等无效代码会导致整批 400，降级逐个拉取
                print(f'batch get_ex_factor fail @{batch[0]}: {exc}; fallback per-code')
                part_rows: list[pd.DataFrame] = []
                for code in batch:
                    try:
                        one = get_ex_factor(
                            [code],
                            start_date='1990-01-01',
                            end_date=end,
                            market='cn',
                        )
                        if one is not None and not one.empty:
                            part_rows.append(one.reset_index(drop=True))
                    except Exception:
                        continue
                part = pd.concat(part_rows, ignore_index=True) if part_rows else None
            if part is not None and not part.empty:
                frames.append(part.reset_index(drop=True))
            print(
                f'get_ex_factor [{offset // batch_size + 1}/'
                f'{(len(codelist) + batch_size - 1) // batch_size}] '
                f'codes={len(batch)} rows='
                f'{0 if part is None or part.empty else len(part)}'
            )

        if not frames:
            raise RuntimeError('get_ex_factor 无返回数据，无法写入 stock_adj')
        fac = pd.concat(frames, ignore_index=True)
        print('finish get factor', len(fac))

        # 2) 交易日历
        try:
            if get_trading_dates is None:
                raise RuntimeError('get_trading_dates 未绑定')
            cal_frame = get_trading_dates(
                start_date='1990-01-01',
                end_date=str(datetime.date.today()),
                market='cn',
            )
            calendar = pd.to_datetime(cal_frame['trading_date'], errors='coerce').dropna()
        except Exception:
            # 兜底：清洗本地日历中的坏条目
            calendar = pd.to_datetime(
                [d for d in QA.trade_date_sse if isinstance(d, str) and len(d) == 10],
                errors='coerce',
            ).dropna()
        cal_df = pd.DataFrame({'ex_date': calendar})
        today = pd.Timestamp(datetime.date.today())

        def apply_adjust(group: pd.DataFrame) -> pd.DataFrame:
            u1 = group.reset_index(drop=True).copy()
            u1['ex_date'] = pd.to_datetime(u1['ex_date'], errors='coerce')
            u1['ex_factor'] = pd.to_numeric(u1['ex_factor'], errors='coerce')
            u1 = u1.dropna(subset=['ex_date', 'ex_factor']).sort_values('ex_date')
            if u1.empty:
                return pd.DataFrame(columns=['adj'])
            u1 = u1.assign(adj=(1 / u1.ex_factor.values[::-1].cumprod())[::-1])
            eu = (
                pd.concat([u1.loc[:, ['ex_date', 'adj']], cal_df], ignore_index=True)
                .sort_values('ex_date')
                .bfill()
                .set_index('ex_date')
                .loc[:today]
                .fillna(1)
            )
            return eu.shift(-1).fillna(1)

        fu = (
            fac.groupby('order_book_id', group_keys=True)
            .apply(apply_adjust, include_groups=False)
            .reset_index()
        )
        fu.columns = ['order_book_id', 'date', 'adj']
        fu['date'] = pd.to_datetime(fu['date'], errors='coerce').dt.date
        fu['adj'] = pd.to_numeric(fu['adj'], errors='coerce').fillna(1.0)
        fu = fu.dropna(subset=['date', 'order_book_id'])
        print('finish execute factor', len(fu))

        print('prepare clear database')
        if rebuild:
            self.client.execute('DROP TABLE IF EXISTS quantaxis.stock_adj')
        query = "CREATE TABLE IF NOT EXISTS \
            `quantaxis`.`stock_adj` (\
                date Date DEFAULT '1970-01-01',\
                adj Float64,\
                order_book_id String\
                )\
                ENGINE = ReplacingMergeTree() \
                ORDER BY (date, order_book_id)\
                SETTINGS index_granularity=8192"
        self.client.execute(query)

        print('prepare insert database')
        values = [
            (row.date, float(row.adj), str(row.order_book_id))
            for row in fu.loc[:, ['date', 'adj', 'order_book_id']].itertuples(index=False)
        ]
        insert_sql = (
            'INSERT INTO quantaxis.stock_adj (date, adj, order_book_id) VALUES'
        )
        chunk = 20000
        for i in range(0, len(values), chunk):
            self.client.execute(insert_sql, values[i: i + chunk])
            if i // chunk % 20 == 0:
                print(f'inserted {min(i + chunk, len(values))}/{len(values)}')

        self.client.execute('OPTIMIZE TABLE quantaxis.stock_adj')
        stats = self.client.execute(
            'SELECT count(), uniqExact(order_book_id), min(date), max(date) '
            'FROM quantaxis.stock_adj'
        )[0]
        print(
            f'stock_adj done rows={stats[0]} codes={stats[1]} '
            f'date=[{stats[2]} .. {stats[3]}]'
        )

    def _load_codes_from_table(
        self,
        table: str,
        *,
        status_active_only: bool = True,
        limit: int = 0,
    ) -> list[str]:
        """从 ClickHouse 代码表读取 order_book_id 列表。"""

        where = "WHERE status = 'Active'" if status_active_only else ""
        sql = f"SELECT order_book_id FROM quantaxis.{table} {where} ORDER BY order_book_id"
        if limit > 0:
            sql += f" LIMIT {int(limit)}"
        rows = self.client.execute(sql)
        return [str(r[0]) for r in rows]

    def _insert_frame(
        self,
        table: str,
        frame: pd.DataFrame,
        columns: list[str],
        *,
        chunk: int = 5000,
    ) -> int:
        if frame is None or frame.empty:
            return 0
        data = frame.loc[:, columns].copy()
        records = data.to_dict("records")
        sql = f"INSERT INTO quantaxis.{table} ({', '.join(columns)}) VALUES"
        for i in range(0, len(records), chunk):
            part = [tuple(row[c] for c in columns) for row in records[i:i + chunk]]
            self.client.execute(sql, part)
        return len(records)

    def save_index_weight(
        self,
        start_date: str,
        end_date: str,
        codes: Optional[list[str]] = None,
        *,
        rebuild: bool = True,
        limit: int = 0,
    ) -> dict:
        """从 RQData ``index_weights`` 拉取并写入 ``quantaxis.index_weight``。

        ``codes`` 默认取 ``index_cn_codelist`` 的 Active 指数。
        表结构对齐 QAClickhouse.get_index_weight：date / index_code / order_book_id / weight。
        """

        if index_weights is None:
            raise RuntimeError("index_weights 未绑定，请先 init_rqdatac()")

        if codes is None:
            codes = self._load_codes_from_table(
                "index_cn_codelist", status_active_only=True, limit=limit
            )
        else:
            codes = [str(c) for c in codes]
            if limit > 0:
                codes = codes[:limit]
        if not codes:
            raise RuntimeError("无可用指数代码（index_cn_codelist / codes）")

        if rebuild:
            self.client.execute("DROP TABLE IF EXISTS quantaxis.index_weight")
        self.client.execute(
            """
            CREATE TABLE IF NOT EXISTS quantaxis.index_weight (
                date Date,
                index_code String,
                order_book_id String,
                weight Float64
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (date, index_code, order_book_id)
            SETTINGS index_granularity=8192
            """
        )

        total = 0
        ok = 0
        for i, code in enumerate(codes, 1):
            try:
                part = index_weights(
                    code, start_date=start_date, end_date=end_date, market="cn"
                )
            except Exception as exc:  # noqa: BLE001
                print(f"index_weights fail {code}: {exc}")
                continue
            if part is None or part.empty:
                continue
            frame = part.reset_index(drop=True).copy()
            if "date" not in frame.columns:
                frame["date"] = start_date
            frame["index_code"] = code
            frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
            frame["order_book_id"] = frame["order_book_id"].astype(str)
            frame["weight"] = pd.to_numeric(frame["weight"], errors="coerce").fillna(0.0)
            frame = frame.dropna(subset=["date", "order_book_id"])
            n = self._insert_frame(
                "index_weight",
                frame,
                ["date", "index_code", "order_book_id", "weight"],
            )
            total += n
            ok += 1
            if i == 1 or i % 50 == 0 or i == len(codes):
                print(f"index_weight [{i}/{len(codes)}] code={code} rows+={n} total={total}")

        self.client.execute("OPTIMIZE TABLE quantaxis.index_weight")
        stats = self.client.execute(
            "SELECT count(), uniqExact(index_code), min(date), max(date) "
            "FROM quantaxis.index_weight"
        )[0]
        info = {
            "table": "index_weight",
            "codes_ok": ok,
            "codes_total": len(codes),
            "rows": stats[0],
            "indexes": stats[1],
            "date_min": stats[2],
            "date_max": stats[3],
        }
        print(f"save_index_weight done {info}")
        return info

    def save_index_components(
        self,
        start_date: str,
        end_date: str,
        codes: Optional[list[str]] = None,
        *,
        rebuild: bool = True,
        limit: int = 0,
    ) -> dict:
        """从 RQData ``index_components`` 拉取并写入 ``quantaxis.index_components``。"""

        if index_components is None:
            raise RuntimeError("index_components 未绑定，请先 init_rqdatac()")

        if codes is None:
            codes = self._load_codes_from_table(
                "index_cn_codelist", status_active_only=True, limit=limit
            )
        else:
            codes = [str(c) for c in codes]
            if limit > 0:
                codes = codes[:limit]
        if not codes:
            raise RuntimeError("无可用指数代码（index_cn_codelist / codes）")

        if rebuild:
            self.client.execute("DROP TABLE IF EXISTS quantaxis.index_components")
        self.client.execute(
            """
            CREATE TABLE IF NOT EXISTS quantaxis.index_components (
                date Date,
                index_code String,
                order_book_id String
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (date, index_code, order_book_id)
            SETTINGS index_granularity=8192
            """
        )

        total = 0
        ok = 0
        for i, code in enumerate(codes, 1):
            try:
                part = index_components(
                    code, start_date=start_date, end_date=end_date, market="cn"
                )
            except Exception as exc:  # noqa: BLE001
                print(f"index_components fail {code}: {exc}")
                continue
            if part is None or part.empty:
                continue
            frame = part.reset_index(drop=True).copy()
            if "date" not in frame.columns:
                frame["date"] = end_date
            if "order_book_id" not in frame.columns:
                # 极少数返回仅一列无名 list 时兜底
                frame.columns = ["order_book_id"][: len(frame.columns)]
            frame["index_code"] = code
            frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
            frame["order_book_id"] = frame["order_book_id"].astype(str)
            frame = frame.dropna(subset=["date", "order_book_id"])
            n = self._insert_frame(
                "index_components",
                frame,
                ["date", "index_code", "order_book_id"],
            )
            total += n
            ok += 1
            if i == 1 or i % 50 == 0 or i == len(codes):
                print(
                    f"index_components [{i}/{len(codes)}] code={code} "
                    f"rows+={n} total={total}"
                )

        self.client.execute("OPTIMIZE TABLE quantaxis.index_components")
        stats = self.client.execute(
            "SELECT count(), uniqExact(index_code), min(date), max(date) "
            "FROM quantaxis.index_components"
        )[0]
        info = {
            "table": "index_components",
            "codes_ok": ok,
            "codes_total": len(codes),
            "rows": stats[0],
            "indexes": stats[1],
            "date_min": stats[2],
            "date_max": stats[3],
        }
        print(f"save_index_components done {info}")
        return info

    def save_etf_components(
        self,
        dates: Optional[list[str]] = None,
        codes: Optional[list[str]] = None,
        *,
        rebuild: bool = True,
        limit: int = 0,
    ) -> dict:
        """从 RQData ``etf.get_components`` 拉取并写入 ``quantaxis.etf_components``。

        表结构对齐 QAClickhouse.get_etf_components。
        部分账号对 etf.get_components / fund.get_etf_components_v2 无权限（HTTP 403）。
        """

        if etf is None and fund is None:
            raise RuntimeError("etf/fund API 未绑定，请先 init_rqdatac()")

        if codes is None:
            codes = self._load_codes_from_table(
                "etf_cn_codelist", status_active_only=True, limit=limit
            )
        else:
            codes = [str(c) for c in codes]
            if limit > 0:
                codes = codes[:limit]
        if not codes:
            raise RuntimeError("无可用 ETF 代码（etf_cn_codelist / codes）")

        if not dates:
            # 默认取最近一个可用交易日锚点
            today = datetime.date.today()
            dates = [str(today - datetime.timedelta(days=d)) for d in range(1, 10)]

        if rebuild:
            self.client.execute("DROP TABLE IF EXISTS quantaxis.etf_components")
        self.client.execute(
            """
            CREATE TABLE IF NOT EXISTS quantaxis.etf_components (
                trading_date Date,
                order_book_id String,
                stock_code String,
                stock_amount Float64,
                cash_substitute String,
                cash_substitute_proportion Float64,
                fixed_cash_substitute Float64
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (trading_date, order_book_id, stock_code)
            SETTINGS index_granularity=8192
            """
        )

        columns = [
            "trading_date",
            "order_book_id",
            "stock_code",
            "stock_amount",
            "cash_substitute",
            "cash_substitute_proportion",
            "fixed_cash_substitute",
        ]
        total = 0
        ok = 0
        last_error = ""
        for date in dates:
            for i, code in enumerate(codes, 1):
                try:
                    if etf is not None and hasattr(etf, "get_components"):
                        part = etf.get_components(code, date=date, market="cn")
                    else:
                        part = fund.get_etf_components(code)
                except Exception as exc:  # noqa: BLE001
                    last_error = str(exc)
                    # 无权限时不必对每个代码重复刷屏
                    if "403" in last_error or "Not permit" in last_error:
                        print(f"etf.get_components not permitted: {last_error}")
                        self.client.execute("OPTIMIZE TABLE quantaxis.etf_components")
                        info = {
                            "table": "etf_components",
                            "rows": 0,
                            "error": last_error,
                            "codes_total": len(codes),
                        }
                        print(f"save_etf_components aborted {info}")
                        return info
                    continue
                if part is None or part.empty:
                    continue
                frame = part.reset_index(drop=True).copy()
                # 文档: order_book_id=ETF, stock_code=成分股；若缺 stock_code 则互换兜底
                if "stock_code" not in frame.columns:
                    if "order_book_id" in frame.columns:
                        frame = frame.rename(columns={"order_book_id": "stock_code"})
                    else:
                        continue
                frame["order_book_id"] = code
                if "trading_date" not in frame.columns:
                    frame["trading_date"] = date
                frame["trading_date"] = pd.to_datetime(
                    frame["trading_date"], errors="coerce"
                ).dt.date
                frame["stock_code"] = frame["stock_code"].astype(str)
                frame["stock_amount"] = pd.to_numeric(
                    frame["stock_amount"] if "stock_amount" in frame.columns else 0.0,
                    errors="coerce",
                ).fillna(0.0)
                if "cash_substitute" not in frame.columns:
                    frame["cash_substitute"] = ""
                frame["cash_substitute"] = frame["cash_substitute"].fillna("").astype(str)
                frame["cash_substitute_proportion"] = pd.to_numeric(
                    frame["cash_substitute_proportion"]
                    if "cash_substitute_proportion" in frame.columns
                    else 0.0,
                    errors="coerce",
                ).fillna(0.0)
                frame["fixed_cash_substitute"] = pd.to_numeric(
                    frame["fixed_cash_substitute"]
                    if "fixed_cash_substitute" in frame.columns
                    else 0.0,
                    errors="coerce",
                ).fillna(0.0)
                frame = frame.dropna(subset=["trading_date", "stock_code"])
                n = self._insert_frame("etf_components", frame, columns)
                total += n
                ok += 1
                if ok <= 3 or i % 100 == 0:
                    print(
                        f"etf_components date={date} [{i}/{len(codes)}] "
                        f"code={code} rows+={n} total={total}"
                    )

        self.client.execute("OPTIMIZE TABLE quantaxis.etf_components")
        stats = self.client.execute(
            "SELECT count(), uniqExact(order_book_id), min(trading_date), max(trading_date) "
            "FROM quantaxis.etf_components"
        )[0]
        info = {
            "table": "etf_components",
            "codes_ok": ok,
            "codes_total": len(codes),
            "rows": stats[0],
            "etfs": stats[1],
            "date_min": stats[2],
            "date_max": stats[3],
            "last_error": last_error,
        }
        print(f"save_etf_components done {info}")
        return info

    def save_citis_industry(
        self,
        dates: Optional[list[str]] = None,
        codes: Optional[list[str]] = None,
        *,
        source: str = "citics_2019",
        rebuild: bool = True,
        level: str = "third",
        limit: int = 0,
    ) -> dict:
        """写入 ``quantaxis.citis_industry``，供 QAClickhouse.get_stock_industry 使用。

        主数据来自 RQData ``get_industry()``：
        1. 用 ``get_industry_mapping`` 枚举中信行业；
        2. 对每个行业调用 ``get_industry(industry, source=citics_2019, date=...)``
           取得成分股，展开为 date × order_book_id 行写入 ClickHouse。

        Args:
            level: ``first`` / ``second`` / ``third``。默认 ``third``，可同时写入
                一/二/三级行业代码与名称；``first``/``second`` 时下级字段置空。
        """

        if get_industry is None:
            raise RuntimeError("get_industry 未绑定，请先 init_rqdatac()")
        if get_industry_mapping is None:
            raise RuntimeError("get_industry_mapping 未绑定，请先 init_rqdatac()")

        mapping = get_industry_mapping(source=source, market="cn")
        if mapping is None or mapping.empty:
            raise RuntimeError(f"get_industry_mapping({source}) 无数据")

        level = str(level).lower()
        level_map = {
            "first": ("first_industry_code", "first_industry_name"),
            "second": ("second_industry_code", "second_industry_name"),
            "third": ("third_industry_code", "third_industry_name"),
        }
        if level not in level_map:
            raise ValueError(f"不支持的 level={level!r}，可选 {list(level_map)}")
        code_col, name_col = level_map[level]
        industries = (
            mapping.loc[:, [
                "first_industry_code", "first_industry_name",
                "second_industry_code", "second_industry_name",
                "third_industry_code", "third_industry_name",
            ]]
            .drop_duplicates(subset=[code_col])
            .dropna(subset=[code_col])
            .copy()
        )
        industries[code_col] = industries[code_col].astype(str)
        if limit > 0:
            industries = industries.head(limit).copy()
        print(
            f"citis_industry via get_industry source={source} level={level} "
            f"industries={len(industries)} mapping_rows={len(mapping)}"
        )

        code_filter: Optional[set[str]] = None
        if codes is not None:
            code_filter = {str(c) for c in codes}

        if not dates:
            dates = [str(datetime.date.today())]

        if rebuild:
            self.client.execute("DROP TABLE IF EXISTS quantaxis.citis_industry")
        self.client.execute(
            """
            CREATE TABLE IF NOT EXISTS quantaxis.citis_industry (
                date Date,
                order_book_id String,
                first_industry_code String,
                first_industry_name String,
                second_industry_code String,
                second_industry_name String,
                third_industry_code String,
                third_industry_name String
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (date, order_book_id)
            SETTINGS index_granularity=8192
            """
        )

        columns = [
            "date",
            "order_book_id",
            "first_industry_code",
            "first_industry_name",
            "second_industry_code",
            "second_industry_name",
            "third_industry_code",
            "third_industry_name",
        ]
        total = 0
        for date in dates:
            frames: list[pd.DataFrame] = []
            for i, row in enumerate(industries.itertuples(index=False), 1):
                industry_key = str(getattr(row, code_col))
                try:
                    part = get_industry(
                        industry_key, source=source, date=date, market="cn"
                    )
                except Exception as exc:  # noqa: BLE001
                    print(f"get_industry fail {industry_key} date={date}: {exc}")
                    continue
                if part is None or part.empty:
                    continue
                frame = part.reset_index(drop=True).copy()
                if "order_book_id" not in frame.columns:
                    continue
                frame["order_book_id"] = frame["order_book_id"].astype(str)
                if code_filter is not None:
                    frame = frame[frame["order_book_id"].isin(code_filter)]
                if frame.empty:
                    continue
                frame["date"] = pd.to_datetime(date).date()
                frame["first_industry_code"] = str(getattr(row, "first_industry_code", "") or "")
                frame["first_industry_name"] = str(getattr(row, "first_industry_name", "") or "")
                frame["second_industry_code"] = str(getattr(row, "second_industry_code", "") or "")
                frame["second_industry_name"] = str(getattr(row, "second_industry_name", "") or "")
                frame["third_industry_code"] = str(getattr(row, "third_industry_code", "") or "")
                frame["third_industry_name"] = str(getattr(row, "third_industry_name", "") or "")
                # 按一级遍历时清空二/三级，避免把 mapping 里任意一行的下级误挂到全行业
                if level == "first":
                    frame["second_industry_code"] = ""
                    frame["second_industry_name"] = ""
                    frame["third_industry_code"] = ""
                    frame["third_industry_name"] = ""
                elif level == "second":
                    frame["third_industry_code"] = ""
                    frame["third_industry_name"] = ""
                frames.append(frame.loc[:, columns])
                if i == 1 or i % 20 == 0 or i == len(industries):
                    print(
                        f"get_industry [{i}/{len(industries)}] "
                        f"{industry_key}/{getattr(row, name_col)} "
                        f"date={date} stocks={len(frame)}"
                    )

            if not frames:
                continue
            day = pd.concat(frames, ignore_index=True)
            # 同一股票同日若落入多个下级行业，保留最后一条
            day = day.drop_duplicates(["date", "order_book_id"], keep="last")
            n = self._insert_frame("citis_industry", day, columns)
            total += n
            print(f"citis_industry date={date} inserted={n} total={total}")

        self.client.execute("OPTIMIZE TABLE quantaxis.citis_industry")
        stats = self.client.execute(
            "SELECT count(), uniqExact(order_book_id), min(date), max(date) "
            "FROM quantaxis.citis_industry"
        )[0]
        info = {
            "table": "citis_industry",
            "mapping_rows": len(mapping),
            "industries": len(industries),
            "rows": stats[0],
            "codes": stats[1],
            "date_min": stats[2],
            "date_max": stats[3],
            "source": source,
            "level": level,
        }
        print(f"save_citis_industry done {info}")
        return info

    def save_list(self, *, rebuild: bool = True):
        """将 RQData all_instruments（self.codelist）写入 quantaxis.{market}_codelist。

        支持 index_cn / etf_cn / future_cn / option_cn / stock_cn 等；
        股票也可用更专用的 save_codelist。
        """

        frame = self.codelist.copy()
        if frame is None or frame.empty:
            raise RuntimeError(f'{self.market}: get_list/codelist 为空，无法入库')

        table = f'{self.market}_codelist'
        full_table = f'quantaxis.{table}'

        # 统一缺省与类型，便于生成 ClickHouse DDL
        for column in frame.columns:
            series = frame[column]
            if pd.api.types.is_bool_dtype(series):
                frame[column] = series.fillna(False).astype(bool)
            elif pd.api.types.is_integer_dtype(series):
                frame[column] = pd.to_numeric(series, errors='coerce').fillna(0).astype('int64')
            elif pd.api.types.is_float_dtype(series):
                frame[column] = pd.to_numeric(series, errors='coerce').fillna(0.0)
            else:
                frame[column] = series.fillna('').astype(str)

        def ch_type(column: str) -> str:
            series = frame[column]
            if pd.api.types.is_bool_dtype(series):
                return 'Boolean'
            if pd.api.types.is_integer_dtype(series):
                return 'Int64'
            if pd.api.types.is_float_dtype(series):
                return 'Float64'
            return 'String'

        columns = list(frame.columns)
        if 'order_book_id' not in columns:
            raise RuntimeError(f'{self.market}: 缺少 order_book_id 列')

        if rebuild:
            self.client.execute(f'DROP TABLE IF EXISTS {full_table}')

        col_defs = ',\n'.join(f'                `{c}` {ch_type(c)}' for c in columns)
        order_cols = (
            '(listed_date, order_book_id)'
            if 'listed_date' in columns
            else '(order_book_id)'
        )
        ddl = f"""CREATE TABLE IF NOT EXISTS `{full_table.replace('.', '`.`')}` (
{col_defs}
                )
                ENGINE = ReplacingMergeTree()
                ORDER BY {order_cols}
                SETTINGS index_granularity=8192"""
        self.client.execute(ddl)

        records = frame.loc[:, columns].to_dict('records')
        sql = (
            f'INSERT INTO {full_table} ('
            + ', '.join(columns)
            + ') VALUES'
        )
        chunk = 2000
        for i in range(0, len(records), chunk):
            part = [tuple(row[c] for c in columns) for row in records[i:i + chunk]]
            self.client.execute(sql, part)
            if i == 0 or i // chunk % 20 == 0:
                print(f'{table}: inserted {min(i + chunk, len(records))}/{len(records)}')

        self.client.execute(f'OPTIMIZE TABLE {full_table}')
        stats = self.client.execute(
            f'SELECT count(), uniqExact(order_book_id) FROM {full_table}'
        )[0]
        print(f'save_list done table={table} rows={stats[0]} codes={stats[1]}')
        return {'table': table, 'rows': stats[0], 'codes': stats[1]}

    def quit(self):
        self.client.disconnect()
