# ruff: noqa: N999
"""同花顺 QuantAPI HTTP 数据源。

本模块只封装 QASU 保存流程需要的 HTTP 接口。指标和报表 ID 与账号
权限相关，未配置的查询会明确失败，避免发送猜测请求。
"""

from __future__ import annotations

import datetime
import json
import os
from collections.abc import Iterable, Mapping, Sequence
from configparser import ConfigParser
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from threading import Lock
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from requests import Response, Session

from QUANTAXIS.QAUtil import (
    QASETTING,
    QA_util_date_stamp,
    QA_util_time_stamp,
)

JsonObject = dict[str, object]
CHINA_TZ = ZoneInfo("Asia/Shanghai")


class THSHttpError(RuntimeError):
    """表示同花顺 HTTP 请求或业务响应失败。"""


class THSConfigError(ValueError):
    """表示查询所需的 token、指标或报表配置缺失。"""


class THSAsset(str, Enum):
    """QASU 支持的同花顺资产类型。"""

    STOCK = "stock"
    INDEX = "index"
    ETF = "etf"
    FUTURE = "future"
    OPTION = "option"
    BOND = "bond"


@dataclass(frozen=True)
class THSUniverse:
    """定义通过 data_pool 获取资产清单所需的参数。

    Attributes:
        report_name: 同花顺专题报表 ID。
        block_name: 同花顺板块 ID。
        output: 报表输出字段。
        code_field: 响应中证券代码字段。
        extra_params: 传给 ``functionpara`` 的额外参数。
    """

    report_name: str
    block_name: str
    output: str
    code_field: str
    extra_params: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class THSIndicator:
    """定义基础数据指标及其参数。"""

    name: str
    params: tuple[str, ...] = ()


@dataclass(frozen=True)
class THSQueryProfile:
    """定义某资产类型在 QASU 中使用的同花顺查询参数。"""

    asset: THSAsset
    universe: THSUniverse | None = None
    day_indicators: tuple[str, ...] = (
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
    )
    minute_indicators: tuple[str, ...] = (
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
    )
    transaction_indicators: tuple[str, ...] = ()
    info_indicators: tuple[THSIndicator, ...] = ()
    xdxr_indicators: tuple[THSIndicator, ...] = ()
    block_universes: tuple[THSUniverse, ...] = ()


STOCK_UNIVERSE = THSUniverse(
    report_name="p03425",
    block_name="001005010",
    output="p03291_f001,p03291_f002,p03291_f003,p03291_f004",
    code_field="p03291_f002",
    extra_params={"iv_type": "allcontract"},
)


# 用户可在取得 SuperCommand 生成的指标/报表 ID 后，通过
# register_query_profile() 替换对应配置。空配置不会静默退回其他数据源。
THS_QUERY_PROFILES: dict[THSAsset, THSQueryProfile] = {
    THSAsset.STOCK: THSQueryProfile(
        asset=THSAsset.STOCK,
        universe=STOCK_UNIVERSE,
    ),
    THSAsset.INDEX: THSQueryProfile(asset=THSAsset.INDEX),
    THSAsset.ETF: THSQueryProfile(asset=THSAsset.ETF),
    THSAsset.FUTURE: THSQueryProfile(asset=THSAsset.FUTURE),
    THSAsset.OPTION: THSQueryProfile(asset=THSAsset.OPTION),
    THSAsset.BOND: THSQueryProfile(asset=THSAsset.BOND),
}


def register_query_profile(profile: THSQueryProfile) -> None:
    """注册或替换资产查询配置。

    Args:
        profile: 已由 iFinD/SuperCommand 验证的查询配置。
    """

    THS_QUERY_PROFILES[profile.asset] = profile


def get_query_profile(asset: THSAsset) -> THSQueryProfile:
    """取得资产查询配置。

    Args:
        asset: 资产类型。

    Returns:
        对应的查询配置。
    """

    return THS_QUERY_PROFILES[asset]


@dataclass(frozen=True)
class THSHttpConfig:
    """同花顺 HTTP 客户端配置。"""

    refresh_token: str
    base_url: str = "https://quantapi.51ifind.com/api/v1"
    timeout: float = 30.0
    language: str = "cn"


class THSHttpClient:
    """同花顺 QuantAPI HTTP 客户端。"""

    def __init__(
        self,
        config: THSHttpConfig,
        session: Session | None = None,
    ) -> None:
        """初始化客户端。

        Args:
            config: HTTP 与凭证配置。
            session: 可注入的 requests 会话，主要用于测试。
        """

        if not config.refresh_token.strip():
            raise THSConfigError(
                "请设置环境变量 THS_REFRESH_TOKEN，或配置 [THS] refresh_token"
            )
        self.config = config
        self.session = session or requests.Session()
        self._access_token: str | None = None
        self._token_lock = Lock()

    def get_access_token(self, force_refresh: bool = False) -> str:
        """获取并缓存 access token。

        Args:
            force_refresh: 是否忽略缓存并重新获取。

        Returns:
            可用于业务请求的 access token。
        """

        with self._token_lock:
            if self._access_token and not force_refresh:
                return self._access_token
            headers = {
                "Content-Type": "application/json",
                "refresh_token": self.config.refresh_token,
            }
            response = self.session.post(
                self._url("get_access_token"),
                headers=headers,
                timeout=self.config.timeout,
            )
            payload = self._decode_response(response)
            data = payload.get("data")
            if not isinstance(data, dict):
                raise THSHttpError("同花顺鉴权响应缺少 data")
            access_token = data.get("access_token")
            if not isinstance(access_token, str) or not access_token:
                raise THSHttpError("同花顺鉴权响应缺少 access_token")
            self._access_token = access_token
            return access_token

    def basic_data(
        self,
        codes: Sequence[str],
        indicators: Sequence[THSIndicator],
    ) -> JsonObject:
        """调用基础数据接口。"""

        if not indicators:
            raise THSConfigError("基础数据指标未配置")
        indicator_payload: list[JsonObject] = []
        for indicator in indicators:
            item: JsonObject = {"indicator": indicator.name}
            if indicator.params:
                item["indiparams"] = list(indicator.params)
            indicator_payload.append(item)
        return self._post(
            "basic_data_service",
            {
                "codes": ",".join(format_ths_code(code) for code in codes),
                "indipara": indicator_payload,
            },
        )

    def history(
        self,
        codes: Sequence[str],
        indicators: Sequence[str],
        start: str,
        end: str,
        function_params: Mapping[str, str] | None = None,
    ) -> JsonObject:
        """调用历史行情接口。"""

        payload: JsonObject = {
            "codes": ",".join(format_ths_code(code) for code in codes),
            "indicators": ",".join(indicators),
            "startdate": str(start)[:10],
            "enddate": str(end)[:10],
            "functionpara": dict(function_params or {}),
        }
        return self._post("cmd_history_quotation", payload)

    def high_frequency(
        self,
        codes: Sequence[str],
        indicators: Sequence[str],
        start: str,
        end: str,
        function_params: Mapping[str, str] | None = None,
    ) -> JsonObject:
        """调用高频序列接口。"""

        payload: JsonObject = {
            "codes": ",".join(format_ths_code(code) for code in codes),
            "indicators": ",".join(indicators),
            "starttime": _normalize_ths_datetime(start, end_of_day=False),
            "endtime": _normalize_ths_datetime(end, end_of_day=True),
        }
        if function_params:
            payload["functionpara"] = dict(function_params)
        return self._post("high_frequency", payload)

    def data_pool(
        self,
        universe: THSUniverse,
        date: str | None = None,
    ) -> JsonObject:
        """调用专题报表接口。"""

        query_date = (
            date or datetime.datetime.now(CHINA_TZ).date().isoformat()
        ).replace("-", "")
        function_params = {
            "date": query_date,
            "blockname": universe.block_name,
            **dict(universe.extra_params),
        }
        return self._post(
            "data_pool",
            {
                "reportname": universe.report_name,
                "functionpara": function_params,
                "outputpara": universe.output,
            },
        )

    def _post(
        self,
        endpoint: str,
        payload: JsonObject,
        retry_auth: bool = True,
    ) -> JsonObject:
        """发送业务请求，并在认证失败时刷新一次 token。"""

        headers = {
            "Content-Type": "application/json",
            "access_token": self.get_access_token(),
            "ifindlang": self.config.language,
        }
        response = self.session.post(
            self._url(endpoint),
            data=json.dumps(payload, ensure_ascii=False),
            headers=headers,
            timeout=self.config.timeout,
        )
        if response.status_code in (401, 403) and retry_auth:
            self.get_access_token(force_refresh=True)
            return self._post(endpoint, payload, retry_auth=False)
        result = self._decode_response(response)
        error_code = result.get("errorcode", 0)
        if error_code not in (0, "0", None):
            message = result.get("errmsg", "未知错误")
            raise THSHttpError(f"同花顺接口 {endpoint} 失败: {error_code} {message}")
        return result

    def _url(self, endpoint: str) -> str:
        return f"{self.config.base_url.rstrip('/')}/{endpoint}"

    @staticmethod
    def _decode_response(response: Response) -> JsonObject:
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise THSHttpError("同花顺接口返回的 JSON 不是对象")
        return dict(payload)


_client: THSHttpClient | None = None


def set_refresh_token(token: str, persist: bool = True) -> None:
    """设置 refresh token 并重建默认客户端。

    Args:
        token: 同花顺 refresh token。
        persist: 是否写入 QASETTING 的 THS 配置段。
    """

    global _client
    if not token.strip():
        raise THSConfigError("refresh token 不能为空")
    if persist:
        QASETTING.set_config("THS", "refresh_token", token)
    _client = THSHttpClient(_load_config(token))


def get_client() -> THSHttpClient:
    """获取模块级默认客户端。"""

    global _client
    if _client is None:
        _client = THSHttpClient(_load_config())
    return _client


def _load_config(token: str | None = None) -> THSHttpConfig:
    local_config = _load_local_config()
    refresh_token = token or os.getenv("THS_REFRESH_TOKEN")
    if not refresh_token:
        configured = QASETTING.get_config(
            "THS",
            "refresh_token",
            default_value="",
        )
        refresh_token = configured if isinstance(configured, str) else ""
    if not refresh_token:
        refresh_token = local_config.get("refresh_token", "")
    base_url = os.getenv("THS_BASE_URL") or QASETTING.get_config(
        "THS",
        "base_url",
        default_value=local_config.get(
            "base_url",
            "https://quantapi.51ifind.com/api/v1",
        ),
    )
    timeout_value = os.getenv("THS_TIMEOUT") or local_config.get("timeout", "30")
    language = os.getenv("THS_LANGUAGE") or local_config.get("language", "cn")
    try:
        timeout = float(timeout_value)
    except ValueError as error:
        raise THSConfigError("THS timeout 必须为数值") from error
    return THSHttpConfig(
        refresh_token=refresh_token,
        base_url=str(base_url),
        timeout=timeout,
        language=language,
    )


def _load_local_config() -> dict[str, str]:
    """读取项目本地且不纳入版本控制的 THS 配置文件。"""

    default_path = (
        Path(__file__).resolve().parents[2] / "config" / "ths_http.ini"
    )
    config_path = Path(os.getenv("THS_CONFIG_FILE", str(default_path)))
    if not config_path.exists():
        return {}
    parser = ConfigParser()
    parser.read(config_path, encoding="utf-8")
    if not parser.has_section("THS"):
        raise THSConfigError(f"{config_path} 缺少 [THS] 配置段")
    return {key: value.strip() for key, value in parser.items("THS")}


def _normalize_ths_datetime(value: str, *, end_of_day: bool = False) -> str:
    """将日期或时间字符串规范为同花顺高频接口要求的 ``YYYY-MM-DD HH:MM:SS``。"""

    text = str(value).strip().replace("T", " ")
    if len(text) == 10:
        return f"{text} {'23:59:59' if end_of_day else '00:00:00'}"
    if len(text) == 16:
        return f"{text}:00"
    return text[:19]


def format_ths_code(
    code: str,
    asset: THSAsset = THSAsset.STOCK,
) -> str:
    """将六位证券代码转换为同花顺代码。

    已带市场后缀的代码保持不变。无法可靠推断的期货、期权代码必须由
    调用方传入后缀。
    """

    normalized = str(code).strip().upper()
    if "." in normalized:
        return normalized
    if asset in {THSAsset.FUTURE, THSAsset.OPTION, THSAsset.BOND}:
        raise THSConfigError(f"{asset.value} 代码 {code!r} 必须包含同花顺市场后缀")
    if len(normalized) != 6 or not normalized.isdigit():
        raise THSConfigError(f"代码 {code!r} 缺少同花顺市场后缀")
    if asset is THSAsset.INDEX:
        if normalized.startswith("899"):
            return f"{normalized}.BJ"
        if normalized.startswith("399"):
            return f"{normalized}.SZ"
        return f"{normalized}.SH"
    if asset is THSAsset.ETF:
        suffix = "SH" if normalized.startswith("5") else "SZ"
        return f"{normalized}.{suffix}"
    if normalized[:2] in {"43", "83", "87", "92"}:
        return f"{normalized}.BJ"
    if normalized[0] in {"5", "6", "9"}:
        return f"{normalized}.SH"
    return f"{normalized}.SZ"


def response_to_frame(response: Mapping[str, object]) -> pd.DataFrame:
    """将同花顺 ``tables`` 响应转换为 DataFrame。"""

    tables = response.get("tables")
    if tables is None:
        data = response.get("data")
        if isinstance(data, dict):
            return pd.DataFrame(data)
        if isinstance(data, list):
            return pd.DataFrame(data)
        return pd.DataFrame()

    table_items: Iterable[object]
    if isinstance(tables, list):
        table_items = tables
    elif isinstance(tables, dict):
        table_items = [tables]
    else:
        raise THSHttpError("同花顺 tables 字段格式不受支持")

    frames: list[pd.DataFrame] = []
    for raw_item in table_items:
        if not isinstance(raw_item, dict):
            continue
        table = raw_item.get("table")
        if isinstance(table, dict):
            if not table:
                frame = pd.DataFrame()
            elif all(
                not isinstance(value, (list, tuple)) for value in table.values()
            ):
                # 空区间时同花顺常返回全是标量/空值的 table，不能直接 DataFrame(dict)。
                if all(
                    value is None or value == "" or value == []
                    for value in table.values()
                ):
                    frame = pd.DataFrame()
                else:
                    frame = pd.DataFrame([table])
            else:
                frame = pd.DataFrame(table)
        else:
            list_fields = {
                str(key): value
                for key, value in raw_item.items()
                if isinstance(value, list)
            }
            frame = (
                pd.DataFrame(list_fields) if list_fields else pd.DataFrame([raw_item])
            )
        time_values = raw_item.get("time")
        if (
            "time" not in frame.columns
            and isinstance(time_values, list)
            and len(time_values) == len(frame)
        ):
            frame.insert(0, "time", time_values)
        ths_code = raw_item.get("thscode")
        if "thscode" not in frame.columns and isinstance(ths_code, str):
            frame.insert(0, "thscode", ths_code)
        frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _profile_with_universe(asset: THSAsset) -> THSQueryProfile:
    profile = get_query_profile(asset)
    if profile.universe is None:
        raise THSConfigError(f"{asset.value} 清单报表未配置，请注册 THSQueryProfile")
    return profile


def QA_fetch_get_asset_list(
    asset: THSAsset,
    date: str | None = None,
) -> pd.DataFrame:
    """获取指定资产的代码清单。"""

    profile = _profile_with_universe(asset)
    universe = profile.universe
    if universe is None:
        raise THSConfigError(f"{asset.value} 清单报表未配置")
    frame = response_to_frame(get_client().data_pool(universe, date=date))
    if universe.code_field not in frame.columns:
        raise THSHttpError(f"清单响应缺少代码字段 {universe.code_field}")
    result = frame.copy()
    result["ths_code"] = result[universe.code_field].astype(str)
    result["code"] = result["ths_code"].str.split(".").str[0]
    result["type"] = asset.value
    return result.drop_duplicates(subset=["ths_code"])


def _normalize_market_frame(
    response: Mapping[str, object],
    code: str,
    data_type: str,
) -> pd.DataFrame:
    frame = response_to_frame(response)
    if frame.empty:
        return frame
    result = frame.copy()
    result.columns = [str(column).lower() for column in result.columns]
    if "thscode" in result.columns:
        result["code"] = result["thscode"].astype(str).str.split(".").str[0]
    else:
        result["code"] = str(code).split(".")[0]
    if "time" in result.columns:
        result["datetime"] = pd.to_datetime(result["time"], errors="coerce")
    elif "date" in result.columns:
        result["datetime"] = pd.to_datetime(result["date"], errors="coerce")
    else:
        raise THSHttpError("行情响应缺少 time/date 字段")
    result["date"] = result["datetime"].dt.strftime("%Y-%m-%d")
    result["date_stamp"] = result["date"].map(QA_util_date_stamp)
    result["time_stamp"] = result["datetime"].map(QA_util_time_stamp)
    result["type"] = data_type
    return result.set_index("datetime", drop=False)


def QA_fetch_get_asset_day(
    code: str,
    start: str,
    end: str,
    asset: THSAsset,
    if_fq: str = "00",
) -> pd.DataFrame:
    """获取指定资产的日线数据。"""

    profile = get_query_profile(asset)
    cps_map = {
        "00": "no",
        "bfq": "no",
        "01": "forward1",
        "qfq": "forward1",
        "02": "backward1",
        "hfq": "backward1",
    }
    if if_fq not in cps_map:
        raise THSConfigError(f"不支持的复权类型: {if_fq}")
    ths_code = format_ths_code(code, asset)
    response = get_client().history(
        [ths_code],
        profile.day_indicators,
        start,
        end,
        {"Interval": "D", "Fill": "Blank", "CPS": cps_map[if_fq]},
    )
    return _normalize_market_frame(response, code, "day")


def QA_fetch_get_asset_min(
    code: str,
    start: str,
    end: str,
    asset: THSAsset,
    frequence: str = "1min",
) -> pd.DataFrame:
    """获取指定资产的分钟数据。"""

    profile = get_query_profile(asset)
    interval_map = {
        "1min": "1",
        "3min": "3",
        "5min": "5",
        "10min": "10",
        "15min": "15",
        "30min": "30",
        "60min": "60",
    }
    if frequence not in interval_map:
        raise THSConfigError(f"不支持的分钟频率: {frequence}")
    ths_code = format_ths_code(code, asset)
    response = get_client().high_frequency(
        [ths_code],
        profile.minute_indicators,
        start,
        end,
        {"Interval": interval_map[frequence], "CPS": "no"},
    )
    return _normalize_market_frame(response, code, frequence)


def QA_fetch_get_asset_transaction(
    code: str,
    start: str,
    end: str,
    asset: THSAsset,
) -> pd.DataFrame:
    """获取逐笔数据，指标必须由 SuperCommand 配置。"""

    indicators = get_query_profile(asset).transaction_indicators
    if not indicators:
        raise THSConfigError(f"{asset.value} 逐笔指标未配置，请注册 THSQueryProfile")
    ths_code = format_ths_code(code, asset)
    response = get_client().high_frequency(
        [ths_code],
        indicators,
        start,
        end,
    )
    return _normalize_market_frame(response, code, "transaction")


def QA_fetch_get_stock_info(codes: Sequence[str]) -> pd.DataFrame:
    """获取股票基础信息。"""

    indicators = get_query_profile(THSAsset.STOCK).info_indicators
    return response_to_frame(get_client().basic_data(codes, indicators))


def QA_fetch_get_stock_xdxr(codes: Sequence[str]) -> pd.DataFrame:
    """获取股票除权除息数据。"""

    indicators = get_query_profile(THSAsset.STOCK).xdxr_indicators
    return response_to_frame(get_client().basic_data(codes, indicators))


def QA_fetch_get_stock_block(
    date: str | None = None,
) -> pd.DataFrame:
    """获取配置的同花顺板块及成分。"""

    universes = get_query_profile(THSAsset.STOCK).block_universes
    if not universes:
        raise THSConfigError(
            "股票板块报表未配置，请在 THSQueryProfile.block_universes 中填写"
        )
    frames: list[pd.DataFrame] = []
    for universe in universes:
        frame = response_to_frame(get_client().data_pool(universe, date))
        if universe.code_field not in frame.columns:
            raise THSHttpError(f"板块响应缺少代码字段 {universe.code_field}")
        frame = frame.copy()
        frame["code"] = frame[universe.code_field].astype(str).str.split(".").str[0]
        frame["blockname"] = universe.block_name
        frame["source"] = "ths_http"
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


def QA_fetch_get_stock_list(date: str | None = None) -> pd.DataFrame:
    """获取股票清单。"""

    return QA_fetch_get_asset_list(THSAsset.STOCK, date)


def QA_fetch_get_index_list(date: str | None = None) -> pd.DataFrame:
    """获取指数清单。"""

    return QA_fetch_get_asset_list(THSAsset.INDEX, date)


def QA_fetch_get_etf_list(date: str | None = None) -> pd.DataFrame:
    """获取 ETF 清单。"""

    return QA_fetch_get_asset_list(THSAsset.ETF, date)


def QA_fetch_get_future_list(date: str | None = None) -> pd.DataFrame:
    """获取期货清单。"""

    return QA_fetch_get_asset_list(THSAsset.FUTURE, date)


def QA_fetch_get_option_contract_list(
    date: str | None = None,
) -> pd.DataFrame:
    """获取期权合约清单。"""

    return QA_fetch_get_asset_list(THSAsset.OPTION, date)


def QA_fetch_get_bond_list(date: str | None = None) -> pd.DataFrame:
    """获取债券清单。"""

    return QA_fetch_get_asset_list(THSAsset.BOND, date)


def QA_fetch_get_stock_day(
    code: str,
    start: str,
    end: str,
    if_fq: str = "00",
) -> pd.DataFrame:
    """获取股票日线。"""

    return QA_fetch_get_asset_day(code, start, end, THSAsset.STOCK, if_fq)


def QA_fetch_get_stock_min(
    code: str,
    start: str,
    end: str,
    frequence: str = "1min",
) -> pd.DataFrame:
    """获取股票分钟线。"""

    return QA_fetch_get_asset_min(code, start, end, THSAsset.STOCK, frequence)


def QA_fetch_get_stock_min_batch(
    codes: Sequence[str],
    start: str,
    end: str,
    frequence: str = "5min",
) -> pd.DataFrame:
    """批量获取股票分钟线（多代码一次 high_frequency）。

    注意：同花顺高频接口单次约 200 万点上限，批量时请控制
    ``codes`` 数量与时间跨度（例如 5min × 十余只 × 数月）。
    """

    interval_map = {
        "1min": "1",
        "3min": "3",
        "5min": "5",
        "10min": "10",
        "15min": "15",
        "30min": "30",
        "60min": "60",
    }
    if frequence not in interval_map:
        raise THSConfigError(f"不支持的分钟频率: {frequence}")
    cleaned = [str(code).strip() for code in codes if str(code).strip()]
    if not cleaned:
        return pd.DataFrame()
    profile = get_query_profile(THSAsset.STOCK)
    ths_codes = [format_ths_code(code, THSAsset.STOCK) for code in cleaned]
    response = get_client().high_frequency(
        ths_codes,
        profile.minute_indicators,
        start,
        end,
        {"Interval": interval_map[frequence], "CPS": "no"},
    )
    # code 仅作缺省；有 thscode 时 _normalize_market_frame 会覆盖
    return _normalize_market_frame(response, cleaned[0], frequence)


def QA_fetch_get_index_day(
    code: str,
    start: str,
    end: str,
) -> pd.DataFrame:
    """获取指数日线。"""

    return QA_fetch_get_asset_day(code, start, end, THSAsset.INDEX)


def QA_fetch_get_index_min(
    code: str,
    start: str,
    end: str,
    frequence: str = "1min",
) -> pd.DataFrame:
    """获取指数分钟线。"""

    return QA_fetch_get_asset_min(code, start, end, THSAsset.INDEX, frequence)


def QA_fetch_get_etf_day(
    code: str,
    start: str,
    end: str,
    if_fq: str = "00",
) -> pd.DataFrame:
    """获取 ETF 日线。"""

    return QA_fetch_get_asset_day(code, start, end, THSAsset.ETF, if_fq)


def QA_fetch_get_etf_min(
    code: str,
    start: str,
    end: str,
    frequence: str = "1min",
) -> pd.DataFrame:
    """获取 ETF 分钟线。"""

    return QA_fetch_get_asset_min(code, start, end, THSAsset.ETF, frequence)


def QA_fetch_get_future_day(
    code: str,
    start: str,
    end: str,
) -> pd.DataFrame:
    """获取期货日线。"""

    return QA_fetch_get_asset_day(code, start, end, THSAsset.FUTURE)


def QA_fetch_get_future_min(
    code: str,
    start: str,
    end: str,
    frequence: str = "1min",
) -> pd.DataFrame:
    """获取期货分钟线。"""

    return QA_fetch_get_asset_min(code, start, end, THSAsset.FUTURE, frequence)


def QA_fetch_get_option_day(
    code: str,
    start: str,
    end: str,
) -> pd.DataFrame:
    """获取期权日线。"""

    return QA_fetch_get_asset_day(code, start, end, THSAsset.OPTION)


def QA_fetch_get_option_min(
    code: str,
    start: str,
    end: str,
    frequence: str = "1min",
) -> pd.DataFrame:
    """获取期权分钟线。"""

    return QA_fetch_get_asset_min(code, start, end, THSAsset.OPTION, frequence)


def QA_fetch_get_bond_day(
    code: str,
    start: str,
    end: str,
) -> pd.DataFrame:
    """获取债券日线。"""

    return QA_fetch_get_asset_day(code, start, end, THSAsset.BOND)


def QA_fetch_get_bond_min(
    code: str,
    start: str,
    end: str,
    frequence: str = "1min",
) -> pd.DataFrame:
    """获取债券分钟线。"""

    return QA_fetch_get_asset_min(code, start, end, THSAsset.BOND, frequence)


def QA_fetch_get_stock_transaction(
    code: str,
    start: str,
    end: str,
) -> pd.DataFrame:
    """获取股票逐笔数据。"""

    return QA_fetch_get_asset_transaction(code, start, end, THSAsset.STOCK)


def QA_fetch_get_index_transaction(
    code: str,
    start: str,
    end: str,
) -> pd.DataFrame:
    """获取指数逐笔数据。"""

    return QA_fetch_get_asset_transaction(code, start, end, THSAsset.INDEX)
