# ruff: noqa: N999
"""米筐 RQData HTTP API 客户端（走 HTTPS:443，不依赖 16011）。

文档: https://www.ricequant.com/doc/rqdata/http/index-rqdatah

鉴权:
  POST https://rqdata.ricequant.com/auth
  {"user_name": "...", "password": "..."}  # license 账号用 user_name=license

取数:
  POST https://rqdata.ricequant.com/api
  Header: token: <auth返回文本>
  Body: {"method": "get_price", ...}
  返回: CSV 文本
"""

from __future__ import annotations

import configparser
import io
import os
from pathlib import Path
from threading import Lock
from typing import Any, Optional, Sequence, Union

import pandas as pd
import requests

JsonObject = dict[str, Any]
Codes = Union[str, Sequence[str]]

DEFAULT_AUTH_URL = "https://rqdata.ricequant.com/auth"
DEFAULT_API_URL = "https://rqdata.ricequant.com/api"
_CONFIG_CANDIDATES = (
    Path(__file__).resolve().parents[2] / "config" / "rqdatac.ini",
    Path.cwd() / "config" / "rqdatac.ini",
)


class RQDataHttpError(RuntimeError):
    """RQData HTTP 请求或业务失败。"""


def _clean_secret(value: str) -> str:
    text = (value or "").strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        text = text[1:-1].strip()
    return text


def _load_ini() -> dict[str, str]:
    for path in _CONFIG_CANDIDATES:
        if not path.is_file():
            continue
        parser = configparser.ConfigParser()
        parser.read(path, encoding="utf-8")
        section = (
            "RQDATA"
            if parser.has_section("RQDATA")
            else (parser.sections()[0] if parser.sections() else None)
        )
        if section is None:
            continue
        return {
            key: _clean_secret(value) for key, value in parser.items(section)
        }
    return {}


def _as_code_list(order_book_ids: Codes) -> list[str]:
    if isinstance(order_book_ids, str):
        return [order_book_ids]
    return [str(item) for item in order_book_ids]


def _parse_csv_response(text: str) -> pd.DataFrame:
    body = (text or "").strip()
    if not body:
        return pd.DataFrame()
    if body.startswith("{") or body.startswith("["):
        # 错误时常返回 JSON/引号字符串
        raise RQDataHttpError(body[:500])
    if body.startswith('"') and body.endswith('"') and "," not in body[:80]:
        raise RQDataHttpError(body.strip('"'))
    frame = pd.read_csv(io.StringIO(body))
    return frame


class RQDataHttpClient:
    """RQData HTTP 会话：自动鉴权、缓存 token、调用 method API。"""

    def __init__(
        self,
        *,
        username: str = "",
        password: str = "",
        auth_url: str = DEFAULT_AUTH_URL,
        api_url: str = DEFAULT_API_URL,
        timeout: float = 60.0,
        session: Optional[requests.Session] = None,
    ) -> None:
        conf = _load_ini()
        self.username = _clean_secret(
            username
            or os.getenv("RQDATAC_USERNAME", "")
            or conf.get("username", "")
        )
        self.password = _clean_secret(
            password
            or os.getenv("RQDATAC_PASSWORD", "")
            or conf.get("password", "")
            or conf.get("token", "")
            or conf.get("license", "")
        )
        if not self.username and (
            conf.get("license") or conf.get("token") or self.password
        ):
            self.username = "license"
        self.auth_url = (
            os.getenv("RQDATA_HTTP_AUTH_URL", "").strip()
            or conf.get("auth_url", "")
            or auth_url
        )
        self.api_url = (
            os.getenv("RQDATA_HTTP_API_URL", "").strip()
            or conf.get("api_url", "")
            or api_url
        )
        self.timeout = float(os.getenv("RQDATAC_TIMEOUT", timeout))
        self._session = session or requests.Session()
        self._token = ""
        self._lock = Lock()

    def ensure_credentials(self) -> None:
        if not self.username or not self.password:
            raise RQDataHttpError(
                "RQData HTTP 未配置凭证。请在 config/rqdatac.ini 填写 "
                "license/token，或 username+password；"
                "也可设置 RQDATAC_USERNAME / RQDATAC_PASSWORD。"
            )

    def authenticate(self, force: bool = False) -> str:
        """获取/刷新 HTTP token（约 2 小时有效）。"""

        with self._lock:
            if self._token and not force:
                return self._token
            self.ensure_credentials()
            response = self._session.post(
                self.auth_url,
                json={
                    "user_name": self.username,
                    "password": self.password,
                },
                timeout=self.timeout,
            )
            if response.status_code >= 400:
                raise RQDataHttpError(
                    f"鉴权失败 HTTP {response.status_code}: "
                    f"{response.text[:300]}"
                )
            token = response.text.strip().strip('"')
            if not token or token.lower().startswith("invalid"):
                raise RQDataHttpError(f"鉴权失败: {response.text[:300]}")
            self._token = token
            return token

    def call(self, method: str, **params: Any) -> pd.DataFrame:
        """调用与 Python API 同名的 method，返回 DataFrame（CSV 解析）。"""

        token = self.authenticate()
        payload: JsonObject = {"method": method, **params}
        response = self._session.post(
            self.api_url,
            json=payload,
            headers={"token": token, "Content-Type": "application/json"},
            timeout=self.timeout,
        )
        # token 失效时自动重试一次
        if response.status_code in {401, 403} or (
            response.status_code >= 400
            and "token" in response.text.lower()
        ):
            token = self.authenticate(force=True)
            response = self._session.post(
                self.api_url,
                json=payload,
                headers={"token": token, "Content-Type": "application/json"},
                timeout=self.timeout,
            )
        if response.status_code >= 400:
            raise RQDataHttpError(
                f"{method} 失败 HTTP {response.status_code}: "
                f"{response.text[:500]}"
            )
        return _parse_csv_response(response.text)

    def get_quota(self) -> dict[str, Any]:
        frame = self.call("user.get_quota")
        if frame.empty or not {"name", "value"}.issubset(frame.columns):
            return {}
        return {
            str(row["name"]): row["value"]
            for row in frame.to_dict("records")
        }

    def all_instruments(
        self,
        type: Optional[str] = None,
        date: Optional[object] = None,
        market: str = "cn",
        **kwargs: Any,
    ) -> pd.DataFrame:
        params: JsonObject = {"market": market, **kwargs}
        if type is not None:
            params["type"] = type
        if date is not None:
            params["date"] = date
        return self.call("all_instruments", **params)

    def get_price(
        self,
        order_book_ids: Codes,
        start_date: object = None,
        end_date: object = None,
        frequency: str = "1d",
        fields: Optional[Sequence[str]] = None,
        adjust_type: str = "pre",
        skip_suspended: bool = False,
        expect_df: bool = True,
        market: str = "cn",
        **kwargs: Any,
    ) -> pd.DataFrame:
        del expect_df  # HTTP 忽略该参数
        params: JsonObject = {
            "order_book_ids": _as_code_list(order_book_ids),
            "frequency": frequency,
            "adjust_type": adjust_type,
            "skip_suspended": skip_suspended,
            "market": market,
            **kwargs,
        }
        if start_date is not None:
            params["start_date"] = start_date
        if end_date is not None:
            params["end_date"] = end_date
        if fields is not None:
            params["fields"] = list(fields)
        frame = self.call("get_price", **params)
        return _normalize_price_frame(frame, frequency)

    def get_ex_factor(
        self,
        order_book_ids: Codes,
        start_date: object = None,
        end_date: object = None,
        market: str = "cn",
        **kwargs: Any,
    ) -> pd.DataFrame:
        params: JsonObject = {
            "order_book_ids": _as_code_list(order_book_ids),
            "market": market,
            **kwargs,
        }
        if start_date is not None:
            params["start_date"] = start_date
        if end_date is not None:
            params["end_date"] = end_date
        return self.call("get_ex_factor", **params)

    def get_trading_dates(
        self,
        start_date: object,
        end_date: object,
        market: str = "cn",
        **kwargs: Any,
    ) -> pd.DataFrame:
        return self.call(
            "get_trading_dates",
            start_date=start_date,
            end_date=end_date,
            market=market,
            **kwargs,
        )

    def instruments(
        self,
        order_book_ids: Codes,
        market: str = "cn",
        **kwargs: Any,
    ) -> pd.DataFrame:
        return self.call(
            "instruments",
            order_book_ids=_as_code_list(order_book_ids),
            market=market,
            **kwargs,
        )

    def fund_all_instruments(self, **kwargs: Any) -> pd.DataFrame:
        """尽量对齐 rqdatac.fund.all_instruments。

        部分账号对 fund.all_instruments 无权限（HTTP 403）；此时回退为
        ETF + LOF 的 all_instruments，并补齐 save_fundlist 所需缺省列。
        """

        try:
            frame = self.call("fund.all_instruments", **kwargs)
        except RQDataHttpError as exc:
            if "403" not in str(exc) and "Not permit" not in str(exc):
                raise
            frames = [
                self.all_instruments(type="ETF", market="cn"),
                self.all_instruments(type="LOF", market="cn"),
            ]
            frame = pd.concat(frames, ignore_index=True, sort=False)
        return _normalize_fund_instruments(frame)

    def index_weights(
        self,
        order_book_id: str,
        date: Optional[object] = None,
        start_date: Optional[object] = None,
        end_date: Optional[object] = None,
        market: str = "cn",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """对齐 rqdatac.index_weights；HTTP 返回扁平 DataFrame。"""

        params: JsonObject = {
            "order_book_id": order_book_id,
            "market": market,
            **kwargs,
        }
        if date is not None:
            params["date"] = date
        if start_date is not None:
            params["start_date"] = start_date
        if end_date is not None:
            params["end_date"] = end_date
        return self.call("index_weights", **params)

    def index_components(
        self,
        order_book_id: str,
        date: Optional[object] = None,
        start_date: Optional[object] = None,
        end_date: Optional[object] = None,
        market: str = "cn",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """对齐 rqdatac.index_components；HTTP 返回扁平 DataFrame。"""

        params: JsonObject = {
            "order_book_id": order_book_id,
            "market": market,
            **kwargs,
        }
        if date is not None:
            params["date"] = date
        if start_date is not None:
            params["start_date"] = start_date
        if end_date is not None:
            params["end_date"] = end_date
        return self.call("index_components", **params)

    def get_industry_mapping(
        self,
        source: str = "citics_2019",
        date: Optional[object] = None,
        market: str = "cn",
        **kwargs: Any,
    ) -> pd.DataFrame:
        params: JsonObject = {"source": source, "market": market, **kwargs}
        if date is not None:
            params["date"] = date
        return self.call("get_industry_mapping", **params)

    def get_industry(
        self,
        industry: str,
        source: str = "citics_2019",
        date: Optional[object] = None,
        market: str = "cn",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """对齐 rqdatac.get_industry：按行业名称/代码取成分股列表。"""

        params: JsonObject = {
            "industry": industry,
            "source": source,
            "market": market,
            **kwargs,
        }
        if date is not None:
            params["date"] = date
        return self.call("get_industry", **params)

    def get_instrument_industry(
        self,
        order_book_ids: Codes,
        source: str = "citics_2019",
        level: object = 0,
        date: Optional[object] = None,
        market: str = "cn",
        **kwargs: Any,
    ) -> pd.DataFrame:
        params: JsonObject = {
            "order_book_ids": _as_code_list(order_book_ids),
            "source": source,
            "level": level,
            "market": market,
            **kwargs,
        }
        if date is not None:
            params["date"] = date
        return self.call("get_instrument_industry", **params)

    def etf_get_components(
        self,
        order_book_ids: Codes,
        date: Optional[object] = None,
        market: str = "cn",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """对齐 etf.get_components / fund.get_etf_components。

        优先 ``etf.get_components``；无权限时回退 ``fund.get_etf_components``。
        """

        codes = _as_code_list(order_book_ids)
        params: JsonObject = {
            "order_book_ids": codes,
            "market": market,
            **kwargs,
        }
        if date is not None:
            params["date"] = date
        try:
            return self.call("etf.get_components", **params)
        except RQDataHttpError as exc:
            text = str(exc)
            if "403" not in text and "Not permit" not in text:
                raise
            # 旧接口 fund.get_etf_components 不接受 date；仅在未指定 date 时回退
            if date is not None:
                raise RQDataHttpError(
                    f"etf.get_components 无权限且无法按 date 回退 fund API: {text}"
                ) from exc
            legacy = {"order_book_ids": codes, "market": market}
            return self.call("fund.get_etf_components", **legacy)


class FundHttpAPI:
    """rqdatac.fund 的 HTTP 兼容命名空间。"""

    def all_instruments(self, **kwargs: Any) -> pd.DataFrame:
        return get_client().fund_all_instruments(**kwargs)

    def get_etf_components(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        return get_client().etf_get_components(*args, **kwargs)


class EtfHttpAPI:
    """rqdatac.etf 的 HTTP 兼容命名空间。"""

    def get_components(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        return get_client().etf_get_components(*args, **kwargs)


def _normalize_fund_instruments(frame: pd.DataFrame) -> pd.DataFrame:
    """补齐 fund_cn_codelist / save_fundlist 期望字段。"""

    if frame is None or frame.empty:
        return pd.DataFrame()
    result = frame.copy()
    defaults: dict[str, Any] = {
        "establishment_date": "0000-00-00",
        "listed_date": "0000-00-00",
        "transition_time": 0,
        "amc": "",
        "symbol": "",
        "fund_type": "",
        "fund_manager": "",
        "latest_size": 0.0,
        "benchmark": "",
        "accrued_daily": False,
        "de_listed_date": "0000-00-00",
        "stop_date": "0000-00-00",
        "exchange": "",
        "round_lot": 1.0,
        "order_book_id": "",
    }
    for column, default in defaults.items():
        if column not in result.columns:
            result[column] = default
    result["benchmark"] = result["benchmark"].map(str)
    result["latest_size"] = pd.to_numeric(
        result["latest_size"], errors="coerce"
    ).fillna(0.0)
    result["round_lot"] = pd.to_numeric(
        result["round_lot"], errors="coerce"
    ).fillna(1.0)
    result["transition_time"] = (
        pd.to_numeric(result["transition_time"], errors="coerce")
        .fillna(0)
        .astype("int64")
    )
    return result


def _normalize_price_frame(frame: pd.DataFrame, frequency: str) -> pd.DataFrame:
    """对齐 rqdatac.get_price 常见索引形态，便于 data_init 入库。"""

    if frame is None or frame.empty:
        return pd.DataFrame() if frame is None else frame
    result = frame.copy()
    # 日线 HTTP 常返回 datetime 列；标准表/脚本期望 date
    if "date" not in result.columns and "datetime" in result.columns:
        if str(frequency).endswith("d") or frequency in {"1d", "day"}:
            result = result.rename(columns={"datetime": "date"})
    index_cols = [
        col
        for col in ("order_book_id", "date", "datetime")
        if col in result.columns
    ]
    if len(index_cols) >= 2:
        result = result.set_index(index_cols)
    return result


_CLIENT: Optional[RQDataHttpClient] = None
_CLIENT_LOCK = Lock()


def get_client(force_new: bool = False) -> RQDataHttpClient:
    global _CLIENT
    with _CLIENT_LOCK:
        if _CLIENT is None or force_new:
            _CLIENT = RQDataHttpClient()
        return _CLIENT


def init_rqdatac_http(force: bool = False) -> RQDataHttpClient:
    """初始化 HTTP 客户端并完成一次鉴权。"""

    client = get_client(force_new=force)
    client.authenticate(force=force)
    return client


def get_price(*args: Any, **kwargs: Any) -> pd.DataFrame:
    return get_client().get_price(*args, **kwargs)


def all_instruments(*args: Any, **kwargs: Any) -> pd.DataFrame:
    return get_client().all_instruments(*args, **kwargs)


def get_ex_factor(*args: Any, **kwargs: Any) -> pd.DataFrame:
    return get_client().get_ex_factor(*args, **kwargs)


def get_trading_dates(*args: Any, **kwargs: Any) -> pd.DataFrame:
    return get_client().get_trading_dates(*args, **kwargs)


def instruments(*args: Any, **kwargs: Any) -> pd.DataFrame:
    return get_client().instruments(*args, **kwargs)


def user_get_quota() -> dict[str, Any]:
    return get_client().get_quota()


def index_weights(*args: Any, **kwargs: Any) -> pd.DataFrame:
    return get_client().index_weights(*args, **kwargs)


def index_components(*args: Any, **kwargs: Any) -> pd.DataFrame:
    return get_client().index_components(*args, **kwargs)


def get_industry_mapping(*args: Any, **kwargs: Any) -> pd.DataFrame:
    return get_client().get_industry_mapping(*args, **kwargs)


def get_industry(*args: Any, **kwargs: Any) -> pd.DataFrame:
    return get_client().get_industry(*args, **kwargs)


def get_instrument_industry(*args: Any, **kwargs: Any) -> pd.DataFrame:
    return get_client().get_instrument_industry(*args, **kwargs)


def etf_get_components(*args: Any, **kwargs: Any) -> pd.DataFrame:
    return get_client().etf_get_components(*args, **kwargs)


# 与 `from rqdatac import fund / etf` 对齐的模块级对象
fund = FundHttpAPI()
etf = EtfHttpAPI()
