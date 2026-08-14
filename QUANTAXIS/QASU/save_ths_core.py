# ruff: noqa: UP045
"""同花顺数据保存的共享抓取编排与存储协议。"""

from __future__ import annotations

import datetime
import hashlib
import json
from collections.abc import Callable, Iterable, Sequence
from typing import Optional, Protocol
from zoneinfo import ZoneInfo

import pandas as pd

from QUANTAXIS.QAFetch import QAThs_http as ths
from QUANTAXIS.QAUtil import DATABASE

CHINA_TZ: ZoneInfo = ZoneInfo("Asia/Shanghai")


class StorageBackend(Protocol):
    """定义同花顺抓取编排所需的最小存储接口。"""

    def save(
        self,
        dataset: str,
        frame: pd.DataFrame,
        unique_fields: Sequence[str],
    ) -> int:
        """幂等保存数据。

        Args:
            dataset: 目标数据集名称。
            frame: 待保存的数据。
            unique_fields: 用于生成记录唯一标识的字段。

        Returns:
            写入或更新的记录数。
        """

    def latest(
        self,
        dataset: str,
        code: str,
        field: str,
    ) -> Optional[pd.Timestamp]:
        """查询指定证券的最新时间。

        Args:
            dataset: 目标数据集名称。
            code: 证券代码。
            field: 时间字段名称。

        Returns:
            最新时间；不存在时返回 ``None``。
        """


StorageResolver = Callable[[object, Optional[str]], StorageBackend]

DAY_DATASETS: dict[ths.THSAsset, str] = {
    ths.THSAsset.STOCK: "stock_day",
    ths.THSAsset.INDEX: "index_day",
    ths.THSAsset.ETF: "etf_day",
    ths.THSAsset.FUTURE: "future_day",
    ths.THSAsset.OPTION: "option_day_all",
    ths.THSAsset.BOND: "bond_day",
}

MIN_DATASETS: dict[ths.THSAsset, str] = {
    ths.THSAsset.STOCK: "stock_min",
    ths.THSAsset.INDEX: "index_min",
    ths.THSAsset.ETF: "etf_min",
    ths.THSAsset.FUTURE: "future_min",
    ths.THSAsset.OPTION: "option_min_all",
    ths.THSAsset.BOND: "bond_min",
}


def frame_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    """把 DataFrame 转换为可序列化的记录。

    Args:
        frame: 待转换的数据。

    Returns:
        由字典组成的记录列表。

    Raises:
        TypeError: DataFrame 无法转换为记录列表。
    """

    normalized = frame.reset_index(drop=True).copy()
    payload = normalized.to_json(
        orient="records",
        date_format="iso",
        force_ascii=False,
    )
    parsed = json.loads(payload)
    if not isinstance(parsed, list):
        raise TypeError("DataFrame 无法转换为记录列表")
    return [dict(item) for item in parsed if isinstance(item, dict)]


def record_id(
    dataset: str,
    record: dict[str, object],
    unique_fields: Sequence[str],
) -> str:
    """生成稳定的记录标识。

    Args:
        dataset: 数据集名称。
        record: 单条记录。
        unique_fields: 唯一键字段。

    Returns:
        SHA-256 记录标识。
    """

    identity: object
    if unique_fields:
        identity = [record.get(field) for field in unique_fields]
    else:
        identity = record
    raw_identity = json.dumps(
        identity,
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(f"{dataset}:{raw_identity}".encode()).hexdigest()


def event_time(record: dict[str, object]) -> pd.Timestamp:
    """从记录中提取 ClickHouse 事件时间。

    Args:
        record: 单条记录。

    Returns:
        可用的事件时间；缺失时返回当前上海时区的朴素时间戳，
        避免 ``1970-01-01`` 在东八区 ClickHouse ``DateTime`` 中越界。
    """

    for field in ("datetime", "date", "time"):
        value = record.get(field)
        if value is not None:
            parsed = pd.to_datetime(value, errors="coerce")
            if not pd.isna(parsed):
                ts = pd.Timestamp(parsed)
                if ts.tzinfo is not None:
                    ts = ts.tz_convert(CHINA_TZ).replace(tzinfo=None)
                # ClickHouse DateTime 以 UTC 纪元为下界；东八区本地 1970-01-01
                # 会被解释为 1969-12-31 UTC 并触发 out of range。
                if ts < pd.Timestamp("1970-01-02"):
                    break
                return ts
    return pd.Timestamp.now(tz=CHINA_TZ).replace(tzinfo=None)

class SaveAPI:
    """复用抓取流程并把持久化委托给选定后端。"""

    def __init__(self, storage_resolver: StorageResolver) -> None:
        """初始化共享保存 API。

        Args:
            storage_resolver: 根据客户端和可选后端名创建存储的函数。
        """

        self._storage_resolver = storage_resolver

    def _storage(
        self,
        client: object,
        backend: Optional[str],
    ) -> StorageBackend:
        """创建本次调用使用的存储后端。

        Args:
            client: 数据库客户端。
            backend: 可选的后端覆盖名称。

        Returns:
            存储后端实例。
        """

        return self._storage_resolver(client, backend)

    @staticmethod
    def _fetch_list(asset: ths.THSAsset) -> pd.DataFrame:
        """抓取指定资产的证券清单。

        Args:
            asset: 同花顺资产类型。

        Returns:
            证券清单。
        """

        fetchers = {
            ths.THSAsset.STOCK: ths.QA_fetch_get_stock_list,
            ths.THSAsset.INDEX: ths.QA_fetch_get_index_list,
            ths.THSAsset.ETF: ths.QA_fetch_get_etf_list,
            ths.THSAsset.FUTURE: ths.QA_fetch_get_future_list,
            ths.THSAsset.OPTION: ths.QA_fetch_get_option_contract_list,
            ths.THSAsset.BOND: ths.QA_fetch_get_bond_list,
        }
        return fetchers[asset]()

    @staticmethod
    def _code_values(frame: pd.DataFrame) -> Iterable[str]:
        """提取去重后的同花顺代码。

        Args:
            frame: 包含代码列的数据。

        Returns:
            代码迭代器。
        """

        field = "ths_code" if "ths_code" in frame.columns else "code"
        return frame[field].dropna().astype(str).drop_duplicates().tolist()

    @staticmethod
    def _batches(values: Sequence[str], size: int) -> Iterable[list[str]]:
        """按固定大小切分代码。

        Args:
            values: 待切分代码。
            size: 每批最大数量。

        Yields:
            当前批次的代码列表。
        """

        for index in range(0, len(values), size):
            yield list(values[index : index + size])

    @staticmethod
    def _start_date(
        storage: StorageBackend,
        dataset: str,
        code: str,
        field: str,
        default: str,
        step: datetime.timedelta,
    ) -> str:
        """计算增量抓取起始时间。

        Args:
            storage: 存储后端。
            dataset: 数据集名称。
            code: 同花顺代码。
            field: 时间字段。
            default: 无历史记录时的起始时间。
            step: 最新记录之后的增量步长。

        Returns:
            格式化后的起始时间。
        """

        latest = storage.latest(dataset, code.split(".")[0], field)
        if latest is None:
            return default
        return (latest + step).strftime("%Y-%m-%d %H:%M:%S")

    def _save_list(
        self,
        asset: ths.THSAsset,
        client: object,
        backend: Optional[str],
    ) -> int:
        """保存指定资产清单。

        Args:
            asset: 同花顺资产类型。
            client: 数据库客户端。
            backend: 可选的后端覆盖名称。

        Returns:
            保存记录数。
        """

        dataset = (
            f"{asset.value}_list"
            if asset is not ths.THSAsset.OPTION
            else "option_contract_list"
        )
        return self._storage(client, backend).save(
            dataset,
            self._fetch_list(asset),
            ("ths_code",),
        )

    def _save_single_day(
        self,
        asset: ths.THSAsset,
        code: str,
        client: object,
        backend: Optional[str],
    ) -> int:
        """增量保存单个资产日线。

        Args:
            asset: 同花顺资产类型。
            code: 同花顺代码。
            client: 数据库客户端。
            backend: 可选的后端覆盖名称。

        Returns:
            保存记录数。
        """

        storage = self._storage(client, backend)
        dataset = DAY_DATASETS[asset]
        start = self._start_date(
            storage,
            dataset,
            code,
            "date",
            "1990-01-01",
            datetime.timedelta(days=1),
        )
        end = datetime.datetime.now(CHINA_TZ).date().isoformat()
        if start[:10] > end:
            return 0
        frame = ths.QA_fetch_get_asset_day(code, start, end, asset)
        return storage.save(dataset, frame, ("code", "date"))

    def _save_single_min(
        self,
        asset: ths.THSAsset,
        code: str,
        client: object,
        backend: Optional[str],
        frequency: str = "1min",
    ) -> int:
        """增量保存单个资产分钟线。

        Args:
            asset: 同花顺资产类型。
            code: 同花顺代码。
            client: 数据库客户端。
            backend: 可选的后端覆盖名称。
            frequency: 分钟线周期。

        Returns:
            保存记录数。
        """

        storage = self._storage(client, backend)
        dataset = MIN_DATASETS[asset]
        default = (
            datetime.datetime.now(CHINA_TZ) - datetime.timedelta(days=10)
        ).strftime("%Y-%m-%d 09:00:00")
        start = self._start_date(
            storage,
            dataset,
            code,
            "datetime",
            default,
            datetime.timedelta(minutes=1),
        )
        end = datetime.datetime.now(CHINA_TZ).strftime("%Y-%m-%d %H:%M:%S")
        frame = ths.QA_fetch_get_asset_min(
            code,
            start,
            end,
            asset,
            frequency,
        )
        return storage.save(dataset, frame, ("code", "datetime", "type"))

    def _save_all_day(
        self,
        asset: ths.THSAsset,
        client: object,
        backend: Optional[str],
    ) -> int:
        """保存指定资产的全部日线。

        Args:
            asset: 同花顺资产类型。
            client: 数据库客户端。
            backend: 可选的后端覆盖名称。

        Returns:
            保存记录总数。
        """

        total = 0
        for code in self._code_values(self._fetch_list(asset)):
            total += self._save_single_day(asset, code, client, backend)
        return total

    def _save_all_min(
        self,
        asset: ths.THSAsset,
        client: object,
        backend: Optional[str],
    ) -> int:
        """保存指定资产的全部分钟线。

        Args:
            asset: 同花顺资产类型。
            client: 数据库客户端。
            backend: 可选的后端覆盖名称。

        Returns:
            保存记录总数。
        """

        total = 0
        for code in self._code_values(self._fetch_list(asset)):
            total += self._save_single_min(asset, code, client, backend)
        return total

    def _save_transactions(
        self,
        asset: ths.THSAsset,
        client: object,
        backend: Optional[str],
    ) -> int:
        """保存指定资产的当日逐笔数据。

        Args:
            asset: 同花顺资产类型。
            client: 数据库客户端。
            backend: 可选的后端覆盖名称。

        Returns:
            保存记录总数。
        """

        storage = self._storage(client, backend)
        dataset = f"{asset.value}_transaction"
        today = datetime.datetime.now(CHINA_TZ).date().isoformat()
        total = 0
        for code in self._code_values(self._fetch_list(asset)):
            frame = ths.QA_fetch_get_asset_transaction(
                code,
                f"{today} 09:00:00",
                f"{today} 15:30:00",
                asset,
            )
            total += storage.save(dataset, frame, ("code", "datetime"))
        return total

    def QA_SU_save_stock_info(
        self,
        client: object = DATABASE,
        backend: Optional[str] = None,
    ) -> int:
        """保存股票基本信息。

        Args:
            client: 数据库客户端。
            backend: 可选的后端覆盖名称。

        Returns:
            保存记录数。
        """

        codes = list(self._code_values(ths.QA_fetch_get_stock_list()))
        storage = self._storage(client, backend)
        total = 0
        for batch in self._batches(codes, 100):
            total += storage.save(
                "stock_info",
                ths.QA_fetch_get_stock_info(batch),
                ("code",),
            )
        return total

    def QA_SU_save_stock_list(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存股票清单。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_list(ths.THSAsset.STOCK, client, backend)

    def QA_SU_save_index_list(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存指数清单。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_list(ths.THSAsset.INDEX, client, backend)

    def QA_SU_save_etf_list(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存 ETF 清单。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_list(ths.THSAsset.ETF, client, backend)

    def QA_SU_save_future_list(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存期货清单。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_list(ths.THSAsset.FUTURE, client, backend)

    def QA_SU_save_option_contract_list(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存期权合约清单。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_list(ths.THSAsset.OPTION, client, backend)

    def QA_SU_save_bond_list(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存债券清单。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_list(ths.THSAsset.BOND, client, backend)

    def QA_SU_save_single_stock_day(
        self,
        code: str,
        client: object = DATABASE,
        backend: Optional[str] = None,
    ) -> int:
        """保存单只股票日线。

        Args:
            code: 同花顺代码。
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_single_day(ths.THSAsset.STOCK, code, client, backend)

    def QA_SU_save_stock_day(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存全部股票日线。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_all_day(ths.THSAsset.STOCK, client, backend)

    def QA_SU_save_single_index_day(
        self,
        code: str,
        client: object = DATABASE,
        backend: Optional[str] = None,
    ) -> int:
        """保存单个指数日线。

        Args:
            code: 同花顺代码。
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_single_day(ths.THSAsset.INDEX, code, client, backend)

    def QA_SU_save_index_day(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存全部指数日线。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_all_day(ths.THSAsset.INDEX, client, backend)

    def QA_SU_save_single_etf_day(
        self,
        code: str,
        client: object = DATABASE,
        backend: Optional[str] = None,
    ) -> int:
        """保存单只 ETF 日线。

        Args:
            code: 同花顺代码。
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_single_day(ths.THSAsset.ETF, code, client, backend)

    def QA_SU_save_etf_day(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存全部 ETF 日线。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_all_day(ths.THSAsset.ETF, client, backend)

    def QA_SU_save_single_future_day(
        self,
        code: str,
        client: object = DATABASE,
        backend: Optional[str] = None,
    ) -> int:
        """保存单个期货合约日线。

        Args:
            code: 同花顺代码。
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_single_day(ths.THSAsset.FUTURE, code, client, backend)

    def QA_SU_save_future_day(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存全部期货日线。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_all_day(ths.THSAsset.FUTURE, client, backend)

    def QA_SU_save_bond_day(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存全部债券日线。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_all_day(ths.THSAsset.BOND, client, backend)

    def QA_SU_save_single_bond_day(
        self,
        code: str,
        client: object = DATABASE,
        backend: Optional[str] = None,
    ) -> int:
        """保存单只债券日线。

        Args:
            code: 同花顺代码。
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_single_day(ths.THSAsset.BOND, code, client, backend)

    def QA_SU_save_option_day_all(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存全部期权日线。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_all_day(ths.THSAsset.OPTION, client, backend)

    def QA_SU_save_single_stock_min(
        self,
        code: str,
        client: object = DATABASE,
        backend: Optional[str] = None,
    ) -> int:
        """保存单只股票分钟线。

        Args:
            code: 同花顺代码。
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_single_min(ths.THSAsset.STOCK, code, client, backend)

    def QA_SU_save_stock_min(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存全部股票分钟线。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_all_min(ths.THSAsset.STOCK, client, backend)

    def QA_SU_save_single_index_min(
        self,
        code: str,
        client: object = DATABASE,
        backend: Optional[str] = None,
    ) -> int:
        """保存单个指数分钟线。

        Args:
            code: 同花顺代码。
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_single_min(ths.THSAsset.INDEX, code, client, backend)

    def QA_SU_save_index_min(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存全部指数分钟线。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_all_min(ths.THSAsset.INDEX, client, backend)

    def QA_SU_save_single_etf_min(
        self,
        code: str,
        client: object = DATABASE,
        backend: Optional[str] = None,
    ) -> int:
        """保存单只 ETF 分钟线。

        Args:
            code: 同花顺代码。
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_single_min(ths.THSAsset.ETF, code, client, backend)

    def QA_SU_save_etf_min(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存全部 ETF 分钟线。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_all_min(ths.THSAsset.ETF, client, backend)

    def QA_SU_save_single_future_min(
        self,
        code: str,
        client: object = DATABASE,
        backend: Optional[str] = None,
    ) -> int:
        """保存单个期货合约分钟线。

        Args:
            code: 同花顺代码。
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_single_min(ths.THSAsset.FUTURE, code, client, backend)

    def QA_SU_save_future_min(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存全部期货分钟线。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_all_min(ths.THSAsset.FUTURE, client, backend)

    def QA_SU_save_bond_min(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存全部债券分钟线。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_all_min(ths.THSAsset.BOND, client, backend)

    def QA_SU_save_single_bond_min(
        self,
        code: str,
        client: object = DATABASE,
        backend: Optional[str] = None,
    ) -> int:
        """保存单只债券分钟线。

        Args:
            code: 同花顺代码。
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_single_min(ths.THSAsset.BOND, code, client, backend)

    def QA_SU_save_option_min_all(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存全部期权分钟线。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_all_min(ths.THSAsset.OPTION, client, backend)

    def QA_SU_save_stock_transaction(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存当日股票逐笔数据。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_transactions(ths.THSAsset.STOCK, client, backend)

    def QA_SU_save_index_transaction(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存当日指数逐笔数据。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._save_transactions(ths.THSAsset.INDEX, client, backend)

    def QA_SU_save_stock_xdxr(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存股票除权除息数据。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        codes = list(self._code_values(ths.QA_fetch_get_stock_list()))
        storage = self._storage(client, backend)
        total = 0
        for batch in self._batches(codes, 100):
            total += storage.save(
                "stock_xdxr",
                ths.QA_fetch_get_stock_xdxr(batch),
                ("code", "date"),
            )
        return total

    def QA_SU_save_stock_block(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存股票板块及成分。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        return self._storage(client, backend).save(
            "stock_block",
            ths.QA_fetch_get_stock_block(),
            ("code", "blockname"),
        )

    def QA_SU_save_stock_divyield_day(
        self, client: object = DATABASE, backend: Optional[str] = None
    ) -> int:
        """保存分红送转数据。

        Args:
            client: 数据库客户端。
            backend: 可选后端名称。

        Returns:
            保存记录数。
        """

        codes = list(self._code_values(ths.QA_fetch_get_stock_list()))
        storage = self._storage(client, backend)
        total = 0
        for batch in self._batches(codes, 100):
            total += storage.save(
                "stock_divyield",
                ths.QA_fetch_get_stock_xdxr(batch),
                ("code", "date"),
            )
        return total
