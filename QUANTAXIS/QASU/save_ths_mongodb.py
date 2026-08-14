# ruff: noqa: UP045
"""同花顺数据的 MongoDB 存储实现与保存接口。"""

from __future__ import annotations

import os
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Protocol, cast

import pandas as pd
from pymongo import ASCENDING, UpdateOne

from QUANTAXIS.QAFetch import QAThs_http as ths
from QUANTAXIS.QASU.save_ths_core import (
    SaveAPI,
    StorageBackend,
    frame_records,
    record_id,
)

MONGO_COLLECTIONS: dict[str, str] = {
    "etf_day": "index_day",
    "etf_min": "index_min",
}


class MongoCollection(Protocol):
    """定义保存流程使用的最小 MongoDB Collection 接口。"""

    def create_index(
        self,
        keys: Sequence[tuple[str, int]],
        unique: bool = False,
    ) -> object:
        """创建索引。

        Args:
            keys: 索引字段与排序方向。
            unique: 是否创建唯一索引。

        Returns:
            MongoDB 驱动返回值。
        """

    def bulk_write(
        self,
        operations: Sequence[object],
        ordered: bool = False,
    ) -> object:
        """批量写入。

        Args:
            operations: 批量更新操作。
            ordered: 是否按顺序执行。

        Returns:
            MongoDB 驱动返回值。
        """

    def find_one(
        self,
        query: dict[str, object],
        sort: Sequence[tuple[str, int]],
    ) -> Optional[dict[str, object]]:
        """查询单条记录。

        Args:
            query: MongoDB 查询条件。
            sort: 排序字段。

        Returns:
            命中的记录；无结果时返回 ``None``。
        """


class MongoDatabase(Protocol):
    """定义保存流程使用的最小 MongoDB Database 接口。"""

    def __getattr__(self, name: str) -> MongoCollection:
        """取得集合。

        Args:
            name: 集合名称。

        Returns:
            MongoDB 集合。
        """


class MongoStorage:
    """使用幂等 upsert 保存同花顺数据。"""

    def __init__(self, client: object) -> None:
        """初始化 MongoDB 存储。

        Args:
            client: QUANTAXIS MongoDB 数据库对象。
        """

        self.database: MongoDatabase = cast(MongoDatabase, client)

    def save(
        self,
        dataset: str,
        frame: pd.DataFrame,
        unique_fields: Sequence[str],
    ) -> int:
        """幂等保存 DataFrame。

        Args:
            dataset: MongoDB 集合名称。
            frame: 待保存数据。
            unique_fields: 业务唯一键字段。

        Returns:
            批量写入的记录数。
        """

        if frame.empty:
            return 0
        records = frame_records(frame)
        collection_name = MONGO_COLLECTIONS.get(dataset, dataset)
        collection = getattr(self.database, collection_name)
        available_fields = (
            list(unique_fields)
            if all(field in records[0] for field in unique_fields)
            else []
        )
        if available_fields:
            collection.create_index(
                [(field, ASCENDING) for field in available_fields],
                unique=False,
            )
        # sparse：兼容 TDX 等旧数据缺少 _record_id 的文档，避免唯一索引构建失败。
        collection.create_index(
            [("_record_id", ASCENDING)],
            unique=True,
            sparse=True,
        )
        operations: list[object] = []
        for record in records:
            identifier = record_id(
                collection_name,
                record,
                available_fields,
            )
            record["_record_id"] = identifier
            record["source"] = "ths_http"
            # 兼容 QA_fetch_* 读取端对 TDX ``vol`` 字段的依赖。
            if "volume" in record and "vol" not in record:
                record["vol"] = record["volume"]
            elif "vol" in record and "volume" not in record:
                record["volume"] = record["vol"]
            operations.append(
                UpdateOne(
                    {"_record_id": identifier},
                    {"$set": record},
                    upsert=True,
                )
            )
        collection.bulk_write(operations, ordered=False)
        return len(operations)

    def latest(
        self,
        dataset: str,
        code: str,
        field: str,
    ) -> Optional[pd.Timestamp]:
        """查询指定证券的最新时间。

        Args:
            dataset: MongoDB 集合名称。
            code: 证券代码。
            field: 时间字段名称。

        Returns:
            最新时间；无记录时返回 ``None``。
        """

        collection_name = MONGO_COLLECTIONS.get(dataset, dataset)
        collection = getattr(self.database, collection_name)
        record = collection.find_one({"code": code}, sort=[(field, -1)])
        if not record or field not in record:
            return None
        value = pd.to_datetime(record[field], errors="coerce")
        return None if pd.isna(value) else pd.Timestamp(value)


def create_storage(
    client: object,
    backend: Optional[str] = None,
) -> StorageBackend:
    """创建 MongoDB 存储后端。

    Args:
        client: MongoDB 数据库对象。
        backend: 可选后端名称，仅接受 ``mongodb``。

    Returns:
        MongoDB 存储实例。

    Raises:
        ValueError: 请求了非 MongoDB 后端。
    """

    if backend is not None and backend.lower() != "mongodb":
        raise ValueError(f"MongoDB 模块不支持后端: {backend}")
    return MongoStorage(client)


class MongoSaveAPI(SaveAPI):
    """支持可控并行采集的 MongoDB 保存 API。"""

    def __init__(self) -> None:
        super().__init__(create_storage)
        self.parallel: bool = False
        self.max_workers: int = int(os.getenv("THS_MONGO_WORKERS", "4"))

    def configure_parallel(
        self,
        enabled: bool,
        max_workers: Optional[int] = None,
    ) -> None:
        """设置批量日线和分钟线任务的并行方式。"""

        if max_workers is not None:
            if max_workers < 1:
                raise ValueError("max_workers 必须大于 0")
            self.max_workers = max_workers
        self.parallel = enabled

    def _parallel_sum(
        self,
        codes: Sequence[str],
        worker: Callable[[str], int],
    ) -> int:
        if not self.parallel or len(codes) < 2:
            return sum(worker(code) for code in codes)
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            return sum(executor.map(worker, codes))

    def _save_all_day(
        self,
        asset: ths.THSAsset,
        client: object,
        backend: Optional[str],
    ) -> int:
        codes = list(self._code_values(self._fetch_list(asset)))
        return self._parallel_sum(
            codes,
            lambda code: self._save_single_day(
                asset,
                code,
                client,
                backend,
            ),
        )

    def _save_all_min(
        self,
        asset: ths.THSAsset,
        client: object,
        backend: Optional[str],
    ) -> int:
        codes = list(self._code_values(self._fetch_list(asset)))
        return self._parallel_sum(
            codes,
            lambda code: self._save_single_min(
                asset,
                code,
                client,
                backend,
            ),
        )


_API: MongoSaveAPI = MongoSaveAPI()


def configure_parallel(
    enabled: bool,
    max_workers: Optional[int] = None,
) -> None:
    """配置 MongoDB 批量采集并行度。"""

    _API.configure_parallel(enabled, max_workers)

QA_SU_save_stock_info = _API.QA_SU_save_stock_info
QA_SU_save_stock_list = _API.QA_SU_save_stock_list
QA_SU_save_index_list = _API.QA_SU_save_index_list
QA_SU_save_etf_list = _API.QA_SU_save_etf_list
QA_SU_save_future_list = _API.QA_SU_save_future_list
QA_SU_save_option_contract_list = _API.QA_SU_save_option_contract_list
QA_SU_save_bond_list = _API.QA_SU_save_bond_list
QA_SU_save_single_stock_day = _API.QA_SU_save_single_stock_day
QA_SU_save_stock_day = _API.QA_SU_save_stock_day
QA_SU_save_single_index_day = _API.QA_SU_save_single_index_day
QA_SU_save_index_day = _API.QA_SU_save_index_day
QA_SU_save_single_etf_day = _API.QA_SU_save_single_etf_day
QA_SU_save_etf_day = _API.QA_SU_save_etf_day
QA_SU_save_single_future_day = _API.QA_SU_save_single_future_day
QA_SU_save_future_day = _API.QA_SU_save_future_day
QA_SU_save_future_day_all = QA_SU_save_future_day
QA_SU_save_bond_day = _API.QA_SU_save_bond_day
QA_SU_save_single_bond_day = _API.QA_SU_save_single_bond_day
QA_SU_save_option_day_all = _API.QA_SU_save_option_day_all
QA_SU_save_option_50etf_day = QA_SU_save_option_day_all
QA_SU_save_option_300etf_day = QA_SU_save_option_day_all
QA_SU_save_option_commodity_day = QA_SU_save_option_day_all
QA_SU_save_single_stock_min = _API.QA_SU_save_single_stock_min
QA_SU_save_stock_min = _API.QA_SU_save_stock_min
QA_SU_save_single_index_min = _API.QA_SU_save_single_index_min
QA_SU_save_index_min = _API.QA_SU_save_index_min
QA_SU_save_single_etf_min = _API.QA_SU_save_single_etf_min
QA_SU_save_etf_min = _API.QA_SU_save_etf_min
QA_SU_save_single_future_min = _API.QA_SU_save_single_future_min
QA_SU_save_future_min = _API.QA_SU_save_future_min
QA_SU_save_future_min_all = QA_SU_save_future_min
QA_SU_save_bond_min = _API.QA_SU_save_bond_min
QA_SU_save_single_bond_min = _API.QA_SU_save_single_bond_min
QA_SU_save_option_min_all = _API.QA_SU_save_option_min_all
QA_SU_save_option_50etf_min = QA_SU_save_option_min_all
QA_SU_save_option_300etf_min = QA_SU_save_option_min_all
QA_SU_save_option_commodity_min = QA_SU_save_option_min_all
QA_SU_save_stock_transaction = _API.QA_SU_save_stock_transaction
QA_SU_save_index_transaction = _API.QA_SU_save_index_transaction
QA_SU_save_stock_xdxr = _API.QA_SU_save_stock_xdxr
QA_SU_save_stock_block = _API.QA_SU_save_stock_block
QA_SU_save_stock_divyield_day = _API.QA_SU_save_stock_divyield_day
QA_SU_save_stock_divyield_his = QA_SU_save_stock_divyield_day
