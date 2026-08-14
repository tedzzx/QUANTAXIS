# ruff: noqa: UP045
"""同花顺保存接口的 MongoDB/ClickHouse 兼容门面。"""

from __future__ import annotations

import os
from typing import Optional

from QUANTAXIS.QASU import save_ths_clickhouse, save_ths_mongodb
from QUANTAXIS.QASU.save_ths_clickhouse import ClickHouseStorage
from QUANTAXIS.QASU.save_ths_core import SaveAPI, StorageBackend
from QUANTAXIS.QASU.save_ths_mongodb import MongoStorage

__all__ = ["ClickHouseStorage", "MongoStorage"]

_DEFAULT_BACKEND: str = os.getenv("THS_STORAGE_BACKEND", "mongodb").lower()
_SUPPORTED_BACKENDS: set[str] = {"mongodb", "clickhouse"}


def configure_backend(backend: str) -> None:
    """设置后续保存调用使用的默认后端。

    Args:
        backend: ``mongodb`` 或 ``clickhouse``。

    Raises:
        ValueError: 后端名称不受支持。
    """

    global _DEFAULT_BACKEND
    normalized = backend.lower()
    if normalized not in _SUPPORTED_BACKENDS:
        raise ValueError(f"不支持的 THS 存储后端: {backend}")
    _DEFAULT_BACKEND = normalized


def configure_parallel(
    enabled: bool,
    max_workers: Optional[int] = None,
) -> None:
    """配置 THS MongoDB 批量保存的并行度。"""

    save_ths_mongodb.configure_parallel(enabled, max_workers)


def _storage(
    client: object,
    backend: Optional[str] = None,
) -> StorageBackend:
    """根据默认配置或调用参数创建存储后端。

    Args:
        client: 数据库客户端。
        backend: 可选的单次调用后端覆盖名称。

    Returns:
        选定的存储后端。

    Raises:
        ValueError: 后端名称不受支持。
    """

    selected = (backend or _DEFAULT_BACKEND).lower()
    if selected == "mongodb":
        return save_ths_mongodb.create_storage(client)
    if selected == "clickhouse":
        return save_ths_clickhouse.create_storage(client)
    raise ValueError(f"不支持的 THS 存储后端: {selected}")


_API: SaveAPI = SaveAPI(_storage)

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
