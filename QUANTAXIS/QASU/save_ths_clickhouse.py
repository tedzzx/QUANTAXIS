# ruff: noqa: UP045
"""同花顺数据的 ClickHouse 标准表与 raw 存储接口。"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Optional, Protocol, cast

import pandas as pd

from QUANTAXIS.QAFetch.QAClickhouse import PRICE_SCALE, stock_format
from QUANTAXIS.QASU.save_ths_core import (
    SaveAPI,
    StorageBackend,
    event_time,
    frame_records,
    record_id,
)

# 写入 ClickHouse 时价格需 ×PRICE_SCALE 的股票标准表数据集
_STOCK_INT_DATASETS = frozenset({"stock_day", "stock_min", "stock_5min"})
_PRICE_INT_COLUMNS = (
    "open",
    "high",
    "low",
    "close",
    "limit_up",
    "limit_down",
)
_AMOUNT_INT_COLUMNS = ("volume", "total_turnover", "num_trades")

CANONICAL_TABLES: dict[str, tuple[str, str]] = {
    "stock_day": ("quantaxis.stock_cn_day", "date"),
    "stock_min": ("quantaxis.stock_cn_1min", "datetime"),
    "stock_5min": ("quantaxis.stock_cn_5min", "datetime"),
    "index_day": ("quantaxis.index_cn_day", "date"),
    "index_min": ("quantaxis.index_cn_1min", "datetime"),
    "etf_day": ("quantaxis.etf_cn_day", "date"),
    "etf_min": ("quantaxis.etf_cn_1min", "datetime"),
    "future_day": ("quantaxis.future_cn_day", "date"),
    "future_min": ("quantaxis.future_cn_1min", "datetime"),
}

# 标准分钟/日线表 DDL（按需建表；价格 Int32×10000，成交额/量 Int64 元或股）
STOCK_CN_5MIN_DDL = """
CREATE TABLE IF NOT EXISTS quantaxis.stock_cn_5min (
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

STOCK_CN_1MIN_DDL = """
CREATE TABLE IF NOT EXISTS quantaxis.stock_cn_1min (
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

STOCK_CN_DAY_DDL = """
CREATE TABLE IF NOT EXISTS quantaxis.stock_cn_day (
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

# 证券清单写入 QAClickhouse 码表（非行情时间序列）。
CODELIST_TABLES: dict[str, str] = {
    "stock_list": "quantaxis.stock_cn_codelist",
}


def _to_clickhouse_value(value: object) -> object:
    """将 pandas/numpy 标量转换为 clickhouse-driver 可接受的 Python 值。"""

    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    shape = getattr(value, "shape", None)
    if shape == () and hasattr(value, "item"):
        value = value.item()
    try:
        if value is pd.NA or pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, float) and value != value:  # NaN
        return None
    if isinstance(value, (int, float, str, bool)):
        return value
    if hasattr(value, "isoformat"):
        return value
    try:
        return float(value)
    except (TypeError, ValueError):
        return str(value)


class THSClickHouseSchemaError(RuntimeError):
    """表示现有 ClickHouse 标准表无法接收标准化数据。"""


def _is_pymongo_container(obj: object) -> bool:
    """判断对象是否为 pymongo Database/Client（hasattr 会伪造集合名）。"""

    module = getattr(type(obj), "__module__", "")
    return module.startswith("pymongo")


def _type_defines(obj: object, name: str) -> bool:
    """判断类型继承链上是否真实定义了属性/方法。"""

    return any(name in cls.__dict__ for cls in type(obj).mro())


class ClickHouseClient(Protocol):
    """定义 ``ths_raw`` 存储使用的最小 ClickHouse 客户端接口。"""

    def execute(
        self,
        query: str,
        params: Optional[object] = None,
    ) -> object:
        """执行 SQL。

        Args:
            query: SQL 语句。
            params: 可选查询参数。

        Returns:
            ClickHouse 驱动返回值。
        """

    def insert_dataframe(
        self,
        query: str,
        dataframe: pd.DataFrame,
    ) -> object:
        """插入 DataFrame。

        Args:
            query: INSERT SQL 语句。
            dataframe: 待插入数据。

        Returns:
            ClickHouse 驱动返回值。
        """


class ClickHouseStorage:
    """把同花顺扩展字段完整保存到通用 ``ths_raw`` 表。"""

    TABLE_NAME: str = "ths_raw"

    def __init__(self, client: Optional[object] = None) -> None:
        """初始化 ClickHouse 存储并确保原始表存在。

        Args:
            client: ClickHouse 客户端或资源管理器。
        """

        self.client: ClickHouseClient = self._resolve_client(client)
        self._ensure_table()

    def insert_canonical_rows(
        self,
        dataset: str,
        rows: Sequence[tuple],
        columns: Sequence[str],
        *,
        batch_size: int = 20000,
    ) -> int:
        """将已规范化的行元组直接写入标准行情表（跳过 DataFrame 去重）。

        适用于 EQ 等已产出 ``order_book_id`` + OHLCV 元组的快速入库路径。
        依赖 ``ReplacingMergeTree`` 消化重复键。

        Args:
            dataset: 逻辑数据集（如 ``stock_day`` / ``stock_5min``）。
            rows: 与 ``columns`` 对齐的行元组。
            columns: 插入列名。
            batch_size: 单批 INSERT 行数。

        Returns:
            插入行数。
        """

        if not rows:
            return 0
        if dataset not in CANONICAL_TABLES:
            raise THSClickHouseSchemaError(f"不支持的标准数据集: {dataset}")
        table_name, _time_field = CANONICAL_TABLES[dataset]
        if not self._table_exists(table_name):
            raise THSClickHouseSchemaError(f"标准表不存在: {table_name}")
        if not columns:
            raise THSClickHouseSchemaError("insert_canonical_rows 需要 columns")
        columns_sql = ", ".join(columns)
        insert_sql = f"INSERT INTO {table_name} ({columns_sql}) VALUES"
        size = max(1, int(batch_size))
        values = [tuple(row) for row in rows]
        for offset in range(0, len(values), size):
            self.client.execute(insert_sql, values[offset : offset + size])
        return len(values)

    def save(
        self,
        dataset: str,
        frame: pd.DataFrame,
        unique_fields: Sequence[str],
    ) -> int:
        """保存 DataFrame 到 ``ths_raw``。

        Args:
            dataset: 逻辑数据集名称。
            frame: 待保存数据。
            unique_fields: 业务唯一键字段。

        Returns:
            插入的记录数。
        """

        if frame.empty:
            return 0
        if dataset in CODELIST_TABLES:
            try:
                return self._save_codelist(dataset, frame)
            except THSClickHouseSchemaError:
                return self._save_raw(dataset, frame, unique_fields)
        if dataset in CANONICAL_TABLES:
            try:
                return self._save_canonical(dataset, frame)
            except THSClickHouseSchemaError:
                # 标准表不存在或字段不兼容时，回退到 ths_raw，
                # 避免本地空库联调被硬失败打断。
                return self._save_raw(dataset, frame, unique_fields)
        return self._save_raw(dataset, frame, unique_fields)

    def _save_codelist(
        self,
        dataset: str,
        frame: pd.DataFrame,
    ) -> int:
        """将证券清单写入 ``stock_cn_codelist`` 等码表。"""

        table_name = CODELIST_TABLES[dataset]
        if not self._table_exists(table_name):
            raise THSClickHouseSchemaError(f"码表不存在: {table_name}")
        normalized = self._codelist_frame(dataset, frame)
        table_columns = self._table_columns(table_name)
        if "order_book_id" not in table_columns:
            raise THSClickHouseSchemaError(
                f"{table_name} 缺少必要字段 order_book_id"
            )
        for column in table_columns:
            if column in normalized.columns:
                continue
            if column in {"market_tplus"}:
                normalized[column] = 1
            elif column in {"round_lot", "issue_price"}:
                normalized[column] = 0.0
            else:
                normalized[column] = ""
        normalized = normalized.loc[:, table_columns].drop_duplicates(
            subset=["order_book_id"],
            keep="last",
        )
        # 清单为全量快照：先清空再写入，避免旧代码残留。
        self.client.execute(f"TRUNCATE TABLE {table_name}")
        if normalized.empty:
            return 0
        columns_sql = ", ".join(table_columns)
        values = [
            tuple(_to_clickhouse_value(value) for value in row)
            for row in normalized.itertuples(index=False, name=None)
        ]
        self.client.execute(
            f"INSERT INTO {table_name} ({columns_sql}) VALUES",
            values,
        )
        return len(normalized)

    @staticmethod
    def _codelist_frame(
        dataset: str,
        frame: pd.DataFrame,
    ) -> pd.DataFrame:
        """将 THS 清单字段映射为 ``stock_cn_codelist`` 结构。"""

        del dataset
        result = frame.reset_index(drop=True).copy()
        source_field = (
            "ths_code"
            if "ths_code" in result.columns
            else ("thscode" if "thscode" in result.columns else "code")
        )
        if source_field not in result.columns:
            raise THSClickHouseSchemaError("股票清单缺少证券代码字段")
        result["order_book_id"] = result[source_field].astype(str).map(
            stock_format
        )
        if "code" in result.columns:
            result["trading_code"] = result["code"].astype(str)
        else:
            result["trading_code"] = (
                result["order_book_id"].astype(str).str.split(".").str[0]
            )
        for name_field in ("p03291_f003", "symbol", "name", "sec_name"):
            if name_field in result.columns:
                result["symbol"] = result[name_field].astype(str)
                break
        else:
            result["symbol"] = result["trading_code"]
        if "p03291_f004" in result.columns:
            result["abbrev_symbol"] = result["p03291_f004"].astype(str)
        else:
            result["abbrev_symbol"] = result["symbol"]
        if "p03291_f001" in result.columns:
            listed = pd.to_datetime(result["p03291_f001"], errors="coerce")
            result["listed_date"] = listed.dt.strftime("%Y-%m-%d").fillna(
                "0000-00-00"
            )
        elif "listed_date" not in result.columns:
            result["listed_date"] = "0000-00-00"
        result["de_listed_date"] = "0000-00-00"
        result["exchange"] = result["order_book_id"].map(
            lambda code: (
                "XSHG"
                if str(code).endswith(".XSHG")
                else ("XSHE" if str(code).endswith(".XSHE") else "XBEI")
            )
        )
        result["status"] = "Active"
        result["type"] = "CS"
        result["special_type"] = "Normal"
        result["market_tplus"] = 1
        result["round_lot"] = 100.0
        result["issue_price"] = 0.0
        result["industry_code"] = ""
        result["industry_name"] = ""
        result["sector_code"] = ""
        result["sector_code_name"] = ""
        result["board_type"] = ""
        result["trading_hours"] = ""
        result["purchasedate"] = ""
        return result

    def _save_canonical(
        self,
        dataset: str,
        frame: pd.DataFrame,
    ) -> int:
        """将标准行情写入 QAClickhouse 已使用的业务表。"""

        table_name, time_field = CANONICAL_TABLES[dataset]
        if not self._table_exists(table_name):
            raise THSClickHouseSchemaError(f"标准表不存在: {table_name}")
        normalized = self._canonical_frame(dataset, frame, time_field)
        table_columns = self._table_columns(table_name)
        table_column_set = set(table_columns)
        required = {"order_book_id", time_field}
        if not required.issubset(table_column_set):
            raise THSClickHouseSchemaError(
                f"{table_name} 缺少必要字段: "
                f"{sorted(required - table_column_set)}"
            )
        insert_columns = [
            column for column in table_columns if column in normalized.columns
        ]
        if not required.issubset(set(insert_columns)):
            raise THSClickHouseSchemaError(
                f"THS 数据无法生成 {table_name} 的必要字段"
            )
        normalized = normalized.loc[:, insert_columns]
        # 过滤 OHLC 全空的无效行。
        ohlc = [
            column
            for column in ("open", "high", "low", "close")
            if column in normalized.columns
        ]
        if ohlc:
            normalized = normalized.dropna(subset=ohlc, how="all")
        if dataset in _STOCK_INT_DATASETS:
            # _canonical_frame 已将价格×PRICE_SCALE、成交额/量转为整数
            for column in list(_PRICE_INT_COLUMNS) + list(_AMOUNT_INT_COLUMNS):
                if column in normalized.columns:
                    normalized[column] = (
                        pd.to_numeric(normalized[column], errors="coerce")
                        .fillna(0)
                        .round()
                        .astype("int64")
                    )
        else:
            for column in normalized.columns:
                if column in {
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "total_turnover",
                    "num_trades",
                    "limit_up",
                    "limit_down",
                    "open_interest",
                    "settlement",
                    "prev_settlement",
                    "iopv",
                }:
                    normalized[column] = (
                        pd.to_numeric(normalized[column], errors="coerce")
                        .fillna(0.0)
                        .astype("float64")
                    )
            price_round = [
                column
                for column in (
                    "open",
                    "high",
                    "low",
                    "close",
                    "limit_up",
                    "limit_down",
                )
                if column in normalized.columns
            ]
            if price_round:
                normalized[price_round] = normalized[price_round].round(2)
        normalized = self._drop_existing_rows(
            table_name,
            time_field,
            normalized,
        )
        if normalized.empty:
            return 0
        columns_sql = ", ".join(insert_columns)
        values = [
            tuple(
                _to_clickhouse_value(value)
                for value in row
            )
            for row in normalized.itertuples(index=False, name=None)
        ]
        # stock_cn_* 建议按月分区；单批控制在安全范围内。
        batch_size = 500
        insert_sql = f"INSERT INTO {table_name} ({columns_sql}) VALUES"
        for offset in range(0, len(values), batch_size):
            self.client.execute(insert_sql, values[offset : offset + batch_size])
        return len(normalized)

    def _save_raw(
        self,
        dataset: str,
        frame: pd.DataFrame,
        unique_fields: Sequence[str],
    ) -> int:
        """保存不能可靠映射到标准表的数据。"""

        self._ensure_table()
        now = pd.Timestamp.now(tz="Asia/Shanghai").replace(tzinfo=None)
        rows: list[dict[str, object]] = []
        records = frame_records(frame)
        available_fields = (
            list(unique_fields)
            if all(field in records[0] for field in unique_fields)
            else []
        )
        for record in records:
            rows.append(
                {
                    "dataset": dataset,
                    "record_id": record_id(
                        dataset,
                        record,
                        available_fields,
                    ),
                    "code": str(record.get("code", "")),
                    "event_time": event_time(record),
                    "payload": json.dumps(
                        record,
                        ensure_ascii=False,
                        sort_keys=True,
                        default=str,
                    ),
                    "updated_at": now,
                    "source": "ths_http",
                }
            )
        columns = [
            "dataset",
            "record_id",
            "code",
            "event_time",
            "payload",
            "updated_at",
            "source",
        ]
        values = [
            (
                str(row["dataset"]),
                str(row["record_id"]),
                str(row["code"]),
                pd.Timestamp(row["event_time"]).to_pydatetime(),
                str(row["payload"]),
                pd.Timestamp(row["updated_at"]).to_pydatetime(),
                str(row["source"]),
            )
            for row in rows
        ]
        self.client.execute(
            (
                f"INSERT INTO {self.TABLE_NAME} "
                f"({', '.join(columns)}) VALUES"
            ),
            values,
        )
        return len(rows)

    @staticmethod
    def _canonical_frame(
        dataset: str,
        frame: pd.DataFrame,
        time_field: str,
    ) -> pd.DataFrame:
        """转换为 QAClickhouse 标准字段。"""

        result = frame.reset_index(drop=True).copy()
        if "total_turnover" not in result.columns and "amount" in result.columns:
            result = result.rename(columns={"amount": "total_turnover"})
        if "open_interest" not in result.columns and "position" in result.columns:
            result = result.rename(columns={"position": "open_interest"})
        if time_field not in result.columns:
            raise THSClickHouseSchemaError(
                f"{dataset} 缺少时间字段 {time_field}"
            )
        parsed_time = pd.to_datetime(result[time_field], errors="coerce")
        if parsed_time.isna().any():
            raise THSClickHouseSchemaError(
                f"{dataset} 包含无法解析的 {time_field}"
            )
        result[time_field] = (
            parsed_time.dt.date if time_field == "date" else parsed_time
        )
        if dataset.startswith("future_"):
            if "code" not in result.columns:
                raise THSClickHouseSchemaError(f"{dataset} 缺少 code")
            result["order_book_id"] = (
                result["code"].astype(str).str.split(".").str[0].str.upper()
            )
        else:
            source_field = (
                "thscode" if "thscode" in result.columns else "code"
            )
            if source_field not in result.columns:
                raise THSClickHouseSchemaError(f"{dataset} 缺少证券代码")
            result["order_book_id"] = result[source_field].astype(str).map(
                stock_format
            )
        float_columns = (
            "open",
            "high",
            "low",
            "close",
            "volume",
            "total_turnover",
            "num_trades",
            "limit_up",
            "limit_down",
            "open_interest",
            "settlement",
            "prev_settlement",
            "iopv",
        )
        for column in float_columns:
            if column in result.columns:
                result[column] = pd.to_numeric(
                    result[column],
                    errors="coerce",
                ).astype("float64")
        if dataset in _STOCK_INT_DATASETS:
            for column in _PRICE_INT_COLUMNS:
                if column in result.columns:
                    result[column] = (
                        pd.to_numeric(result[column], errors="coerce")
                        .fillna(0.0)
                        .mul(PRICE_SCALE)
                        .round()
                        .astype("int64")
                    )
            for column in _AMOUNT_INT_COLUMNS:
                if column in result.columns:
                    result[column] = (
                        pd.to_numeric(result[column], errors="coerce")
                        .fillna(0.0)
                        .round()
                        .astype("int64")
                    )
        return result

    def _table_columns(self, table_name: str) -> list[str]:
        """读取标准表字段。"""

        result = self.client.execute(f"DESCRIBE TABLE {table_name}")
        if not isinstance(result, list) or not result:
            raise THSClickHouseSchemaError(
                f"无法读取 ClickHouse 表结构: {table_name}"
            )
        columns: list[str] = []
        for row in result:
            if isinstance(row, tuple) and row:
                columns.append(str(row[0]))
        if not columns:
            raise THSClickHouseSchemaError(
                f"ClickHouse 表结构为空: {table_name}"
            )
        return columns

    def _table_exists(self, table_name: str) -> bool:
        """判断标准表是否存在。"""

        database = "quantaxis"
        table = table_name
        if "." in table_name:
            database, table = table_name.split(".", maxsplit=1)
        result = self.client.execute(
            (
                "SELECT count() FROM system.tables "
                "WHERE database = %(database)s AND name = %(table)s"
            ),
            {"database": database, "table": table},
        )
        return bool(
            isinstance(result, list)
            and result
            and isinstance(result[0], tuple)
            and result[0]
            and result[0][0] is not None
            and int(result[0][0]) > 0
        )

    def _drop_existing_rows(
        self,
        table_name: str,
        time_field: str,
        frame: pd.DataFrame,
    ) -> pd.DataFrame:
        """按业务键过滤标准表中已经存在的记录。"""

        if frame.empty:
            return frame
        codes = tuple(frame["order_book_id"].astype(str).unique().tolist())
        start = frame[time_field].min()
        end = frame[time_field].max()
        result = self.client.execute(
            (
                f"SELECT order_book_id, {time_field} FROM {table_name} "
                f"WHERE {time_field} >= %(start)s "
                f"AND {time_field} <= %(end)s "
                "AND order_book_id IN %(codes)s"
            ),
            {"start": start, "end": end, "codes": codes},
        )
        existing: set[tuple[str, pd.Timestamp]] = set()
        if isinstance(result, list):
            for row in result:
                if isinstance(row, tuple) and len(row) >= 2:
                    existing.add((str(row[0]), pd.Timestamp(row[1])))
        keep_mask = [
            (str(code), pd.Timestamp(moment)) not in existing
            for code, moment in zip(
                frame["order_book_id"],
                frame[time_field],
            )
        ]
        return frame.loc[keep_mask].reset_index(drop=True)

    def latest(
        self,
        dataset: str,
        code: str,
        field: str,
    ) -> Optional[pd.Timestamp]:
        """查询指定证券的最新事件时间。

        Args:
            dataset: 逻辑数据集名称。
            code: 证券代码。
            field: 兼容存储协议的时间字段，原始表不直接使用。

        Returns:
            最新事件时间；无记录时返回 ``None``。
        """

        if dataset in CANONICAL_TABLES:
            table_name, time_field = CANONICAL_TABLES[dataset]
            if self._table_exists(table_name):
                order_book_id = self._format_lookup_code(dataset, code)
                result = self.client.execute(
                    (
                        f"SELECT max({time_field}) FROM {table_name} "
                        "WHERE order_book_id = %(order_book_id)s"
                    ),
                    {"order_book_id": order_book_id},
                )
                return self._timestamp_from_max_result(result)
        del field
        result = self.client.execute(
            (
                f"SELECT max(event_time) FROM {self.TABLE_NAME} "
                "WHERE dataset = %(dataset)s AND code = %(code)s"
            ),
            {"dataset": dataset, "code": code},
        )
        return self._timestamp_from_max_result(result)

    @staticmethod
    def _format_lookup_code(dataset: str, code: str) -> str:
        """将增量查询代码转换为标准表的 order_book_id。"""

        normalized = str(code).strip().upper()
        if dataset.startswith("future_"):
            return normalized.split(".")[0]
        if dataset.startswith("index_") and "." not in normalized:
            if normalized.startswith("399"):
                return normalized + ".XSHE"
            if normalized.startswith("899"):
                return normalized + ".XBEI"
            return normalized + ".XSHG"
        return stock_format(normalized)

    @staticmethod
    def _timestamp_from_max_result(
        result: object,
    ) -> Optional[pd.Timestamp]:
        """解析 ClickHouse max 时间查询结果。"""

        if not isinstance(result, list) or not result:
            return None
        row = result[0]
        if not isinstance(row, tuple) or not row or row[0] is None:
            return None
        stamp = pd.Timestamp(row[0])
        # ClickHouse 对空结果的 max(Date) 常返回默认纪元日，视为无数据。
        if stamp < pd.Timestamp("1970-01-02"):
            return None
        return stamp

    @staticmethod
    def _resolve_client(client: Optional[object]) -> ClickHouseClient:
        """解析 ClickHouse 客户端。

        Args:
            client: 客户端、资源管理器或 ``None``。

        Returns:
            可执行查询和 DataFrame 插入的客户端。
        """

        if (
            client is not None
            and not _is_pymongo_container(client)
            and _type_defines(client, "raw_client")
        ):
            raw_client = client.raw_client
            if _type_defines(raw_client, "insert_dataframe") or callable(
                getattr(raw_client, "insert_dataframe", None)
            ):
                return cast(ClickHouseClient, raw_client)
        if (
            client is not None
            and not _is_pymongo_container(client)
            and _type_defines(client, "insert_dataframe")
        ):
            return cast(ClickHouseClient, client)
        if (
            client is not None
            and not _is_pymongo_container(client)
            and _type_defines(client, "get_client")
        ):
            raw_client = client.get_client()
            return cast(ClickHouseClient, raw_client)
        from QUANTAXIS.QAUtil.QAResourceManager import (
            QAClickHouseResourceManager,
        )

        manager = QAClickHouseResourceManager()
        return cast(ClickHouseClient, manager.get_client())

    def ensure_stock_cn_5min(self) -> None:
        """创建 ``quantaxis.stock_cn_5min``（按月分区、价格 Int32×10000）。"""

        self.client.execute(STOCK_CN_5MIN_DDL)

    def ensure_stock_cn_1min(self) -> None:
        """创建 ``quantaxis.stock_cn_1min``（按月分区、价格 Int32×10000）。"""

        self.client.execute(STOCK_CN_1MIN_DDL)

    def ensure_stock_cn_day(self) -> None:
        """创建 ``quantaxis.stock_cn_day``（按月分区、价格 Int32×10000）。"""

        self.client.execute(STOCK_CN_DAY_DDL)

    def _ensure_table(self) -> None:
        """创建缺失的 ``ths_raw`` 表。"""

        self.client.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE_NAME}
            (
                dataset LowCardinality(String),
                record_id String,
                code String,
                event_time DateTime,
                payload String,
                updated_at DateTime,
                source LowCardinality(String)
            )
            ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (dataset, code, event_time, record_id)
            """
        )


def create_storage(
    client: object,
    backend: Optional[str] = None,
) -> StorageBackend:
    """创建 ClickHouse 存储后端。

    Args:
        client: ClickHouse 客户端、资源管理器或其他对象。
        backend: 可选后端名称，仅接受 ``clickhouse``。

    Returns:
        ClickHouse 存储实例。

    Raises:
        ValueError: 请求了非 ClickHouse 后端。
    """

    if backend is not None and backend.lower() != "clickhouse":
        raise ValueError(f"ClickHouse 模块不支持后端: {backend}")
    # 默认 client 常为 Mongo DATABASE；pymongo 的 hasattr 会伪造任意集合名，
    # 不能据此把它当成 ClickHouse 客户端。
    if client is not None and not _is_pymongo_container(client) and (
        _type_defines(client, "insert_dataframe")
        or _type_defines(client, "execute")
        or _type_defines(client, "raw_client")
        or _type_defines(client, "get_client")
    ):
        clickhouse_client: Optional[object] = client
    else:
        clickhouse_client = None
    return ClickHouseStorage(clickhouse_client)


_API: SaveAPI = SaveAPI(create_storage)

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
