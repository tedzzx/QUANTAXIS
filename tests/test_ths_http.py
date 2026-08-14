"""同花顺 QuantAPI HTTP 接入的离线回归测试。"""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest
from requests import Response

# QUANTAXIS 顶层包会启动 Web/QIFI 组件并访问 MongoDB；离线单元测试只需
# 子包，因此提供标准 package stub，避免测试收集产生外部副作用。
if "QUANTAXIS" not in sys.modules:
    quantaxis_package = ModuleType("QUANTAXIS")
    quantaxis_package.__path__ = [  # type: ignore[attr-defined]
        str(Path(__file__).resolve().parents[1] / "QUANTAXIS")
    ]
    sys.modules["QUANTAXIS"] = quantaxis_package

from QUANTAXIS.QAFetch import QAThs_http as ths
from QUANTAXIS.QAFetch.QAClickhouse import QACKClient, stock_format
from QUANTAXIS.QASU import main as qasu_main
from QUANTAXIS.QASU import save_ths, save_ths_clickhouse, save_ths_mongodb
from QUANTAXIS.QASU.save_ths_core import SaveAPI
from QUANTAXIS.QASU.save_ths_mongodb import MongoStorage


class FakeResponse:
    """最小 requests.Response 替身。"""

    def __init__(
        self,
        payload: Mapping[str, object],
        status_code: int = 200,
    ) -> None:
        self.payload = dict(payload)
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            response = Response()
            response.status_code = self.status_code
            response.raise_for_status()

    def json(self) -> dict[str, object]:
        return self.payload


class FakeSession:
    """记录 HTTP 调用并顺序返回预设响应。"""

    def __init__(self, responses: Sequence[FakeResponse]) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, object]] = []

    def post(self, url: str, **kwargs: object) -> FakeResponse:
        self.calls.append({"url": url, **kwargs})
        return self.responses.pop(0)


class FakeMongoCollection:
    """MongoStorage 测试替身。"""

    def __init__(self) -> None:
        self.indexes: list[tuple[Sequence[tuple[str, int]], bool]] = []
        self.operations: list[object] = []
        self.latest_record: dict[str, object] | None = None

    def create_index(
        self,
        keys: Sequence[tuple[str, int]],
        unique: bool = False,
        **kwargs: object,
    ) -> object:
        del kwargs
        self.indexes.append((keys, unique))
        return "index"

    def bulk_write(
        self,
        operations: Sequence[object],
        ordered: bool = False,
    ) -> object:
        del ordered
        self.operations.extend(operations)
        return object()

    def find_one(
        self,
        query: dict[str, object],
        sort: Sequence[tuple[str, int]],
    ) -> dict[str, object] | None:
        del query, sort
        return self.latest_record


class FakeMongoDatabase:
    """按名称创建测试集合。"""

    def __init__(self) -> None:
        self.collections: dict[str, FakeMongoCollection] = {}

    def __getattr__(self, name: str) -> FakeMongoCollection:
        return self.collections.setdefault(name, FakeMongoCollection())


class FakeClickHouse:
    """ClickHouseStorage 测试替身。"""

    def __init__(self) -> None:
        self.queries: list[str] = []
        self.inserted: pd.DataFrame | None = None
        self.existing_rows: list[tuple[object, ...]] = []
        self.tables: set[str] = {
            "quantaxis.stock_cn_day",
            "quantaxis.stock_cn_codelist",
        }

    def execute(
        self,
        query: str,
        params: object | None = None,
    ) -> object:
        self.queries.append(query)
        if query.startswith("SELECT count() FROM system.tables"):
            assert isinstance(params, dict)
            database = str(params.get("database", ""))
            table = str(params.get("table", ""))
            full_name = f"{database}.{table}" if database else table
            return [(1 if full_name in self.tables else 0,)]
        if query.startswith("DESCRIBE TABLE"):
            if "stock_cn_codelist" in query:
                return [
                    ("order_book_id", "String"),
                    ("industry_code", "String"),
                    ("market_tplus", "Int32"),
                    ("symbol", "String"),
                    ("special_type", "String"),
                    ("exchange", "String"),
                    ("status", "String"),
                    ("type", "String"),
                    ("de_listed_date", "String"),
                    ("listed_date", "String"),
                    ("sector_code_name", "String"),
                    ("abbrev_symbol", "String"),
                    ("sector_code", "String"),
                    ("round_lot", "Float32"),
                    ("trading_hours", "String"),
                    ("board_type", "String"),
                    ("industry_name", "String"),
                    ("issue_price", "Float32"),
                    ("trading_code", "String"),
                    ("purchasedate", "String"),
                ]
            return [
                ("date", "Date"),
                ("datetime", "DateTime"),
                ("order_book_id", "LowCardinality(String)"),
                ("open", "Int32"),
                ("high", "Int32"),
                ("low", "Int32"),
                ("close", "Int32"),
                ("volume", "Int64"),
                ("total_turnover", "Int64"),
            ]
        if query.lstrip().startswith("TRUNCATE TABLE"):
            self.inserted = None
            return []
        if query.lstrip().startswith("INSERT INTO quantaxis.stock_cn_codelist"):
            assert isinstance(params, list)
            columns = [
                part.strip()
                for part in query[
                    query.index("(") + 1 : query.index(") VALUES")
                ].split(",")
            ]
            self.inserted = pd.DataFrame(
                [dict(zip(columns, row)) for row in params]
            )
            return len(params)
        if query.startswith("SELECT order_book_id"):
            return self.existing_rows
        if query.lstrip().startswith("INSERT INTO ths_raw"):
            assert isinstance(params, list)
            rows = [
                {
                    "dataset": row[0],
                    "record_id": row[1],
                    "code": row[2],
                    "event_time": row[3],
                    "payload": row[4],
                    "updated_at": row[5],
                    "source": row[6],
                }
                for row in params
            ]
            self.inserted = pd.DataFrame(rows)
            return len(rows)
        if query.lstrip().startswith("INSERT INTO quantaxis.stock_cn_day"):
            assert isinstance(params, list)
            # DESCRIBE 顺序: date, datetime, order_book_id, open...
            # 实际 insert 列来自 normalized ∩ table_columns
            columns = [
                part.strip()
                for part in query[
                    query.index("(") + 1 : query.index(") VALUES")
                ].split(",")
            ]
            self.inserted = pd.DataFrame(
                [dict(zip(columns, row)) for row in params]
            )
            return len(params)
        if query.lstrip().startswith("SELECT"):
            return [(None,)]
        return []

    def insert_dataframe(
        self,
        query: str,
        dataframe: pd.DataFrame,
    ) -> object:
        self.queries.append(query)
        self.inserted = dataframe.copy()
        return len(dataframe)


class FakeQACKRawClient:
    """QACKClient 公共写接口测试替身。"""

    def __init__(self) -> None:
        self.commands: list[tuple[str, object | None]] = []

    def query_dataframe(self, sql: str) -> pd.DataFrame:
        return pd.DataFrame([{"sql": sql}])

    def execute(
        self,
        sql: str,
        params: object | None = None,
    ) -> object:
        self.commands.append((sql, params))
        if sql.startswith("SELECT count()"):
            return [(1,)]
        if sql.startswith("DESCRIBE TABLE"):
            return [("date", "Date"), ("order_book_id", "String")]
        return []

    def insert_dataframe(
        self,
        sql: str,
        dataframe: pd.DataFrame,
    ) -> int:
        self.commands.append((sql, dataframe.copy()))
        return len(dataframe)


class FakeStorage:
    """增量保存流程测试替身。"""

    def __init__(self, latest: pd.Timestamp | None) -> None:
        self.latest_value = latest
        self.saved: pd.DataFrame | None = None
        self.dataset: str | None = None

    def save(
        self,
        dataset: str,
        frame: pd.DataFrame,
        unique_fields: Sequence[str],
    ) -> int:
        del unique_fields
        self.dataset = dataset
        self.saved = frame
        return len(frame)

    def latest(
        self,
        dataset: str,
        code: str,
        field: str,
    ) -> pd.Timestamp | None:
        del dataset, code, field
        return self.latest_value


def test_client_authenticates_and_builds_history_request() -> None:
    session = FakeSession(
        [
            FakeResponse({"data": {"access_token": "access-1"}}),
            FakeResponse({"errorcode": 0, "tables": []}),
        ]
    )
    client = ths.THSHttpClient(
        ths.THSHttpConfig(refresh_token="refresh"),
        session=session,  # type: ignore[arg-type]
    )

    result = client.history(
        ["000001"],
        ["open", "close"],
        "2026-01-01",
        "2026-01-02",
    )

    assert result["errorcode"] == 0
    auth_url = session.calls[0]["url"]
    assert isinstance(auth_url, str)
    assert auth_url.endswith("/get_access_token")
    request = session.calls[1]
    headers = request["headers"]
    assert isinstance(headers, dict)
    assert headers["access_token"] == "access-1"
    raw_body = request["data"]
    assert isinstance(raw_body, str)
    body = json.loads(raw_body)
    assert isinstance(body, dict)
    assert body["codes"] == "000001.SZ"


def test_client_refreshes_token_once_after_unauthorized() -> None:
    session = FakeSession(
        [
            FakeResponse({"data": {"access_token": "access-1"}}),
            FakeResponse({}, status_code=401),
            FakeResponse({"data": {"access_token": "access-2"}}),
            FakeResponse({"errorcode": 0, "tables": []}),
        ]
    )
    client = ths.THSHttpClient(
        ths.THSHttpConfig(refresh_token="refresh"),
        session=session,  # type: ignore[arg-type]
    )

    client.data_pool(ths.STOCK_UNIVERSE, date="2026-01-01")

    final_headers = session.calls[-1]["headers"]
    assert isinstance(final_headers, dict)
    assert final_headers["access_token"] == "access-2"
    raw_body = session.calls[-1]["data"]
    assert isinstance(raw_body, str)
    body = json.loads(raw_body)
    assert isinstance(body, dict)
    assert body["reportname"] == "p03425"
    assert len(session.calls) == 4


def test_loads_refresh_token_from_local_setting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "ths_http.ini"
    config_path.write_text(
        (
            "[THS]\n"
            "refresh_token = local-refresh\n"
            "base_url = https://example.test/api/v1\n"
            "timeout = 12\n"
            "language = en\n"
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("THS_CONFIG_FILE", str(config_path))
    monkeypatch.delenv("THS_REFRESH_TOKEN", raising=False)
    monkeypatch.setattr(
        ths.QASETTING,
        "get_config",
        lambda *args, **kwargs: kwargs["default_value"],
    )

    config = ths._load_config()

    assert config.refresh_token == "local-refresh"
    assert config.base_url == "https://example.test/api/v1"
    assert config.timeout == 12.0
    assert config.language == "en"


def test_response_to_frame_supports_nested_and_flat_tables() -> None:
    nested = ths.response_to_frame(
        {
            "tables": [
                {
                    "thscode": "000001.SZ",
                    "time": ["2026-01-01", "2026-01-02"],
                    "table": {"open": [1.0, 2.0], "close": [2.0, 3.0]},
                }
            ]
        }
    )
    flat = ths.response_to_frame(
        {
            "tables": {
                "p03291_f002": ["000001.SZ", "600000.SH"],
                "p03291_f003": ["平安银行", "浦发银行"],
            }
        }
    )

    assert nested["thscode"].tolist() == ["000001.SZ", "000001.SZ"]
    assert nested["close"].tolist() == [2.0, 3.0]
    assert flat["p03291_f002"].tolist() == ["000001.SZ", "600000.SH"]


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("000001", "000001.SZ"),
        ("600000", "600000.SH"),
        ("920116", "920116.BJ"),
        ("000001.SZ", "000001.SZ"),
    ],
)
def test_format_ths_code(code: str, expected: str) -> None:
    assert ths.format_ths_code(code) == expected


def test_format_ths_index_code_uses_index_market_rules() -> None:
    assert ths.format_ths_code("000001", ths.THSAsset.INDEX) == "000001.SH"
    assert ths.format_ths_code("399001", ths.THSAsset.INDEX) == "399001.SZ"


def test_qaclickhouse_code_format_supports_all_main_markets() -> None:
    assert stock_format("600000") == "600000.XSHG"
    assert stock_format("000001.SZ") == "000001.XSHE"
    assert stock_format("920116.BJ") == "920116.XBEI"


def test_qaclickhouse_exposes_compatible_write_contract() -> None:
    raw_client = FakeQACKRawClient()
    client = QACKClient.__new__(QACKClient)
    client.client = raw_client
    frame = pd.DataFrame(
        [{"date": "2026-01-01", "order_book_id": "000001.XSHE"}]
    )

    count = client.insert_dataframe(
        "INSERT INTO stock_cn_day (date, order_book_id) VALUES",
        frame,
    )

    assert count == 1
    assert client.table_exists("quantaxis.stock_cn_day")
    assert client.get_table_columns("stock_cn_day") == [
        "date",
        "order_book_id",
    ]


def test_ambiguous_bond_code_requires_market_suffix() -> None:
    with pytest.raises(ths.THSConfigError, match="必须包含同花顺市场后缀"):
        ths.format_ths_code("110030", ths.THSAsset.BOND)


def test_unconfigured_asset_universe_fails_explicitly() -> None:
    with pytest.raises(ths.THSConfigError, match="清单报表未配置"):
        ths.QA_fetch_get_asset_list(ths.THSAsset.INDEX)


def test_mongo_storage_builds_idempotent_upserts() -> None:
    database = FakeMongoDatabase()
    storage = save_ths.MongoStorage(database)
    frame = pd.DataFrame(
        [
            {"code": "000001", "date": "2026-01-01", "close": 10.0},
            {"code": "000001", "date": "2026-01-02", "close": 11.0},
        ]
    )

    count = storage.save("stock_day", frame, ("code", "date"))

    collection = database.stock_day
    assert count == 2
    assert len(collection.operations) == 2
    assert any(unique for _, unique in collection.indexes)


def test_clickhouse_storage_preserves_payload() -> None:
    client = FakeClickHouse()
    storage = save_ths.ClickHouseStorage(client)
    frame = pd.DataFrame(
        [{"code": "000001", "blockname": "银行", "weight": 1.0}]
    )

    count = storage.save("stock_block", frame, ("code", "blockname"))

    assert count == 1
    assert client.inserted is not None
    assert client.inserted.iloc[0]["dataset"] == "stock_block"
    assert '"weight": 1.0' in client.inserted.iloc[0]["payload"]


def test_clickhouse_storage_routes_stock_list_to_codelist() -> None:
    client = FakeClickHouse()
    storage = save_ths.ClickHouseStorage(client)
    frame = pd.DataFrame(
        [
            {
                "p03291_f001": "2026/07/29",
                "p03291_f002": "000001.SZ",
                "p03291_f003": "平安银行",
                "p03291_f004": "平安银行",
                "ths_code": "000001.SZ",
                "code": "000001",
                "type": "stock",
            }
        ]
    )

    count = storage.save("stock_list", frame, ("ths_code",))

    assert count == 1
    assert client.inserted is not None
    assert client.inserted.iloc[0]["order_book_id"] == "000001.XSHE"
    assert client.inserted.iloc[0]["symbol"] == "平安银行"
    assert client.inserted.iloc[0]["status"] == "Active"
    assert any(
        "TRUNCATE TABLE quantaxis.stock_cn_codelist" in query
        for query in client.queries
    )
    assert any(
        "INSERT INTO quantaxis.stock_cn_codelist" in query
        for query in client.queries
    )


def test_clickhouse_storage_routes_market_data_to_canonical_table() -> None:
    client = FakeClickHouse()
    storage = save_ths.ClickHouseStorage(client)
    frame = pd.DataFrame(
        [
            {
                "thscode": "000001.SZ",
                "code": "000001",
                "date": "2026-01-01",
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "volume": 1000.0,
                "amount": 10000.0,
            }
        ]
    )

    count = storage.save("stock_day", frame, ("code", "date"))

    assert count == 1
    assert client.inserted is not None
    assert client.inserted.iloc[0]["order_book_id"] == "000001.XSHE"
    assert "total_turnover" in client.inserted.columns
    # 价格 ×10000 定点；成交额为元整数
    assert int(client.inserted.iloc[0]["close"]) == 105000
    assert int(client.inserted.iloc[0]["total_turnover"]) == 10000
    assert any(
        "INSERT INTO quantaxis.stock_cn_day" in query
        for query in client.queries
    )


def test_clickhouse_storage_filters_existing_business_keys() -> None:
    client = FakeClickHouse()
    client.existing_rows = [("000001.XSHE", "2026-01-01")]
    storage = save_ths.ClickHouseStorage(client)
    frame = pd.DataFrame(
        [
            {
                "thscode": "000001.SZ",
                "code": "000001",
                "date": "2026-01-01",
                "close": 10.0,
            },
            {
                "thscode": "000001.SZ",
                "code": "000001",
                "date": "2026-01-02",
                "close": 11.0,
            },
        ]
    )

    count = storage.save("stock_day", frame, ("code", "date"))

    assert count == 1
    assert client.inserted is not None
    assert str(client.inserted.iloc[0]["date"]) == "2026-01-02"


def test_ths_save_modules_export_the_same_public_api() -> None:
    facade_names = {
        name for name in dir(save_ths) if name.startswith("QA_SU_save_")
    }
    mongo_names = {
        name
        for name in dir(save_ths_mongodb)
        if name.startswith("QA_SU_save_")
    }
    clickhouse_names = {
        name
        for name in dir(save_ths_clickhouse)
        if name.startswith("QA_SU_save_")
    }

    assert facade_names == mongo_names == clickhouse_names


def test_mongodb_parallel_configuration_is_bounded() -> None:
    api = save_ths_mongodb.MongoSaveAPI()

    api.configure_parallel(True, max_workers=2)
    result = api._parallel_sum(["a", "b", "c"], lambda code: len(code))

    assert result == 3
    assert api.parallel
    assert api.max_workers == 2


def test_single_day_uses_incremental_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = FakeStorage(pd.Timestamp("2026-01-01"))
    captured: dict[str, str] = {}

    def fake_fetch(
        code: str,
        start: str,
        end: str,
        asset: ths.THSAsset,
    ) -> pd.DataFrame:
        del asset, code, end
        captured["start"] = start
        return pd.DataFrame([{"code": "000001", "date": "2026-01-02"}])

    api = SaveAPI(lambda client, backend: storage)
    monkeypatch.setattr(ths, "QA_fetch_get_asset_day", fake_fetch)

    count = api.QA_SU_save_single_stock_day("000001")

    assert count == 1
    assert captured["start"].startswith("2026-01-02")


def test_etf_uses_existing_index_collections() -> None:
    database = FakeMongoDatabase()
    storage = MongoStorage(database)
    frame = pd.DataFrame(
        [{"code": "510300", "date": "2026-01-02"}]
    )

    count = storage.save("etf_day", frame, ("code", "date"))

    assert count == 1
    assert len(database.index_day.operations) == 1


def test_qasu_selects_ths_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    selected: list[str] = []
    monkeypatch.setattr(
        save_ths,
        "configure_backend",
        lambda backend: selected.append(backend),
    )

    engine = qasu_main.select_save_engine(
        "ths_http",
        backend="clickhouse",
    )

    assert engine is save_ths
    assert selected == ["clickhouse"]
