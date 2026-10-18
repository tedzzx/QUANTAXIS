# ruff: noqa: N999
import re
from typing import Optional

import clickhouse_driver
import pandas as pd
from qaenv import (clickhouse_ip, clickhouse_password, clickhouse_port,
                   clickhouse_user)
from QUANTAXIS.QAData import (QA_DataStruct_Future_day,
                              QA_DataStruct_Future_min,
                              QA_DataStruct_Index_day, QA_DataStruct_Index_min,
                              QA_DataStruct_Stock_day,
                              QA_DataStruct_Stock_min)
from QUANTAXIS.QAUtil import QA_util_get_real_date


# ClickHouse 股票表价格定点：元 × PRICE_SCALE 存 Int32；读侧再还原为元。
PRICE_SCALE = 10000
_STOCK_PRICE_COLS = (
    "open",
    "high",
    "low",
    "close",
    "limit_up",
    "limit_down",
)


def promise_list(x):
    """保证入参为 list：str -> [str]，其它原样返回。"""

    return [x] if isinstance(x, str) else x


def unscale_stock_prices(frame: pd.DataFrame, columns=None) -> pd.DataFrame:
    """将 Int×PRICE_SCALE 价格列还原为元（原地修改并返回）。"""

    cols = [
        c
        for c in (columns if columns is not None else _STOCK_PRICE_COLS)
        if c in frame.columns
    ]
    if cols:
        frame[cols] = frame[cols] / float(PRICE_SCALE)
    return frame


def stock_format(code: str) -> str:
    """转换为 QAClickhouse 使用的 order_book_id。

    Args:
        code: 六位证券代码或带交易所后缀的代码。

    Returns:
        使用 XSHG、XSHE 或 XBEI 后缀的证券代码。
    """

    normalized = str(code).strip().upper()
    if normalized.endswith((".XSHG", ".XSHE", ".XBEI")):
        return normalized
    suffix_map = {".SH": ".XSHG", ".SZ": ".XSHE", ".BJ": ".XBEI"}
    for source_suffix, target_suffix in suffix_map.items():
        if normalized.endswith(source_suffix):
            return normalized.removesuffix(source_suffix) + target_suffix
    if normalized[:2] in {"43", "83", "87", "92"}:
        return normalized + ".XBEI"
    if normalized[0] in {"5", "6", "9"} or normalized[:3] in {
        "009",
        "110",
        "126",
        "201",
        "202",
        "203",
        "204",
    }:
        return normalized + ".XSHG"
    return normalized + ".XSHE"


def index_format(code: str) -> str:
    """指数代码补全交易所后缀（上证/中证常见 .XSHG，深证 399/15x .XSHE）。"""

    normalized = str(code).strip().upper()
    if normalized.endswith((".XSHG", ".XSHE", ".XBEI", ".INDX", ".RI")):
        return normalized
    if normalized.startswith("399") or normalized.startswith(("15", "16", "18")):
        return normalized + ".XSHE"
    return normalized + ".XSHG"


class QACKClient():
    def __init__(self, host=clickhouse_ip, port=clickhouse_port,  database='quantaxis', user=clickhouse_user, password=clickhouse_password):
        self.client = clickhouse_driver.Client(host=host, port=port,  database=database, user=user, password=password,
                                               settings={
                                                   'insert_block_size': 100000000},
                                               compression=True)

    def execute(self, sql):
        return self.client.query_dataframe(sql)

    def query_dataframe(self, sql: str) -> pd.DataFrame:
        """执行查询并返回 DataFrame，语义与现有 execute 一致。"""

        return self.client.query_dataframe(sql)

    def execute_command(
        self,
        sql: str,
        params: Optional[object] = None,
    ) -> object:
        """执行建表、插入或其他非 DataFrame 命令。"""

        return self.client.execute(sql, params)

    def insert_dataframe(
        self,
        sql: str,
        dataframe: pd.DataFrame,
    ) -> int:
        """批量写入 DataFrame。

        Args:
            sql: 带显式列名的 INSERT SQL。
            dataframe: 列顺序与 SQL 一致的数据。

        Returns:
            写入行数。
        """

        result = self.client.insert_dataframe(sql, dataframe)
        return result if isinstance(result, int) else len(dataframe)

    def table_exists(self, table_name: str) -> bool:
        """判断当前数据库是否存在目标表。"""

        database, table = self._split_table_name(table_name)
        result = self.client.execute(
            (
                "SELECT count() FROM system.tables "
                "WHERE database = %(database)s AND name = %(table)s"
            ),
            {"database": database, "table": table},
        )
        return bool(result and result[0][0])

    def get_table_columns(self, table_name: str) -> list[str]:
        """读取目标表的字段顺序。"""

        safe_name = self._validate_table_name(table_name)
        result = self.client.execute(f"DESCRIBE TABLE {safe_name}")
        return [str(row[0]) for row in result]

    @property
    def raw_client(self) -> object:
        """返回 clickhouse-driver 原始客户端。"""

        return self.client

    @staticmethod
    def _validate_table_name(table_name: str) -> str:
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]*", table_name) is None:
            raise ValueError(f"非法 ClickHouse 表名: {table_name}")
        return table_name

    def _split_table_name(self, table_name: str) -> tuple[str, str]:
        safe_name = self._validate_table_name(table_name)
        if "." in safe_name:
            database, table = safe_name.split(".", maxsplit=1)
            return database, table
        return "quantaxis", safe_name

    def to_qfq(self, res):
        u = res.data.reset_index()
        u = u.assign(date=u.datetime.apply(lambda x: x.date()))
        u = u.set_index(['date', 'code'], drop=False)
        code = u.index.levels[1].unique().tolist()

        start = u.index.levels[0][0]
        end = u.index.levels[0][-1]
        adjx = self.get_stock_adj(code, start, end)
        if adjx is None:
            data = u.set_index(['datetime', 'code'])
        else:

            adjx = adjx.reset_index()
            adjx = adjx.assign(code=adjx.order_book_id).set_index(
                ['date', 'code']).adj
            data = u.join(adjx).set_index(['datetime', 'code']).fillna(1)

            for col in ['open', 'high', 'low', 'close']:
                data[col] = data[col] * data['adj']

            try:
                data['high_limit'] = data['high_limit'] * data['adj']
                data['low_limit'] = data['high_limit'] * data['adj']
            except:
                pass

        return QA_DataStruct_Stock_min(data.sort_index(), if_fq='qfq')

    def make_ret5(self, data):
        """
        use open data make ret
        """
        r = data.groupby(level=1).open.apply(
            lambda x: x.pct_change(5).shift(-6))
        r.name = 'ret5'
        return r

    def make_ret_adjust(self, data):
        r = data.groupby(level=1).open.apply(lambda x: x.pct_change())
        return r

    def get_stock_industry(self, code, start, end):
        codex = []
        if isinstance(code, list):
            pass
        else:
            code = [code]
        start = QA_util_get_real_date(start)
        end = QA_util_get_real_date(end)
        for coder in code:
            if '.' in coder:
                codex.append(coder)
            else:
                codex.append(stock_format(coder))

        res = self.execute("SELECT * FROM quantaxis.citis_industry  WHERE ((`date` >= '{}')) AND (`date` <= '{}') AND (`order_book_id` IN ({}))".format(
            start, end, "'{}'".format("','".join(codex)))).drop_duplicates(['date', 'order_book_id'])
        return res

    def get_index_weight(self, code, start, end):
        codes = promise_list(code)
        start = QA_util_get_real_date(start)
        end = QA_util_get_real_date(end)

        # 权重按月更新；对齐月初方便区间覆盖
        start = start[0:8] + '01'
        end = end[0:8] + '01'
        codex = [index_format(coder) for coder in codes]

        res = self.execute("SELECT * FROM quantaxis.index_weight  WHERE ((`date` >= '{}')) AND (`date` <= '{}') AND (`index_code` IN ({}))".format(
            start, end, "'{}'".format("','".join(codex)))).drop_duplicates(['date', 'index_code', 'order_book_id'])
        return res

    def get_index_components(self, code, start, end):
        """读取 ``quantaxis.index_components``（对齐 get_index_weight 查询形态）。"""

        codes = promise_list(code)
        start = QA_util_get_real_date(start)
        end = QA_util_get_real_date(end)
        codex = [index_format(coder) for coder in codes]

        res = self.execute(
            "SELECT * FROM quantaxis.index_components WHERE "
            "((`date` >= '{}')) AND (`date` <= '{}') AND (`index_code` IN ({}))".format(
                start, end, "'{}'".format("','".join(codex))
            )
        ).drop_duplicates(['date', 'index_code', 'order_book_id'])
        return res

    def get_stock_adj(self, code, start, end):
        codex = []
        if isinstance(code, list):
            pass
        else:
            code = [code]

        start = QA_util_get_real_date(start)
        end = QA_util_get_real_date(end)

        for coder in code:
            if '.' in coder:
                codex.append(coder)
            else:
                codex.append(stock_format(coder))
        res = self.execute("SELECT * FROM quantaxis.stock_adj  WHERE ((`date` >= '{}')) AND (`date` <= '{}') AND (`order_book_id` IN ({}))".format(
            start, end, "'{}'".format("','".join(codex)))).drop_duplicates(['date', 'order_book_id'])
        #res = res.assign(code = res.code.apply(lambda x: x[0:6]))
        return res.set_index(['date', 'order_book_id']).sort_index()

    def get_stock_day_qfq(self, code, start, end):
        codes = promise_list(code)
        # 与 get_stock_day 一致：六位码需转成 688813.XSHG / 000001.XSHE
        if codes and '.X' not in str(codes[0]):
            codes = pd.Series(codes).astype(str).apply(stock_format).tolist()
        adjx = self.get_stock_adj(codes, start, end)

        columns_raw = ['date', 'order_book_id', 'num_trades', 'limit_up',
                       'limit_down', 'open', 'high', 'low', 'close', 'volume', 'total_turnover']
        u = self.execute("SELECT * FROM quantaxis.stock_cn_day  WHERE ((`date` >= '{}')) \
                         AND (`date` <= '{}') AND (`order_book_id` IN ({}))".format(start, end, "'{}'".format("','".join(codes)))).loc[:, columns_raw].drop_duplicates(['date', 'order_book_id'])
        unscale_stock_prices(u)

        u = u.set_index(['date', 'order_book_id'], drop=False).sort_index()

        data = u.join(adjx).set_index(
            ['date', 'order_book_id']).sort_index().fillna(1)

        for col in ['open', 'high', 'low', 'close']:
            data[col] = data[col] * data['adj']

        try:
            data['limit_up'] = data['limit_up'] * data['adj']
            data['limit_down'] = data['limit_down'] * data['adj']
        except:
            pass

        return data.sort_index()

    def get_stock_min_qfq_with_fields(self, code, start, end, fields):
        codes = promise_list(code)
        fields = promise_list(fields)
        if codes and '.X' not in str(codes[0]):
            codes = pd.Series(codes).apply(stock_format).tolist()
        columns_raw = ['datetime', 'order_book_id'].extend(fields)

        res = self.client.query_dataframe("SELECT datetime, order_book_id, {} FROM quantaxis.stock_cn_1min  WHERE ((`datetime` >= '{}')) \
                         AND (`datetime` <= '{}') AND (`order_book_id` IN ({}))".format(','.join(fields),
                         start, end, "'{}'".format("','".join(codes)))).drop_duplicates(['datetime', 'order_book_id'])
        unscale_stock_prices(res, [c for c in fields if c in _STOCK_PRICE_COLS])

        u = res.assign(datetime=pd.to_datetime(res.datetime), code=res.order_book_id)
        u = u.assign(date=u.datetime.apply(lambda x: x.date()))
        u = u.set_index(['date', 'code'], drop=False)
        codes = u.index.levels[1].unique().tolist()

        start = u.index.levels[0][0]
        end = u.index.levels[0][-1]
        adjx = self.get_stock_adj(codes, start, end)
        if adjx is None:
            data = u.set_index(['datetime', 'code'])
        else:

            adjx = adjx.reset_index()
            adjx = adjx.assign(code=adjx.order_book_id).set_index(
                ['date', 'code']).adj
            data = u.join(adjx).set_index(['datetime', 'code']).fillna(1)

            for col in ['open', 'high', 'low', 'close']:
                if col in fields:
                    data[col] = data[col] * data['adj']

            try:
                data['high_limit'] = data['high_limit'] * data['adj']
                data['low_limit'] = data['high_limit'] * data['adj']
            except:
                pass

        return data.loc[:, fields].sort_index()

    def get_stock_day_qfq_with_fields(self, code, start, end, fields=None):
        codes = promise_list(code)
        fields = promise_list(fields)
        if codes and '.X' not in str(codes[0]):
            codes = pd.Series(codes).astype(str).apply(stock_format).tolist()
        adjx = self.get_stock_adj(codes, start, end)

        columns_raw = ['date', 'order_book_id'].extend(fields)
        print("SELECT date, order_book_id, {} FROM quantaxis.stock_cn_day  WHERE ((`date` >= '{}')) \
                         AND (`date` <= '{}') AND (`order_book_id` IN ({}))".format(','.join(fields),
                         start, end, "'{}'".format("','".join(codes))))

        u = self.execute("SELECT date, order_book_id, {} FROM quantaxis.stock_cn_day  WHERE ((`date` >= '{}')) \
                         AND (`date` <= '{}') AND (`order_book_id` IN ({}))".format(','.join(fields),
                         start, end, "'{}'".format("','".join(codes)))).drop_duplicates(['date', 'order_book_id'])
        unscale_stock_prices(u, [c for c in fields if c in _STOCK_PRICE_COLS])

        u = u.set_index(['date', 'order_book_id'], drop=False).sort_index()

        data = u.join(adjx).set_index(
            ['date', 'order_book_id']).sort_index().fillna(1)

        for col in ['open', 'high', 'low', 'close']:
            if col in fields:
                data[col] = data[col] * data['adj']

        try:
            data['limit_up'] = data['limit_up'] * data['adj']
            data['limit_down'] = data['limit_down'] * data['adj']
        except:
            pass

        return data.loc[:, fields].sort_index()

    def get_stock_day_date(self, code, start, end):
        print("SELECT order_book_id, date FROM quantaxis.stock_cn_day  WHERE ((`date` >= '{}'))  AND (`date` <= '{}') AND (`order_book_id` == '{}')".format(start, end, code))
        u = self.execute("SELECT order_book_id, date FROM quantaxis.stock_cn_day  WHERE ((`date` >= '{}')) \
                         AND (`date` <= '{}') AND (`order_book_id` == '{}')".format(start, end, code)).drop_duplicates()

        u = u.set_index('order_book_id').sort_values('date')
        return u

    def get_stock_min_datetime(self, code, start, end):

        u = self.execute("SELECT order_book_id, datetime FROM quantaxis.stock_cn_1min  WHERE ((`datetime` >= '{}')) \
                         AND (`datetime` <= '{}') AND (`order_book_id` == '{}')".format(start, end, code)).drop_duplicates().set_index('order_book_id').sort_values('datetime')
        return u

    #def get_stock_day_qfq()
    def get_stock_day_qfq_adv(self, code, start, end):

        res = self.get_stock_day_qfq(code, start, end).reset_index()
        return QA_DataStruct_Stock_day(res.assign(amount=res.total_turnover, code=res.order_book_id).set_index(['date', 'code']), if_fq='qfq')

    def get_stock_min_qfq_adv(self, code, start, end):
        return self.to_qfq(self.get_stock_min(code, start, end))


    def get_stock_list(self):
        return self.client.query_dataframe('select * from stock_cn_codelist').query('status=="Active"')

    def get_fund_list(self):
        return self.client.query_dataframe('select * from fund_cn_codelist')

    def get_etf_components(self, code, start, end):
        codes = promise_list(code)
        columns_raw = ['stock_code', 'stock_amount', 'cash_substitute',
                       'cash_substitute_proportion', 'fixed_cash_substitute', 'order_book_id',
                       'trading_date']
        return self.client.query_dataframe("SELECT * FROM quantaxis.etf_components  WHERE ((`trading_date` >= '{}')) \
                                AND (`trading_date` <= '{}') AND (`order_book_id` IN ({}))".format(start, end, "'{}'".format("','".join(codes)))).loc[:, columns_raw].drop_duplicates(['stock_code', 'trading_date', 'order_book_id'])

    def get_stock_day(self, code, start, end):
        codes = promise_list(code)
        if codes and '.X' not in str(codes[0]):
            codes = pd.Series(codes).apply(stock_format).tolist()
        columns_raw = ['date', 'order_book_id', 'num_trades', 'limit_up',
                       'limit_down', 'open', 'high', 'low', 'close', 'volume', 'total_turnover']
        res = self.client.query_dataframe("SELECT * FROM quantaxis.stock_cn_day  WHERE ((`date` >= '{}')) \
                         AND (`date` <= '{}') AND (`order_book_id` IN ({}))".format(start, end, "'{}'".format("','".join(codes)))).loc[:, columns_raw].drop_duplicates(['date', 'order_book_id'])
        unscale_stock_prices(res)
        return QA_DataStruct_Stock_day(res.assign(date=pd.to_datetime(res.date), code=res.order_book_id, amount=res.total_turnover).set_index(['date', 'code']).sort_index())

    def get_stock_min(self, code, start, end, frequence: str = "1min"):
        """读取股票分钟线。

        Args:
            code: 证券代码或列表。
            start: 起始时间。
            end: 结束时间。
            frequence: ``1min`` 或 ``5min``（对应 stock_cn_1min / stock_cn_5min）。
        """

        table_map = {
            "1min": ("quantaxis.stock_cn_1min", "1min"),
            "5min": ("quantaxis.stock_cn_5min", "5min"),
        }
        if frequence not in table_map:
            raise ValueError(f"不支持的分钟频率: {frequence}，可选 {list(table_map)}")
        table_name, bar_type = table_map[frequence]
        codes = promise_list(code)
        if codes and '.X' not in str(codes[0]):
            codes = pd.Series(codes).apply(stock_format).tolist()
        columns_raw = ['datetime', 'order_book_id', 'open',
                       'high', 'low', 'close', 'volume', 'total_turnover']
        res = self.client.query_dataframe(
            "SELECT * FROM {} WHERE ((`datetime` >= '{}')) "
            "AND (`datetime` <= '{}') AND (`order_book_id` IN ({}))".format(
                table_name, start, end, "'{}'".format("','".join(codes))
            )
        )
        if res is None or getattr(res, "empty", True):
            empty = pd.DataFrame(columns=columns_raw)
            return QA_DataStruct_Stock_min(
                empty.assign(
                    datetime=pd.to_datetime([]),
                    code=pd.Series(dtype=str),
                    amount=pd.Series(dtype=float),
                    type=bar_type,
                ).set_index(["datetime", "code"])
            )
        res = res.loc[:, columns_raw].drop_duplicates(["datetime", "order_book_id"])
        unscale_stock_prices(res)
        return QA_DataStruct_Stock_min(
            res.assign(
                datetime=pd.to_datetime(res.datetime),
                code=res.order_book_id,
                amount=res.total_turnover,
                type=bar_type,
            ).set_index(["datetime", "code"]).sort_index()
        )

    def get_stock_5min(self, code, start, end):
        """读取股票 5 分钟线（``quantaxis.stock_cn_5min``）。"""

        return self.get_stock_min(code, start, end, frequence="5min")

    def get_stock_min_close(self, code, start, end, frequence: str = "1min"):
        codes = promise_list(code)
        if codes and '.X' not in str(codes[0]):
            codes = pd.Series(codes).apply(stock_format).tolist()
        table_map = {
            "1min": "quantaxis.stock_cn_1min",
            "5min": "quantaxis.stock_cn_5min",
        }
        if frequence not in table_map:
            raise ValueError(f"不支持的分钟频率: {frequence}")
        table_name = table_map[frequence]
        columns_raw = ['datetime', 'order_book_id', 'close']
        res = self.client.query_dataframe(
            "SELECT datetime, order_book_id, close FROM {} WHERE ((`datetime` >= '{}')) "
            "AND (`datetime` <= '{}') AND (`order_book_id` IN ({}))".format(
                table_name, start, end, "'{}'".format("','".join(codes))
            )
        )
        if res is None or getattr(res, "empty", True):
            return pd.DataFrame(columns=["close"]).set_index(
                pd.MultiIndex.from_arrays([[], []], names=["datetime", "order_book_id"])
            )
        res = res.loc[:, columns_raw].drop_duplicates(["datetime", "order_book_id"])
        unscale_stock_prices(res, ["close"])
        return res.assign(datetime=pd.to_datetime(res.datetime)).set_index(
            ["datetime", "order_book_id"]
        ).sort_index()

    def get_future_day(self, code, start, end):
        codes = promise_list(code)
        columns_raw = ['date', 'order_book_id', 'limit_up', 'limit_down', 'open_interest',
                       'prev_settlement', 'settlement', 'open', 'high', 'low', 'close',
                       'volume', 'total_turnover']

        res = self.client.query_dataframe("SELECT * FROM quantaxis.future_cn_day  WHERE ((`date` >= '{}')) \
                         AND (`date` <= '{}') AND (`order_book_id` IN ({}))".format(start, end, "'{}'".format("','".join(codes)))).loc[:, columns_raw].drop_duplicates(['date', 'order_book_id'])

        return QA_DataStruct_Future_day(res.assign(date=pd.to_datetime(res.date),
                                                   code=res.order_book_id,
                                                   amount=res.total_turnover,
                                                   position=res.open_interest,
                                                   price=res.settlement).set_index(['date', 'code']))

    def get_future_min(self, code, start, end):
        codes = promise_list(code)
        columns_raw = ['datetime', 'order_book_id', 'open_interest', 'trading_date',
                       'open', 'high', 'low', 'close',
                                   'volume', 'total_turnover']
        res = self.client.query_dataframe("SELECT * FROM quantaxis.future_cn_1min  WHERE ((`datetime` >= '{}')) \
                         AND (`datetime` <= '{}') AND (`order_book_id` IN ({}))".format(start, end, "'{}'".format("','".join(codes)))).loc[:, columns_raw].drop_duplicates(['datetime', 'order_book_id'])
        return QA_DataStruct_Future_min(res.assign(datetime=pd.to_datetime(res.datetime),
                                                   position=res.open_interest,
                                                   code=res.order_book_id,
                                                   tradetime=res.trading_date,
                                                   price=res.close,
                                                   type='1min',
                                                   amount=res.total_turnover).set_index(['datetime', 'code']).sort_index())

    def get_stock_tick(self, code, start, end):
        codes = promise_list(code)
        if codes and '.X' not in str(codes[0]):
            codes = pd.Series(codes).map(str).apply(stock_format).tolist()

        columns_raw = ['datetime', 'trading_date', 'order_book_id', 'open', 'last', 'high',
                       'low', 'prev_close', 'volume', 'total_turnover', 'limit_up',
                       'limit_down', 'a1', 'a2', 'a3', 'a4', 'a5', 'b1', 'b2', 'b3', 'b4',
                       'b5', 'a1_v', 'a2_v', 'a3_v', 'a4_v', 'a5_v', 'b1_v', 'b2_v', 'b3_v',
                       'b4_v', 'b5_v', 'change_rate']
        res = self.client.query_dataframe("SELECT * FROM quantaxis.stock_cn_tick  WHERE ((`datetime` >= '{}')) \
                         AND (`datetime` <= '{}') AND (`order_book_id` IN ({}))".format(start, end, "'{}'".format("','".join(codes)))).loc[:, columns_raw]
        return res.assign(datetime=pd.to_datetime(res.datetime), code=res.order_book_id, amount=res.total_turnover).set_index(['datetime', 'code']).sort_index()

    def get_index_tick(self, code, start, end):
        codes = promise_list(code)

        columns_raw = ['datetime', 'trading_date', 'order_book_id', 'open', 'last', 'high',
                       'low', 'prev_close', 'volume', 'total_turnover', 'limit_up',
                       'limit_down', 'a1', 'a2', 'a3', 'a4', 'a5', 'b1', 'b2', 'b3', 'b4',
                       'b5', 'a1_v', 'a2_v', 'a3_v', 'a4_v', 'a5_v', 'b1_v', 'b2_v', 'b3_v',
                       'b4_v', 'b5_v', 'change_rate']
        res = self.client.query_dataframe("SELECT * FROM quantaxis.index_cn_tick  WHERE ((`datetime` >= '{}')) \
                         AND (`datetime` <= '{}') AND (`order_book_id` IN ({}))".format(start, end, "'{}'".format("','".join(codes)))).loc[:, columns_raw].drop_duplicates(['datetime', 'order_book_id'])
        return res.assign(datetime=pd.to_datetime(res.datetime), code=res.order_book_id, amount=res.total_turnover).set_index(['datetime', 'code']).sort_index()

    def get_future_tick(self, code, start, end):
        codes = promise_list(code)
        columns_raw = ['datetime', 'trading_date', 'order_book_id', 'open', 'last', 'high',
                       'low', 'prev_settlement', 'prev_close', 'open_interest', 'volume',
                       'total_turnover', 'limit_up', 'limit_down', 'a1', 'a2', 'a3', 'a4',
                       'a5', 'b1', 'b2', 'b3', 'b4', 'b5', 'a1_v', 'a2_v', 'a3_v', 'a4_v',
                       'a5_v', 'b1_v', 'b2_v', 'b3_v', 'b4_v', 'b5_v', 'change_rate']
        res = self.client.query_dataframe("SELECT * FROM quantaxis.future_cn_tick  WHERE ((`datetime` >= '{}')) \
                         AND (`datetime` <= '{}') AND (`order_book_id` IN ({}))".format(start, end, "'{}'".format("','".join(codes)))).loc[:, columns_raw]
        return res.assign(datetime=pd.to_datetime(res.datetime), code=res.order_book_id, amount=res.total_turnover).set_index(['datetime', 'code']).sort_index()

    def get_index_day(self, code, start, end):
        codes = promise_list(code)
        # if 'XS' not in codes[0]:
        #     codes = pd.Series(codes).apply(
        #         lambda x: x+'.XSHG' if x[0] != 6 else x+'.XSHE').tolist()

        columns_raw = ['date', 'order_book_id', 'num_trades', 'open', 'high', 'low', 'close',
                       'volume', 'total_turnover']
        res = self.client.query_dataframe("SELECT * FROM quantaxis.index_cn_day  WHERE ((`date` >= '{}')) \
                         AND (`date` <= '{}') AND (`order_book_id` IN ({}))".format(start, end, "'{}'".format("','".join(codes)))).loc[:, columns_raw].drop_duplicates(['date', 'order_book_id'])
        unscale_stock_prices(res)
        return QA_DataStruct_Index_day(res.assign(date=pd.to_datetime(res.date), code=res.order_book_id, amount=res.total_turnover).set_index(['date', 'code']).sort_index())

    def get_index_min(self, code, start, end, frequence: str = "1min"):
        """读取指数分钟线。

        Args:
            code: 指数代码或列表。
            start: 起始时间。
            end: 结束时间。
            frequence: ``1min`` 或 ``5min``（对应 index_cn_1min / index_cn_5min）。
        """

        table_map = {
            "1min": ("quantaxis.index_cn_1min", "1min"),
            "5min": ("quantaxis.index_cn_5min", "5min"),
        }
        if frequence not in table_map:
            raise ValueError(f"不支持的分钟频率: {frequence}，可选 {list(table_map)}")
        table_name, bar_type = table_map[frequence]
        codes = promise_list(code)
        columns_raw = ['datetime', 'order_book_id',  'open',
                       'high', 'low', 'close', 'volume', 'total_turnover']
        res = self.client.query_dataframe(
            "SELECT * FROM {} WHERE ((`datetime` >= '{}')) "
            "AND (`datetime` <= '{}') AND (`order_book_id` IN ({}))".format(
                table_name, start, end, "'{}'".format("','".join(codes))
            )
        )
        if res is None or getattr(res, "empty", True):
            empty = pd.DataFrame(columns=columns_raw)
            return QA_DataStruct_Index_min(
                empty.assign(
                    datetime=pd.to_datetime([]),
                    code=pd.Series(dtype=str),
                    amount=pd.Series(dtype=float),
                    type=bar_type,
                ).set_index(["datetime", "code"])
            )
        res = res.loc[:, columns_raw].drop_duplicates(['datetime', 'order_book_id'])
        unscale_stock_prices(res)
        return QA_DataStruct_Index_min(res.assign(datetime=pd.to_datetime(res.datetime), code=res.order_book_id, amount=res.total_turnover, type=bar_type,).set_index(['datetime', 'code']).sort_index())

    def get_index_5min(self, code, start, end):
        """读取指数 5 分钟线（``quantaxis.index_cn_5min``）。"""

        return self.get_index_min(code, start, end, frequence="5min")

    def get_etf_day(self, code, start, end):
        codes = promise_list(code)
        if codes and ".X" not in str(codes[0]):
            codes = pd.Series(codes).apply(stock_format).tolist()

        columns_raw = ['date', 'order_book_id', 'num_trades', 'open', 'high', 'low', 'close',
                       'volume', 'total_turnover']
        res = self.client.query_dataframe("SELECT * FROM quantaxis.etf_cn_day  WHERE ((`date` >= '{}')) \
                         AND (`date` <= '{}') AND (`order_book_id` IN ({}))".format(start, end, "'{}'".format("','".join(codes))))
        if res is None or getattr(res, "empty", True):
            empty = pd.DataFrame(columns=columns_raw)
            return QA_DataStruct_Index_day(
                empty.assign(
                    date=pd.to_datetime([]),
                    code=pd.Series(dtype=str),
                    amount=pd.Series(dtype=float),
                ).set_index(["date", "code"])
            )
        res = res.loc[:, columns_raw].drop_duplicates(['date', 'order_book_id'])
        unscale_stock_prices(res)
        return QA_DataStruct_Index_day(res.assign(date=pd.to_datetime(res.date), code=res.order_book_id, amount=res.total_turnover).set_index(['date', 'code']).sort_index())

    def get_etf_min(self, code, start, end, frequence: str = "1min"):
        """读取 ETF 分钟线。

        Args:
            code: 证券代码或列表。
            start: 起始时间。
            end: 结束时间。
            frequence: ``1min`` 或 ``5min``（对应 etf_cn_1min / etf_cn_5min）。
        """

        table_map = {
            "1min": ("quantaxis.etf_cn_1min", "1min"),
            "5min": ("quantaxis.etf_cn_5min", "5min"),
        }
        if frequence not in table_map:
            raise ValueError(f"不支持的分钟频率: {frequence}，可选 {list(table_map)}")
        table_name, bar_type = table_map[frequence]
        codes = promise_list(code)
        if codes and ".X" not in str(codes[0]):
            codes = pd.Series(codes).apply(stock_format).tolist()

        columns_raw = ['datetime', 'order_book_id',  'open',
                       'high', 'low', 'close', 'volume', 'total_turnover']
        res = self.client.query_dataframe(
            "SELECT * FROM {} WHERE ((`datetime` >= '{}')) "
            "AND (`datetime` <= '{}') AND (`order_book_id` IN ({}))".format(
                table_name, start, end, "'{}'".format("','".join(codes))
            )
        )
        if res is None or getattr(res, "empty", True):
            empty = pd.DataFrame(columns=columns_raw)
            return QA_DataStruct_Index_min(
                empty.assign(
                    datetime=pd.to_datetime([]),
                    code=pd.Series(dtype=str),
                    amount=pd.Series(dtype=float),
                    type=bar_type,
                ).set_index(["datetime", "code"])
            )
        res = res.loc[:, columns_raw].drop_duplicates(['datetime', 'order_book_id'])
        unscale_stock_prices(res)
        return QA_DataStruct_Index_min(res.assign(datetime=pd.to_datetime(res.datetime), code=res.order_book_id, amount=res.total_turnover, type=bar_type,).set_index(['datetime', 'code']).sort_index())

    def get_etf_5min(self, code, start, end):
        """读取 ETF 5 分钟线（``quantaxis.etf_cn_5min``）。"""

        return self.get_etf_min(code, start, end, frequence="5min")

    def get_lof_day(self, code, start, end):
        codes = promise_list(code)
        # if 'XS' not in codes[0]:
        #     codes = pd.Series(codes).apply(
        #         lambda x: x+'.XSHG' if x[0] != 6 else x+'.XSHE').tolist()

        columns_raw = ['date', 'order_book_id', 'num_trades', 'open', 'high', 'low', 'close',
                       'volume', 'total_turnover']
        res = self.client.query_dataframe("SELECT * FROM quantaxis.lof_cn_day  WHERE ((`date` >= '{}')) \
                         AND (`date` <= '{}') AND (`order_book_id` IN ({}))".format(start, end, "'{}'".format("','".join(codes)))).loc[:, columns_raw].drop_duplicates(['date', 'order_book_id'])
        return QA_DataStruct_Index_day(res.assign(date=pd.to_datetime(res.date), code=res.order_book_id, amount=res.total_turnover).set_index(['date', 'code']).sort_index())

    def get_lof_min(self, code, start, end):
        codes = promise_list(code)
        # if 'XS' not in codes[0]:
        #     codes = pd.Series(codes).apply(
        #         lambda x: x+'.XSHG' if x[0] != 6 else x+'.XSHE').tolist()

        columns_raw = ['datetime', 'order_book_id',  'open',
                       'high', 'low', 'close', 'volume', 'total_turnover']
        res = self.client.query_dataframe("SELECT * FROM quantaxis.lof_cn_1min  WHERE ((`datetime` >= '{}')) \
                         AND (`datetime` <= '{}') AND (`order_book_id` IN ({}))".format(start, end, "'{}'".format("','".join(codes)))).loc[:, columns_raw].drop_duplicates(['datetime', 'order_book_id'])
        return QA_DataStruct_Index_min(res.assign(datetime=pd.to_datetime(res.datetime), code=res.order_book_id, amount=res.total_turnover, type='1min',).set_index(['datetime', 'code']).sort_index())
