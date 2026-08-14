#
# The MIT License (MIT)
#
# Copyright (c) 2016-2021 yutiansut/QUANTAXIS
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from types import ModuleType

from QUANTAXIS.QAFetch.QAQuery import QA_fetch_stock_list
# from QUANTAXIS.QASU import crawl_eastmoney as crawl_eastmoney_file
from QUANTAXIS.QASU import save_tdx as stdx
from QUANTAXIS.QASU import save_tdx_parallelism as stdx_parallelism
from QUANTAXIS.QASU import save_tdx_file as tdx_file
from QUANTAXIS.QASU import save_gm as sgm
from QUANTAXIS.QASU import save_jq as sjq
from QUANTAXIS.QASU import save_tushare as sts
from QUANTAXIS.QASU import save_ths as sths
from QUANTAXIS.QASU import save_financialfiles
from QUANTAXIS.QAUtil import DATABASE, print_used_time
import time


# from QUANTAXIS.QASU import crawl_jrj_financial_reportdate as save_financial_calendar
# from QUANTAXIS.QASU import crawl_jrj_stock_divyield as save_stock_divyield


def QA_SU_save_stock_info(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """save stock info

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_stock_info(client=client)


def QA_SU_save_stock_info_tushare(engine="tushare", client=DATABASE):
    '''

    :param engine: tushare
    :param client:
    :return: None
    '''

    # only support the tushare
    engine = select_save_engine("tushare")
    engine.QA_SU_save_stock_info_tushare()

    pass


def QA_SU_save_stock_list(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """save stock_list

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_stock_list(client=client)


def QA_SU_save_stock_list_tushare(client=DATABASE):
    """从 Tushare 获取并保存 stock_list（TDX 不可用时使用）"""
    from QUANTAXIS.QASU.save_tushare import QA_SU_save_stock_list_to_stock_list
    QA_SU_save_stock_list_to_stock_list(client=client)


def QA_SU_save_index_list(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """save index_list

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_index_list(client=client)


def QA_SU_save_etf_list(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """save etf_list

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_etf_list(client=client)


def QA_SU_save_future_list(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """save future_list

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_future_list(client=client)

def QA_SU_save_single_future_day(
    code: str,
    engine: str,
    client: object = DATABASE,
    paralleled: bool = False,
    backend: str = "mongodb",
) -> None:
    """save single_future_day

    Arguments:
        code: stock code
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})

    :param paralleled: 是否并行处理(default: {True})
    """

    engine = select_save_engine(
        engine,
        paralleled=paralleled,
        backend=backend,
    )
    engine.QA_SU_save_single_future_day(code=code, client=client)

def QA_SU_save_future_day(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """save future_day

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_future_day(client=client)


def QA_SU_save_future_day_all(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """save future_day_all

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_future_day_all(client=client)

def QA_SU_save_single_future_min(
    code: str,
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """save single_future_min

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_single_future_min(code=code, client=client)

def QA_SU_save_future_min(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """save future_min
    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_future_min(client=client)


def QA_SU_save_future_min_all(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """[summary]

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_future_min_all(client=client)


@print_used_time
def QA_SU_save_stock_day(
    engine: str,
    client: object = DATABASE,
    paralleled: bool = False,
    backend: str = "mongodb",
) -> None:
    """save stock_day

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})

    :param paralleled: 是否并行处理(default: {True})
    """

    engine = select_save_engine(
        engine,
        paralleled=paralleled,
        backend=backend,
    )
    engine.QA_SU_save_stock_day(client=client)


def QA_SU_save_single_stock_day(
    code: str,
    engine: str,
    client: object = DATABASE,
    paralleled: bool = False,
    backend: str = "mongodb",
) -> None:
    """save stock_day

    Arguments:
        code: stock code
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})

    :param paralleled: 是否并行处理(default: {True})
    """

    engine = select_save_engine(
        engine,
        paralleled=paralleled,
        backend=backend,
    )
    engine.QA_SU_save_single_stock_day(code=code, client=client)


def QA_SU_save_option_contract_list(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    '''

    :param engine: 
    :param client: 
    :return: 
    '''
    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_option_contract_list(client=client)


def QA_SU_save_option_day_all(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    '''

    :param engine:
    :param client:
    :return:
    '''
    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_option_day_all(client=client)


def QA_SU_save_option_min_all(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    '''

    :param engine:
    :param client:
    :return:
    '''
    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_option_min_all(client=client)


def QA_SU_save_option_50etf_day(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    '''

    :param engine:
    :param client:
    :return:
    '''
    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_option_50etf_day(client=client)


def QA_SU_save_option_50etf_min(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    '''

    :param engine:
    :param client:
    :return:
    '''
    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_option_50etf_min(client=client)



def QA_SU_save_option_300etf_day(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    '''

    :param engine:
    :param client:
    :return:
    '''
    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_option_300etf_day(client=client)


def QA_SU_save_option_300etf_min(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    '''

    :param engine:
    :param client:
    :return:
    '''
    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_option_300etf_min(client=client)


def QA_SU_save_option_commodity_min(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    '''
    :param engine:
    :param client:
    :return:
    '''
    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_option_commodity_min(client=client)


def QA_SU_save_option_commodity_day(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    '''
    :param engine:
    :param client:
    :return:
    '''
    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_option_commodity_day(client=client)


def QA_SU_save_stock_min(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """save stock_min

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_stock_min(client=client)


def QA_SU_save_stock_transaction(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """save stock_transaction

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_stock_transaction(client=client)


def QA_SU_save_index_transaction(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """save index_transaction

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_index_transaction(client=client)



def QA_SU_save_single_stock_min(
    code: str,
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """save stock_min

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_single_stock_min(code=code, client=client)


@print_used_time
def QA_SU_save_index_day(
    engine: str,
    client: object = DATABASE,
    paralleled: bool = False,
    backend: str = "mongodb",
) -> None:
    """save index_day

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})

    :param paralleled: 是否并行处理(default: {True})
    """

    engine = select_save_engine(
        engine,
        paralleled=paralleled,
        backend=backend,
    )
    engine.QA_SU_save_index_day(client=client)


def QA_SU_save_single_index_day(
    code: str,
    engine: str,
    client: object = DATABASE,
    paralleled: bool = False,
    backend: str = "mongodb",
) -> None:
    """save index_day

    Arguments:
        code: index code
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})

    :param paralleled: 是否并行处理(default: {True})
    """

    engine = select_save_engine(
        engine,
        paralleled=paralleled,
        backend=backend,
    )
    engine.QA_SU_save_single_index_day(code=code, client=client)


def QA_SU_save_index_min(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """save index_min

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_index_min(client=client)


def QA_SU_save_single_index_min(
    code: str,
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """save index_min

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_single_index_min(code=code, client=client)


@print_used_time
def QA_SU_save_etf_day(
    engine: str,
    client: object = DATABASE,
    paralleled: bool = False,
    backend: str = "mongodb",
) -> None:
    """save etf_day

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(
        engine,
        paralleled=paralleled,
        backend=backend,
    )
    engine.QA_SU_save_etf_day(client=client)


def QA_SU_save_single_etf_day(
    code: str,
    engine: str,
    client: object = DATABASE,
    paralleled: bool = False,
    backend: str = "mongodb",
) -> None:
    """save etf_day

    Arguments:
        code: etf code
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(
        engine,
        paralleled=paralleled,
        backend=backend,
    )
    engine.QA_SU_save_single_etf_day(code=code, client=client)


def QA_SU_save_etf_min(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """save etf_min

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_etf_min(client=client)


def QA_SU_save_single_etf_min(
    code: str,
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """save etf_min

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_single_etf_min(code=code, client=client)


def QA_SU_save_stock_xdxr(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """save stock_xdxr

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_stock_xdxr(client=client)


def QA_SU_save_stock_block(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """save stock_block

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_stock_block(client=client)


def select_save_engine(
    engine: str,
    paralleled: bool = False,
    backend: str = "mongodb",
) -> ModuleType:
    '''
    select save_engine
    :param engine: 字符串Str
    :param paralleled: 是否并行处理；默认为False
    :return: sts means save_tushare_py  or stdx means save_tdx_py
    '''
    if engine in ['tushare', 'ts', 'Tushare']:
        return sts
    # 默认将 'baostock' 映射到 TDX 保存模块之外，由调用方直接用 QAFetch/QABaostock
    elif engine in ['tdx']:
        if paralleled:
            return stdx_parallelism
        else:
            return stdx
    elif engine in ['gm', 'goldenminer']:
        return sgm
    elif engine in ['jq', 'joinquant']:
        return sjq
    elif engine in ['ths_http', 'ifind', 'ths']:
        sths.configure_backend(backend)
        sths.configure_parallel(paralleled)
        return sths
    else:
        raise ValueError(
            "QASU 不支持的数据源: "
            f"{engine}; 可选 tushare/tdx/gm/jq/ths_http"
        )


def QA_SU_save_stock_min_5(file_dir, client=DATABASE):
    """save stock_min5

    Arguments:
        file_dir {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})

    Returns:
        [type] -- [description]
    """

    return tdx_file.QA_save_tdx_to_mongo(file_dir, client)


def QA_SU_crawl_eastmoney(action="zjlx", stockCode=None):
    '''

    :param action: zjlx 后期支持其他的操作类型
    :param stockCode: 股票代码
    :return:
    '''
    stockItems = QA_fetch_stock_list()

    if stockCode == "all":
        # 读取tushare股票列表代码
        print("💪 一共需要获取 %d 个股票的 资金流向 , 需要大概 %d 小时" %
              (len(stockItems), (len(stockItems) * 5) / 60 / 60))

        code_list = []
        for stock in stockItems:
            code_list.append(stock['code'])
            # print(stock['code'])
        crawl_eastmoney_file.QA_read_eastmoney_zjlx_web_page_to_sqllite(
            code_list)
        # print(stock)

        return
    else:
        # todo 检查股票代码是否合法
        # return crawl_eastmoney_file.QA_read_eastmoney_zjlx_web_page_to_sqllite(stockCode=stockCode)
        code_list = []
        code_list.append(stockCode)
        return crawl_eastmoney_file.QA_request_eastmoney_zjlx(
            param_stock_code_list=code_list)


def QA_SU_save_financialfiles():
    return save_financialfiles.QA_SU_save_financial_files()

def QA_SU_save_financialfiles_fromtdx():
    return save_financialfiles.QA_SU_save_financial_files(fromtdx=True)

def QA_SU_save_report_calendar_day():
    return save_financial_calendar.QA_SU_save_report_calendar_day()


def QA_SU_save_report_calendar_his():
    return save_financial_calendar.QA_SU_save_report_calendar_his()


def QA_SU_save_stock_divyield_day(
    engine: str = "ths_http",
    client: object = DATABASE,
    backend: str = "mongodb",
) -> object:
    """保存股票分红送转数据。"""

    save_engine = select_save_engine(engine, backend=backend)
    return save_engine.QA_SU_save_stock_divyield_day(client=client)


def QA_SU_save_stock_divyield_his(
    engine: str = "ths_http",
    client: object = DATABASE,
    backend: str = "mongodb",
) -> object:
    """保存历史股票分红送转数据。"""

    save_engine = select_save_engine(engine, backend=backend)
    return save_engine.QA_SU_save_stock_divyield_his(client=client)


def QA_SU_save_bond_list(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """save bond_list

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_bond_list(client=client)


@print_used_time
def QA_SU_save_bond_day(
    engine: str,
    client: object = DATABASE,
    paralleled: bool = False,
    backend: str = "mongodb",
) -> None:
    """save bond_day

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})

    :param paralleled: 是否并行处理(default: {True})
    """

    engine = select_save_engine(
        engine,
        paralleled=paralleled,
        backend=backend,
    )
    engine.QA_SU_save_bond_day(client=client)


def QA_SU_save_single_bond_day(
    code: str,
    engine: str,
    client: object = DATABASE,
    paralleled: bool = False,
    backend: str = "mongodb",
) -> None:
    """save bond_day

    Arguments:
        code: bond code
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})

    :param paralleled: 是否并行处理(default: {True})
    """

    engine = select_save_engine(
        engine,
        paralleled=paralleled,
        backend=backend,
    )
    engine.QA_SU_save_single_bond_day(code=code, client=client)


def QA_SU_save_bond_min(
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """save bond_min

    Arguments:
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_bond_min(client=client)


def QA_SU_save_single_bond_min(
    code: str,
    engine: str,
    client: object = DATABASE,
    backend: str = "mongodb",
) -> None:
    """save bond_min

    Arguments:
        code: bond code
        engine {[type]} -- [description]

    Keyword Arguments:
        client {[type]} -- [description] (default: {DATABASE})
    """

    engine = select_save_engine(engine, backend=backend)
    engine.QA_SU_save_single_bond_min(code=code, client=client)
