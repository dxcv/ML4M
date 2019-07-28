# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2019-05-22 11:53:53

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))

import pandas as pd
pd.core.common.is_list_like = pd.api.types.is_list_like
try:
    import empyrical as emp
except:
    emp = None
import tushare as ts
import time
import random
import itertools
from concurrent.futures import ProcessPoolExecutor

from common.log import *
from common.config import Config
from spider.spider_nasdaq import Spider_nasdaq
from spider.spider_coinmarketcap import Spider_coinmarketcap
from spider.spider_okex import Spider_okex

CONF = Config('../conf/secret.yaml').data[0]
ts_token = CONF['TUSHARE']['TOKEN']
ts.set_token(ts_token)
pro = ts.pro_api()

CONF = Config().data[0]
MONGODB = CONF['MONGODB']
NASDAQ = CONF['NASDAQ']
NASDAQ100 = CONF['NASDAQ100']
CRYPTOCURRENCY = CONF['CRYPTOCURRENCY']
CRYPTOCURRENCY = list(CRYPTOCURRENCY.keys())
CRYPTOCURRENCYSYMBOL = CONF['CRYPTOCURRENCYSYMBOL']

BENCHMARK = '399300'
ROTATION_LIST = ['399300', '000016', '000905', '399006', '000012']
ROTATION_LIST = ['399300', '000905', '399006', '000012']
SAFE = '000012'

BENCHMARK = 'BITCOIN'
ROTATION_LIST = ['BITCOIN', 'EOS', 'TETHER', 'ETHEREUM', 'RIPPLE', 'LITECOIN']
ROTATION_LIST = ['BITCOIN', 'EOS', 'ETHEREUM', 'RIPPLE', 'LITECOIN']
SAFE = ''

# BENCHMARK = 'BTC'
# ROTATION_LIST = ['BTC', 'EOS', 'ETH', 'XRP', 'LTC']
# SAFE = ''

### 时间设置
start_date = '2017-01-01'
# end_date = '2019-06-01'
end_date = time.strftime('%Y-%m-%d')

### ETF Rotation System
POS = 1
# N = 13
# M = 100
# K = 0

### 业务设置
START_MONEY = 1000000
PROPERTY = START_MONEY
CASH = START_MONEY


# score_df = None
score_df = pd.DataFrame(columns=[
    'START',
    'END',
    'STRATEGY',
    'POS',
    'N',
    'K',
    'M',
    'ORDER',
    'STOCK',
    'RETURN_ALGO',
    'RETURN_BENC',
    'MAXDROPDOWN_ALGO',
    'MAXDROPDOWN_BENC',
    'WINRATE_ORDER',
    'WINRATE_YEARLY',
    'ANNUAL_RETURN',
    'ANNUAL_VOLATILITY',
    'CALMAR_RATIO',
    'SHARPE_RATIO',
    'ALPHA',
    'BETA',
    'DAYS_ALL',
    'DAYS_NOFULLHOLD',
    'RET_PER_YEAR',
])


def get_stock_df_dict(N, M):
    stock_df_dict = {}

    for symbol in ROTATION_LIST:
        stock_data_file = '../database/market/%s.csv' % symbol
        # if symbol in CRYPTOCURRENCYSYMBOL:
        #     stock_data_file = '../database/market/%s_OKEX.csv' % symbol
        #     stock_data_file = '../database/market/%s_OKEX_HOURS.csv' % symbol
        try:
            stock_df = pd.read_csv(stock_data_file)
        except Exception as e:
            print(symbol, e)
            continue

        # 筛选字段
        stock_df = stock_df.loc[:, ['date', 'open', 'close']]

        # 特殊处理，用收盘价判断和交易
        # stock_df['open'] = stock_df['close']

        # 去掉Nasdaq行情首行的当天行情
        if symbol in NASDAQ100:
            stock_df = stock_df.drop([0])

        # 抛弃空值异常值
        stock_df.dropna(axis=0, how='any', inplace=True)

        # 格式化日期
        stock_df = stock_df.assign(date=pd.to_datetime(stock_df['date']))

        # 用日期作索引，日期升序排序
        if symbol in NASDAQ100 or symbol in CRYPTOCURRENCYSYMBOL or symbol in CRYPTOCURRENCY:
            stock_df = stock_df[::-1]
        stock_df.set_index(['date'], inplace=True)
        stock_df.index = stock_df.index.to_period('D')
        # stock_df.index = stock_df.index.to_period('H')

        # 计算每天涨跌幅
        # stock_df['o_pct_chg'] = stock_df.open.pct_change(1)
        # stock_df['c_o_pct_chg'] = (stock_df.open - stock_df.close.shift(1)) / stock_df.close.shift(1)
        # stock_df['N_chg'] = stock_df.open.pct_change(N)
        # 特殊处理，用昨天收盘价做判定
        stock_df['N_chg'] = (stock_df.close.shift(1) - stock_df.close.shift(N)) / stock_df.close.shift(N)
        stock_df['y_close'] = stock_df.close.shift(1)
        stock_df['MA%d' % M] = stock_df['close'].rolling(M).mean().shift(1)

        # 减少数据
        stock_df.dropna(how='any', inplace=True)

        stock_df_dict[symbol] = stock_df
    return stock_df_dict


def run_turtle(ROTATION_LIST, stock_df_dict, STRATEGY, POS, N, K, M):
    PROPERTY = START_MONEY
    CASH = START_MONEY
    count_day = 0
    yesterday = None

    '''用基准数据来存储策略数据'''
    show_df = None
    show_df = stock_df_dict[BENCHMARK].copy()
    show_df.loc[:, 'CASH'] = START_MONEY
    show_df.loc[:, 'PROPERTY'] = START_MONEY

    order_df = None
    order_df = pd.DataFrame(columns=[
        'borrow_date', 'symbol', 'borrow_count', 'borrow_price', 'borrow_reason',
        'return_date', 'return_price', 'return_reason',
        'profit', 'cash', 'property'
    ])

    ops_df = None
    ops_df = pd.DataFrame(columns=[
        'ops_date', 'ops', 'symbol', 'count', 'price', 'reason', 'profit'
    ])

    df_start_day = stock_df_dict[BENCHMARK].head(1).index[0].strftime('%Y-%m-%d')
    if df_start_day > start_date:
        run_start_day = df_start_day
    else:
        run_start_day = start_date

    # 时间序列
    for today in pd.period_range(start=run_start_day, end=end_date, freq='D'):
    # for today in pd.period_range(start=run_start_day, end=end_date, freq='H'):
        count_day += 1
        is_change = True
        buy_reason = ''
        sell_reason = ''

        if yesterday is None:
            yesterday = today
            continue

        # 日期不在基准交易日，则不交易
        if today not in stock_df_dict[BENCHMARK].index:
            continue

        # benchmark_today_market = stock_df_dict[BENCHMARK].loc[today]

        # 计算标的今天的N天涨跌幅，找到买入目标
        N_chg_list = []
        for symbol in ROTATION_LIST:
            # 部分标的早期不存在
            if today not in stock_df_dict[symbol].index:
                N_chg_list.append(999)
            elif symbol == SAFE:
                N_chg_list.append(999)
            else:
                today_market = stock_df_dict[symbol].loc[today]
                N_chg_list.append(today_market.N_chg)
        min_N_chg = min(N_chg_list)
        target_symbol = ROTATION_LIST[N_chg_list.index(min_N_chg)]
        today_market = stock_df_dict[target_symbol].loc[today]
        if today_market.y_close > today_market['MA%d' % M]:
            target_symbol = SAFE

        if min_N_chg > 0:
            target_symbol = SAFE

        if target_symbol == SAFE:
            buy_reason = 'SAFE'
            sell_reason = 'SAFE'
        else:
            buy_reason = 'HIGH'
            sell_reason = 'LOW'

        # 判断当前持有标的，和买入目标，是否相同，相同则今天不交易
        cur_order = None
        if len(order_df[(order_df['borrow_count'] > 0) & (order_df['return_price'] == 0)]) != 0:
            cur_order = order_df[(order_df['borrow_count'] > 0) & (order_df['return_price'] == 0)].iloc[-1]
            holding_symbol = cur_order.symbol
        else:
            holding_symbol = ''
            is_change = True
        if target_symbol == holding_symbol:
            is_change = False
        if cur_order is not None and today - cur_order.borrow_date < K:
            is_change = False
        if holding_symbol != '' and today not in stock_df_dict[holding_symbol].index:
            is_change = False
        if target_symbol != '' and today not in stock_df_dict[target_symbol].index:
            is_change = False

        # 当前持有标的和买入目标不相同，或者今天是首日空仓，则今天交易，换仓/全仓
        if is_change and holding_symbol != '':
            today_market = stock_df_dict[holding_symbol].loc[today]
            # 停牌了/数据出错了，今天卖不了了，完了
            if today not in stock_df_dict[holding_symbol].index:
                continue
            CASH -= cur_order.borrow_count * today_market.open
            idx = cur_order.name
            order_df.loc[idx, 'return_date'] = today
            order_df.loc[idx, 'return_price'] = today_market.open
            order_df.loc[idx, 'return_reason'] = 'EXIT'
            order_df.loc[idx, 'profit'] = (cur_order.borrow_price - today_market.open) * cur_order.borrow_count

            ops_df = ops_df.append(
                {
                    'ops_date': today,
                    'ops': 'RETURN',
                    'symbol': holding_symbol,
                    'count': cur_order.borrow_count,
                    'price': today_market.open,
                    'reason': cur_order.borrow_reason,
                    'profit': (cur_order.borrow_price - today_market.open) * cur_order.borrow_count,
                },
                ignore_index=True
            )

        if is_change and target_symbol != '':
            # 开始执行买入
            today_market = stock_df_dict[target_symbol].loc[today]

            # 停牌了/数据出错了，今天卖不了了，完了
            if today not in stock_df_dict[target_symbol].index:
                continue

            borrow_price = today_market.open
            borrow_count = int(CASH / borrow_price)
            borrow_reason = 'CHANGE'

            if borrow_count > 0:
                CASH += borrow_count * borrow_price
                order_df = order_df.append(
                    {
                        'borrow_date': today,
                        'symbol': target_symbol,
                        'borrow_count': borrow_count,
                        'borrow_price': today_market.open,
                        'borrow_reason': borrow_reason,
                        'return_date': pd.np.nan,
                        'return_price': 0,
                        'profit': 0,
                        'cash': CASH,
                        'property': PROPERTY,
                    },
                    ignore_index=True
                )
                ops_df = ops_df.append(
                    {
                        'ops_date': today,
                        'ops': 'BORROW',
                        'symbol': target_symbol,
                        'count': borrow_count,
                        'price': borrow_price,
                        'reason': borrow_reason,
                        'profit': 0,
                    },
                    ignore_index=True
                )

        # 每天盘点财产
        show_df.loc[today, 'CASH'] = CASH
        PROPERTY = CASH - \
            sum(
                [
                    stock_df_dict[order_df.loc[idx, 'symbol']][:today].iloc[-1].open * order_df.loc[idx, 'borrow_count'] \
                    for idx in order_df.loc[order_df['return_price']==0].index
                ]
            )
        show_df.loc[today, 'PROPERTY'] = PROPERTY


    # 最后一天，清仓
    # order_arr = order_df.to_records(index=False)
    for idx in order_df[order_df['return_price'] == 0].index:
        cur_order = order_df.loc[idx]
        symbol = cur_order['symbol']
        today_market = stock_df_dict[symbol][:today].iloc[-1]
        CASH -= cur_order.borrow_count * today_market.open
        order_df.loc[idx, 'return_date'] = today
        order_df.loc[idx, 'return_price'] = today_market.open
        order_df.loc[idx, 'return_reason'] = 'EXIT'
        order_df.loc[idx, 'profit'] = \
            (cur_order.borrow_price - today_market.open) * cur_order.borrow_count
        ops_df = ops_df.append(
            {
                'ops_date': today,
                'ops': 'SELL',
                'symbol': symbol,
                'count': cur_order.borrow_count,
                'price': today_market.open,
                'reason': cur_order.borrow_reason,
                'profit': (cur_order.borrow_price - today_market.open) * cur_order.borrow_count,
            },
            ignore_index=True
        )

    return show_df, order_df, PROPERTY


def work(PARAMS):
    info('work %s' % str(PARAMS))
    stock_df_dict = None
    show_df = None
    order_df = None
    PROPERTY = None
    STRATEGY = PARAMS[0]
    POS = PARAMS[1]
    N = PARAMS[2]
    K = PARAMS[3]
    M = PARAMS[4]
    global ROTATION_LIST
    ROTATION_LIST = ROTATION_LIST

    stock_df_dict = get_stock_df_dict(N, M)
    show_df, order_df, PROPERTY = run_turtle(ROTATION_LIST, stock_df_dict, STRATEGY, POS, N, K, M)

    df = show_df.dropna(how='any', inplace=False).copy()
    df = df.loc[start_date:end_date]
    algo = df['PROPERTY'].pct_change()
    benchmark = df.open.pct_change()

    DAYS_ALL = len(df)
    DAYS_NOFULLHOLD = len(df[df['CASH'] > (df['PROPERTY'] / POS)])

    output_str = ''
    for y in range(int(start_date.split('-')[0]), int(end_date.split('-')[0]) + 1, 1):
        # info('y = %d' % y)
        y_df = df.loc['%d-01-01' % y:'%d-01-01' % (y + 1)]
        if len(y_df) == 0:
            continue
        y_algo = y_df['PROPERTY'].pct_change()
        # info(y_algo)
        y_benchmark = y_df.open.pct_change()
        # info('y_benc')
        result = '%d-%d,%.3f,%.3f,%.3f,%.3f' % (
            y, y + 1, emp.cum_returns(y_algo)[-1], emp.cum_returns(y_benchmark)[-1], emp.max_drawdown(y_algo), emp.max_drawdown(y_benchmark)
        )
        output_str += result
        output_str += ';'
    # info(output_str)

    df = order_df.copy()
    df['pro_pct'] = (df.borrow_price - df.return_price) / df.return_price
    df = df.loc[:, ['symbol', 'pro_pct']]
    df = df.groupby(by='symbol').sum()
    buy_stock_count = len(df)

    score_sr = pd.Series({
        'START': start_date,
        'END': end_date,
        'STRATEGY': STRATEGY,
        'POS': POS,
        'N': N,
        'K': K,
        'M': M,
        'ORDER': len(order_df),
        'STOCK': buy_stock_count,
        'RETURN_ALGO': emp.cum_returns(algo)[-1],
        'RETURN_BENC': emp.cum_returns(benchmark)[-1],
        'MAXDROPDOWN_ALGO': emp.max_drawdown(algo),
        'MAXDROPDOWN_BENC': emp.max_drawdown(benchmark),
        'WINRATE_ORDER': len(order_df[order_df.profit > 0]) / len(order_df[order_df.profit != 0]),
        'WINRATE_YEARLY': 0,
        'ANNUAL_RETURN': emp.annual_return(algo),
        'ANNUAL_VOLATILITY': emp.annual_volatility(algo, period='daily'),
        'CALMAR_RATIO': emp.calmar_ratio(algo),
        'SHARPE_RATIO': emp.sharpe_ratio(returns=algo),
        'ALPHA': emp.alpha(returns=algo, factor_returns=benchmark, risk_free=0.00),
        'BETA': emp.beta(returns=algo, factor_returns=benchmark, risk_free=0.00),
        'DAYS_ALL': DAYS_ALL,
        'DAYS_NOFULLHOLD': DAYS_NOFULLHOLD,
        'RET_PER_YEAR': output_str,
    })

    YEAR_COUNT = 0
    ALGO_WIN_YEAR_COUNT = 0
    df = show_df.dropna(how='any', inplace=False).copy()
    df = df.loc[start_date:end_date]
    for y in range(int(start_date.split('-')[0]), int(end_date.split('-')[0]) + 1, 1):
        y_df = df.loc['%d-01-01' % y:'%d-01-01' % (y + 1)]
        # info('y = %d' % y)
        if len(y_df) == 0:
            continue
        y_algo = y_df['PROPERTY'].pct_change()
        y_benchmark = y_df.open.pct_change()
        score_sr['RETURN_ALGO_%d' % y] = emp.cum_returns(y_algo)[-1]
        score_sr['RETURN_BENC_%d' % y] = emp.cum_returns(y_benchmark)[-1]
        YEAR_COUNT += 1
        if score_sr['RETURN_ALGO_%d' % y] > score_sr['RETURN_BENC_%d' % y]:
            ALGO_WIN_YEAR_COUNT += 1

    score_sr['WINRATE_YEARLY'] = ALGO_WIN_YEAR_COUNT / YEAR_COUNT

    return PARAMS, score_sr, order_df


def when_done(r):
    # info('when_done')
    # info(r.result())
    res = r.result()
    # info(res[2])
    info('done N=%s K=%s, M=%s' % (res[1]['N'], res[1]['K'], res[1]['M']))
    global score_df
    score_df = score_df.append(res[1], ignore_index=True)
    return r.result()


def work2(pos, n):
    return pos, n


def main():
    s_type = ['ETF_ROTATION']
    pos_list = [1]
    n_list = list(range(1, 31))
    k_list = [0, 1, 2, 3, 4, 5, 6, 7]
    k_list = [0]
    m_list = list(range(1, 31))

    print(s_type)
    print(pos_list)
    print(n_list)
    print(k_list)
    print(m_list)
    params_list = itertools.product(s_type, pos_list, n_list, k_list, m_list)

    with ProcessPoolExecutor(2) as pool:
        for params in params_list:
            info('submit %s' % str(params))
            future_result = pool.submit(work, params)
            future_result.add_done_callback(when_done)

    print(score_df.loc[:, ['POS', 'N', 'K', 'M', 'RETURN_ALGO', 'RETURN_BENC', 'MAXDROPDOWN_ALGO', 'MAXDROPDOWN_BENC', 'WINRATE_ORDER', 'WINRATE_YEARLY']])
    print(score_df.describe())
    csv_file = '../database/%s.csv' % time.strftime('%Y%m%d-%H%M%S')
    score_df.to_csv(csv_file, index=False)


if __name__ == '__main__':
    set_log(INFO)
    main()
    # PARAMS, score_sr, order_df = work(('ETF_ROTATION_BEAR', 1, 10, 0, 30))
    # print(PARAMS)
    # print(score_sr)
    # print(order_df)
