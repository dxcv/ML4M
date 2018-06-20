# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2018-06-10 13:40:18

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))

import pandas as pd
import empyrical as emp
import time
from concurrent.futures import ProcessPoolExecutor

from common.log import *
from common.config import Config
from spider.spider_nasdaq import Spider_nasdaq

CONF = Config().data[0]
MONGODB = CONF['MONGODB']
NASDAQ = CONF['NASDAQ']
CRYPTOCURRENCY = CONF['CRYPTOCURRENCY']
NASDAQ100 = CONF['NASDAQ100']


# 时间设置
start_date = '2008-06-02'
# start_date = '2017-01-03'
# start_date = '2018-01-01'
end_date = '2018-06-06'


# 海龟参数
TURTLE_POS = 20
TURTLE_POS = 1
ATR_N = 20
# Turtle System One - Short
TURTLE_SHORT_BUY_N = 20
TURTLE_SHORT_SELL_N = 20
# Turtle System Two - Long
TURTLE_LONG_BUY_N = 60
TURTLE_LONG_SELL_N = 20


# 业务设置
IS_HAPPY_MONEY = False
START_MONEY = 100000
HAPPY_MONEY = 0
PROPERTY = START_MONEY
CASH = START_MONEY
IS_TAX = False


# 数据准备
stock_df_dict = {}


def MDD(ret):
    dd_list = []
    for i in ret.index:
        Xc = ret[i]
        Xt = max(ret[:i])
        dd_list.append(((Xt - Xc) / Xt, i, Xt, Xc))
    return max(dd_list)


def callback_prepar_df(r):
    ret = r.result()
    stock_df_dict[ret['symbol']] = ret['stock_df']


def prepar_df(symbol):
    stock_data_file = '../database/market/%s.csv' % symbol
    stock_df = pd.read_csv(stock_data_file)

    # 去掉Nasdaq行情首行的当天行情
    if symbol in NASDAQ:
        stock_df = stock_df.drop([0])

    # 抛弃空值异常值
    stock_df.dropna(axis=0, how='any', inplace=True)

    # 格式化日期
    stock_df = stock_df.assign(date=pd.to_datetime(
        stock_df['date']))  # need .index.to_period('D')

    # 用日期作索引，日期升序排序
    # 95.1 µs ± 1.58 µs per loop (mean ± std. dev. of 7 runs, 10000 loops each)
    stock_df = stock_df[::-1]

    stock_df.set_index(['date'], inplace=True)

    # 822 µs ± 41.3 µs per loop (mean ± std. dev. of 7 runs, 1000 loops each)
    stock_df.index = stock_df.index.to_period('D')

    # 计算涨跌幅
    # stock_df['c_pct_chg'] = stock_df.close.pct_change(1)
    stock_df['o_pct_chg'] = stock_df.open.pct_change(1)

    # Turtle指标
    # stock_df['TR1'] = abs(stock_df['high'] - stock_df['low'])
    # stock_df['TR2'] = abs(stock_df['high'] - stock_df['close'].shift())
    # stock_df['TR3'] = abs(stock_df['low'] - stock_df['close'].shift())
    # stock_df['TR'] = stock_df[['TR1', 'TR2', 'TR3']].max(axis=1)
    # stock_df['ATR'] = stock_df['TR'].rolling(ATR_N).mean()
    # stock_df['Unit'] = (0.01 * START_MONEY) / (stock_df['ATR'])
    stock_df['ROLLING_%d_MAX' % TURTLE_SHORT_BUY_N] = stock_df['open'].rolling(
        TURTLE_SHORT_BUY_N).max()
    stock_df['ROLLING_%d_MIN' % TURTLE_SHORT_SELL_N] = stock_df['open'].rolling(
        TURTLE_SHORT_SELL_N).min()
    stock_df['ROLLING_%d_MAX' % TURTLE_LONG_BUY_N] = stock_df['open'].rolling(
        TURTLE_LONG_BUY_N).max()
    stock_df['ROLLING_%d_MIN' % TURTLE_LONG_SELL_N] = stock_df['open'].rolling(
        TURTLE_LONG_SELL_N).min()
    stock_df['EWMA'] = stock_df['open'].ewm(alpha=0.1, adjust=False).mean()
    stock_df['MA360'] = stock_df['open'].rolling(360).mean()
    stock_df['MA180'] = stock_df['open'].rolling(180).mean()
    stock_df['MA30'] = stock_df['open'].rolling(30).mean()

    # 减少数据
    stock_df.dropna(how='any', inplace=True)
    # stock_df.drop(columns=['volume', 'TR1', 'TR2', 'TR3'], inplace=True)
    stock_df.drop(columns=['volume'], inplace=True)

    return {'symbol': symbol, 'stock_df': stock_df}


def prepar_data():
    with ProcessPoolExecutor(2) as pool:
        for symbol in NASDAQ100[:]:
            future_result = pool.submit(prepar_df, symbol)
            # 当进程完成任务即calc运行结束后的回调函数
            future_result.add_done_callback(callback_prepar_df)


def run_turtle():
    PROPERTY = START_MONEY
    CASH = START_MONEY

    show_df = None
    show_df = stock_df_dict['NDX'].copy()

    order_df = None
    order_df = pd.DataFrame(columns=[
        'buy_date', 'symbol', 'buy_count', 'buy_price', 'buy_reason', 'sell_date', 'sell_price', 'sell_reason', 'profit', 'cash', 'property'
    ])
    count_day = 0
    yesterday = None

    for today in pd.period_range(start=start_date, end=end_date, freq='D'):
        count_day += 1

        if yesterday is None:
            yesterday = today
            continue

        if today not in stock_df_dict['NDX'].index:
            continue

        if IS_HAPPY_MONEY:
            if PROPERTY > START_MONEY * 2:
                global HAPPY_MONEY
                HAPPY_MONEY += int(START_MONEY / 2)
                PROPERTY -= int(START_MONEY / 2)
                CASH = PROPERTY

        # 买卖过程
        # for symbol in NASDAQ100[:]:
        for symbol in ['TSLA']:
            if symbol in ['ALGN', 'ROST', 'ORLY', 'ESRX', 'ULTA', 'REGN', 'MNST']:
                # continue
                pass

            if symbol == 'NDX':
                continue

            if today not in stock_df_dict[symbol].index or yesterday not in stock_df_dict[symbol].index:
                continue

            # 突破下行趋势，清仓退出
            order_arr = order_df.to_records(index=False)
            if len(order_arr[(order_arr.symbol == symbol) & (order_arr.sell_price == 0)]) != 0:
                is_sell = False
                for idx in order_df[(order_df['symbol'] == symbol) & (order_df['sell_price'] == 0)].index:
                    if order_df.loc[idx, 'buy_reason'] == 'SHORT':
                        is_sell = (stock_df_dict[symbol].loc[today, 'open'] <=
                                   stock_df_dict[symbol].loc[today, 'ROLLING_%d_MIN' % TURTLE_SHORT_SELL_N])
                    if order_df.loc[idx, 'buy_reason'] == 'LONG':
                        is_sell = (stock_df_dict[symbol].loc[today, 'open'] <=
                                   stock_df_dict[symbol].loc[today, 'ROLLING_%d_MIN' % TURTLE_LONG_SELL_N])
                    if is_sell:
                        CASH += order_df.loc[idx, 'buy_count'] * \
                            stock_df_dict[symbol].loc[today, 'open']
                        order_df.loc[idx, 'sell_date'] = today
                        order_df.loc[idx,
                                     'sell_price'] = stock_df_dict[symbol].loc[today, 'open']
                        order_df.loc[idx, 'sell_reason'] = 'EXIT'
                        order_df.loc[idx, 'profit'] = \
                            (order_df.loc[idx, 'sell_price'] - order_df.loc[idx, 'buy_price']) * order_df.loc[idx, 'buy_count']
                    # print(today, '退出', stock_df_dict[symbol].loc[today, 'open'], CASH)

            # 突破上行趋势，买入一份
            order_arr = order_df.to_records(index=False)
            if stock_df_dict[symbol].loc[today, 'MA30'] >= stock_df_dict[symbol].loc[today, 'MA180']:
                is_buy = False
                if stock_df_dict[symbol].loc[today, 'open'] >= stock_df_dict[symbol].loc[today, 'ROLLING_%d_MAX' % TURTLE_LONG_BUY_N]:
                    is_buy = True
                    buy_reason = 'LONG'
                elif False and stock_df_dict[symbol].loc[today, 'open'] >= stock_df_dict[symbol].loc[today, 'ROLLING_%d_MAX' % TURTLE_SHORT_BUY_N]:
                    is_buy = True
                    buy_reason = 'SHORT'
                if is_buy:
                    buy_count = 0
                    if CASH >= PROPERTY / TURTLE_POS:
                        buy_count = int(
                            (PROPERTY / TURTLE_POS) / stock_df_dict[symbol].loc[today, 'open'])
                    if buy_count > 0:
                        CASH -= buy_count * \
                            stock_df_dict[symbol].loc[today, 'open']
                        # print(today, '买入', buy_count, stock_df_dict[symbol].loc[today, 'open'], CASH)
                        order_df = order_df.append(
                            {
                                'buy_date': today,
                                'symbol': symbol,
                                'buy_count': buy_count,
                                'buy_price': stock_df_dict[symbol].loc[today, 'open'],
                                'buy_reason': buy_reason,
                                'sell_date': pd.np.nan,
                                'sell_price': 0,
                                'profit': 0,
                                'cash': CASH,
                                'property': PROPERTY,
                            },
                            ignore_index=True
                        )

        # 每天盘点财产
        show_df.loc[today, 'CASH_TURTLE_%d_%d_%d' %
                    (TURTLE_POS, TURTLE_LONG_BUY_N, TURTLE_LONG_SELL_N)] = CASH
        PROPERTY = CASH + \
            sum(
                [
                    stock_df_dict[order_df.loc[idx, 'symbol']].loc[today,
                                                                   'open'] * order_df.loc[idx, 'buy_count']
                    for idx in order_df.loc[order_df['sell_price'] == 0].index
                ]
            )
        show_df.loc[today, 'PROPERTY_TURTLE_%d_%d_%d' %
                    (TURTLE_POS, TURTLE_LONG_BUY_N, TURTLE_LONG_SELL_N)] = PROPERTY
        yesterday = today

    # 最终结果
    print('CASH', CASH)
    print('HAPPY_MONEY', HAPPY_MONEY)
    print('PROPERTY', PROPERTY)

    benchmark_symbol = 'TSLA'
    s_p = stock_df_dict[benchmark_symbol][start_date:].iloc[0].open
    e_p = stock_df_dict[benchmark_symbol].iloc[-1].open
    print(benchmark_symbol, s_p, e_p, e_p / s_p)

    show_df = show_df[start_date:].dropna(how='any', inplace=False)
    show_df['strategy_pct'] = show_df['PROPERTY_TURTLE_%d_%d_%d' % (TURTLE_POS, TURTLE_LONG_BUY_N, TURTLE_LONG_SELL_N)].pct_change()
    # show_df['benchmark_pct'] = show_df['open'].pct_change()
    show_df['benchmark_pct'] = stock_df_dict[benchmark_symbol].open.pct_change()
    # print('cum_returns', emp.cum_returns(show_df.strategy_pct))
    print('max_drawdown', emp.max_drawdown(show_df.strategy_pct))
    print('MDD', MDD(show_df['PROPERTY_TURTLE_%d_%d_%d' % (TURTLE_POS, TURTLE_LONG_BUY_N, TURTLE_LONG_SELL_N)]))
    print('annual_return', emp.annual_return(show_df.strategy_pct))
    print('annual_volatility', emp.annual_volatility(show_df.strategy_pct, period='daily'))
    print('calmar_ratio', emp.calmar_ratio(show_df.strategy_pct))
    print('sharpe_ratio', emp.sharpe_ratio(returns=show_df.strategy_pct))
    print('alpha', emp.alpha(returns=show_df.strategy_pct, factor_returns=show_df.benchmark_pct, risk_free=0.00))
    print('beta', emp.beta(returns=show_df.strategy_pct, factor_returns=show_df.benchmark_pct, risk_free=0.00))
    # print(show_df['PROPERTY_TURTLE_%d_%d_%d' % (TURTLE_POS, TURTLE_LONG_BUY_N, TURTLE_LONG_SELL_N)])



if __name__ == '__main__':
    print(time.ctime())
    prepar_data()
    # print(stock_df_dict['TSLA'].tail(10))
    print(time.ctime())
    run_turtle()
    print(time.ctime())
    # print(stock_df_dict['NDX'].open)
    # print(MDD(stock_df_dict['NDX'].open))
