# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2018-09-13 16:23:14

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

CONF = Config('../conf/secret.yaml').data[0]
ts_token = CONF['TUSHARE']['TOKEN']
ts.set_token(ts_token)
pro = ts.pro_api()

CONF = Config().data[0]
MONGODB = CONF['MONGODB']
NASDAQ = CONF['NASDAQ']
CRYPTOCURRENCY = CONF['CRYPTOCURRENCY']
CRYPTOCURRENCY = list(CRYPTOCURRENCY.keys())
NASDAQ100 = CONF['NASDAQ100']
# HS300 = list(ts.get_hs300s()['code'])
HS300_df = pd.read_csv('../database/HS300IDX_ALL.csv')
HS300 = list(set(HS300_df.con_code))
HS300 = [x.split('.')[0] for x in HS300]
ZZ500_df = pd.read_csv('../database/ZZ500IDX_ALL.csv')
ZZ500 = list(set(ZZ500_df.con_code))
ZZ500 = [x.split('.')[0] for x in ZZ500]

BENCHMARK = '399300'
# BENCHMARK = '163407'
# BENCHMARK = '000905'
# BENCHMARK = '399006'
# BENCHMARK = '512500'
# BENCHMARK = '161017'
# BENCHMARK = 'NDX'
# TARGET = HS300
TARGET = ['399300']
# TARGET = ['163407']
# TARGET = ['000905']
# TARGET = ['399006']
# TARGET = ['NDX']
# TARGET = ZZ500
# TARGET = ['512500']
# TARGET = ['161017']
ALL_TARGET = TARGET[:]

### 时间设置
start_date = '2005-01-01'
end_date = '2019-02-01'

POS = 1
### Turtle System
TURTLE_BUY_N = 0
TURTLE_SELL_N = 0
### MA System
MA_BUY_N = 0
MA_SELL_N = 0

### 业务设置
IS_HAPPYMONEY = False
IS_TAX = False
IS_SLIPPAGE = False
IS_RANDOM_BUY = True
IS_FILTER = False
IS_MARKETUP = False
IS_BUYBENCHMARK = True
IS_SHOWBUYLIST = True
START_MONEY = 100000
HAPPY_MONEY = 0
PROPERTY = START_MONEY
CASH = START_MONEY


# score_df = None
score_df = pd.DataFrame(columns=[
    'START',
    'END',
    'STRATEGY',
    'POS',
    'BUY_N',
    'SELL_N',
    'X_DAY_RETURN',
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
    'MISS_SIGNAL',
    'RET_PER_YEAR',
])


def get_stock_df_dict(N):
    BUY_N = N[0]
    SELL_N = N[1]
    stock_df_dict = {}
    for symbol in TARGET + [BENCHMARK]:
        stock_data_file = '../database/market/%s.csv' % symbol
        # info(symbol)
        if not os.path.isfile(stock_data_file):
            continue
        try:
            stock_df = pd.read_csv(stock_data_file)
        except Exception as e:
            continue

        # 筛选字段
        stock_df = stock_df.loc[:, ['date', 'open', 'close']]

        # 去掉Nasdaq行情首行的当天行情
        if symbol in NASDAQ100:
            stock_df = stock_df.drop([0])

        # 抛弃空值异常值
        stock_df.dropna(axis=0, how='any', inplace=True)

        # 格式化日期
        stock_df = stock_df.assign(date=pd.to_datetime(stock_df['date']))  # need .index.to_period('D')

        # 用日期作索引，日期升序排序
        if symbol in NASDAQ100 or symbol in CRYPTOCURRENCY:
            stock_df = stock_df[::-1]
        stock_df.set_index(['date'], inplace=True)
        stock_df.index = stock_df.index.to_period('D')

        # 计算每天涨跌幅
        stock_df['o_pct_chg'] = stock_df.open.pct_change(1)
        stock_df['c_o_pct_chg'] = (stock_df.open - stock_df.close.shift(1)) / stock_df.close.shift(1)

        # 策略指标
        stock_df['ROLLING_%d_MAX' % BUY_N] = stock_df['open'].rolling(BUY_N).max()
        stock_df['ROLLING_%d_MIN' % SELL_N] = stock_df['open'].rolling(SELL_N).min()
        stock_df['MA%d' % BUY_N] = stock_df['open'].rolling(BUY_N).mean()
        stock_df['MA%d' % SELL_N] = stock_df['open'].rolling(SELL_N).mean()

        # 减少数据
        stock_df.dropna(how='any', inplace=True)

        stock_df_dict[symbol] = stock_df
        # print(stock_df)
    return stock_df_dict


def run_turtle(symbol_list, stock_df_dict, S_TYPE, POS, N):
    TARGET = symbol_list
    POS = POS
    BUY_N = N[0]
    SELL_N = N[1]
    PROPERTY = START_MONEY
    CASH = START_MONEY
    count_day = 0
    yesterday = None
    miss_buy = 0

    '''用基准数据来存储策略数据'''
    show_df = None
    show_df = stock_df_dict[BENCHMARK].copy()
    show_df.loc[:, 'CASH'] = START_MONEY
    show_df.loc[:, 'PROPERTY'] = START_MONEY

    order_df = None
    order_df = pd.DataFrame(columns=[
        'buy_date', 'symbol', 'buy_count', 'buy_price', 'buy_reason', 'sell_date', 'sell_price', 'sell_reason', 'profit', 'cash', 'property'
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
        count_day += 1

        # 每年年初计算回报率
        # if today.dayofyear == 1 or today == (pd.to_datetime(end_date)).to_period(freq='D'):
        #     print(today, time.ctime())

        # 每年筛股
        if IS_FILTER and (today.dayofyear == 1 or count_day == 1):
            TARGET = ALL_TARGET[:]
            benchmark_lastyear = stock_df_dict[BENCHMARK][:today]

            # 不到一年（250个交易日）
            if len(benchmark_lastyear) < 250:
                continue

            # 筛选过滤
            benchmark_return_lastyear = benchmark_lastyear.iloc[-1].open / benchmark_lastyear.iloc[-250].open
            for symbol, stock_df in stock_df_dict.items():
                try:
                    return_lastyear = stock_df[:today].iloc[-1].open / stock_df[:today].iloc[-250].open
                    if return_lastyear < benchmark_return_lastyear:
                        TARGET.remove(symbol)
    #                     print(symbol, return_lastyear)
                except Exception as e:
                    TARGET.remove(symbol)
                    pass
            print(today, 'TARGET after filter', len(TARGET))

        if yesterday is None:
            yesterday = today
            continue

        # 日期不在基准交易日，则不交易
        if today not in stock_df_dict[BENCHMARK].index:
            continue

        # 突破下行趋势，清仓退出
        order_arr = order_df.to_records(index=False)
        if len(order_arr[(order_arr.buy_count > 0) & (order_arr.sell_price == 0)]) != 0:
            is_sell = False
            for idx in order_df[(order_df['buy_count'] > 0) & (order_df['sell_price'] == 0)].index:
                cur_order = order_df.loc[idx]
                symbol = cur_order['symbol']
                if today not in stock_df_dict[symbol].index or yesterday not in stock_df_dict[symbol].index:
                    continue
                today_market = stock_df_dict[symbol].loc[today]
                if today_market.c_o_pct_chg < -0.1:
                    if symbol not in NASDAQ100 or symbol not in CRYPTOCURRENCY:
                        # print(today, symbol, '跌停板，卖不掉')
                        continue
                if S_TYPE == 'MA':
                    is_sell = (today_market['MA%d' % BUY_N] < today_market['MA%d' % SELL_N])
                elif S_TYPE == 'TURTLE':
                    is_sell = (today_market.open <= today_market['ROLLING_%d_MIN' % SELL_N])
                if is_sell:
                    CASH += cur_order.buy_count * today_market.open
                    order_df.loc[idx, 'sell_date'] = today
                    order_df.loc[idx, 'sell_price'] = today_market.open
                    order_df.loc[idx, 'sell_reason'] = 'EXIT'
                    order_df.loc[idx, 'profit'] = \
                        (today_market.open - cur_order.buy_price) * cur_order.buy_count
    #                 print(today, '退出', stock_df_dict[symbol].loc[today, 'open'], CASH)

                    # ops_df = ops_df.append(
                    #     {
                    #         'ops_date': today,
                    #         'ops': 'SELL',
                    #         'symbol': symbol,
                    #         'count': cur_order.buy_count,
                    #         'price': today_market.open,
                    #         'reason': cur_order.buy_reason,
                    #         'profit': (today_market.open - cur_order.buy_price) * cur_order.buy_count,
                    #     },
                    #     ignore_index=True
                    # )

        # # 开心止盈，倍数止盈
        # if IS_HAPPYMONEY:
        #     if PROPERTY > START_MONEY * 2 and CASH > START_MONEY:
        #         HAPPY_MONEY += START_MONEY
        #         PROPERTY -= START_MONEY
        #         CASH -= START_MONEY

        benchmark_today_market = stock_df_dict[BENCHMARK].loc[today]
        # try:
        #     benchmark_yesterday_market = stock_df_dict[BENCHMARK].loc[:today].iloc[-2]
        # except:
        #     benchmark_yesterday_market = stock_df_dict[BENCHMARK].loc[:today].iloc[-1]
        buy_list = []

        # # 更新指数成分
        # if BENCHMARK == '399300':
        #     NEW_TARGET_df = HS300_df[HS300_df.trade_date == int(today.strftime('%Y%m%d'))]
        #     if len(NEW_TARGET_df) != 0:
        #         NEW_TARGET = [x.split('.')[0] for x in list(NEW_TARGET_df.con_code)]
        #         if sorted(NEW_TARGET) != sorted(TARGET):
        #             # print(today, 'CHANGE TARGET', len(NEW_TARGET_df))
        #             TARGET = NEW_TARGET
        #     else:
        #         pass

        # if BENCHMARK == '000905':
        #     NEW_TARGET_df = ZZ500_df[ZZ500_df.trade_date == int(today.strftime('%Y%m%d'))]
        #     if len(NEW_TARGET_df) != 0:
        #         NEW_TARGET = [x.split('.')[0] for x in list(NEW_TARGET_df.con_code)]
        #         if sorted(NEW_TARGET) != sorted(TARGET):
        #             # print(today, 'CHANGE TARGET', len(NEW_TARGET_df))
        #             TARGET = NEW_TARGET
        #     else:
        #         pass

        # 遍历标的，判断和执行买入
        # for symbol in symbol_list:
        for symbol in TARGET:
            if symbol not in stock_df_dict:
                continue

            # 趋势交易，只在好行情时买入
            if IS_MARKETUP:
                # if benchmark_today_market.MA60 < benchmark_today_market.MA180:
                if False:
                    break

            # # 是否购买基准
            # if not IS_BUYBENCHMARK and symbol == BENCHMARK:
            #     continue

            if today not in stock_df_dict[symbol].index or yesterday not in stock_df_dict[symbol].index:
                continue

            today_market = stock_df_dict[symbol].loc[today]

            # 突破上行趋势，就买一份
            # order_arr = order_df.to_records(index=False)
            is_buy = False
            # 指数就不要过滤器了
            if S_TYPE == 'MA':
                    is_buy = (today_market['MA%d' % BUY_N] > today_market['MA%d' % SELL_N])
            elif S_TYPE == 'TURTLE':
                    is_buy = (today_market.open >= today_market['ROLLING_%d_MAX' % BUY_N])
            buy_reason = S_TYPE

            if is_buy:
                buy_list.append(symbol)

        if IS_RANDOM_BUY:
            random.shuffle(buy_list)
        else:
            tmp_list = []
            for symbol in buy_list:
                try:
                    return_lastyear = stock_df_dict[symbol][:today].iloc[-1].open / stock_df_dict[symbol][:today].iloc[-250].open
                except Exception as e:
                    return_lastyear = stock_df_dict[symbol][:today].iloc[-1].open / stock_df_dict[symbol][:today].iloc[1].open
                tmp_list.append((return_lastyear, symbol))
            tmp_list = sorted(tmp_list, reverse=True)
            buy_list = [x[1] for x in tmp_list if x[0] > 1]
            # buy_list = [x[1] for x in tmp_list if x[0] > TURTLE_N[2]]
            # buy_list = [x[1] for x in tmp_list]
            random.shuffle(buy_list)

        for symbol in buy_list:
            today_market = stock_df_dict[symbol].loc[today]
            buy_count = 0

            # 滑点
            if IS_SLIPPAGE:
                buy_price = today_market.open * (1 + random.randint(0, 20) / 1000)
            else:
                buy_price = today_market.open

            # 按份数买
            if CASH >= PROPERTY / POS:
                buy_count = int((PROPERTY / POS) / buy_price)

            # 指数购买，满仓干
            # buy_count = int(CASH / buy_price)

            if buy_count > 0:
                if today_market.c_o_pct_chg > 0.1:
                    if symbol not in NASDAQ100 or symbol not in CRYPTOCURRENCY:
                        # print(today, symbol, '涨停板，买不进')
                        continue

            if buy_count > 0:
                CASH -= buy_count * buy_price
    #             print(today, '建仓', buy_count, stock_df_dict[symbol].loc[today, 'open'], CASH)
                order_df = order_df.append(
                    {
                        'buy_date': today,
                        'symbol': symbol,
                        'buy_count': buy_count,
                        'buy_price': today_market.open,
                        'buy_reason': buy_reason,
                        'sell_date': pd.np.nan,
                        'sell_price': 0,
                        'profit': 0,
                        'cash': CASH,
                        'property': PROPERTY,
                    },
                    ignore_index=True
                )
                # ops_df = ops_df.append(
                #     {
                #         'ops_date': today,
                #         'ops': 'BUY',
                #         'symbol': symbol,
                #         'count': buy_count,
                #         'price': buy_price,
                #         'reason': buy_reason,
                #         'profit': 0,
                #     },
                #     ignore_index=True
                # )
            else:
                miss_buy += 1

        # 每天盘点财产
        # 大盘下行时，闲钱进行T+0无风险投资，如货币基金
        # if benchmark_today_market.MA60 < benchmark_today_market.MA180:
        #     CASH = CASH * (1 + 0.03 / 365)
        show_df.loc[today, 'CASH'] = CASH
        PROPERTY = CASH + \
            sum(
                [
                    stock_df_dict[order_df.loc[idx, 'symbol']][:today].iloc[-1].open * order_df.loc[idx, 'buy_count'] \
                    for idx in order_df.loc[order_df['sell_price']==0].index
                ]
            )
        show_df.loc[today, 'PROPERTY'] = PROPERTY

        yesterday = today

    # 最后一天，清仓
    order_arr = order_df.to_records(index=False)
    for idx in order_df[order_df['sell_price'] == 0].index:
        cur_order = order_df.loc[idx]
        symbol = cur_order['symbol']
        today_market = stock_df_dict[symbol][:today].iloc[-1]
        CASH += cur_order.buy_count * today_market.open
        order_df.loc[idx, 'sell_date'] = today
        order_df.loc[idx, 'sell_price'] = today_market.open
        order_df.loc[idx, 'sell_reason'] = 'EXIT'
        order_df.loc[idx, 'profit'] = \
            (today_market.open - cur_order.buy_price) * cur_order.buy_count
        # ops_df = ops_df.append(
        #     {
        #         'ops_date': today,
        #         'ops': 'SELL',
        #         'symbol': symbol,
        #         'count': cur_order.buy_count,
        #         'price': today_market.open,
        #         'reason': cur_order.buy_reason,
        #         'profit': (today_market.open - cur_order.buy_price) * cur_order.buy_count,
        #     },
        #     ignore_index=True
        # )

    return show_df, order_df, PROPERTY, miss_buy


def work(S_TYPE, POS, N):
    info('work %s %s %s' % (S_TYPE, POS, N))
    stock_df_dict = None
    show_df = None
    order_df = None
    PROPERTY = None
    symbol_list = TARGET + [BENCHMARK]

    stock_df_dict = get_stock_df_dict(N)
    show_df, order_df, PROPERTY, miss_buy = run_turtle(symbol_list, stock_df_dict, S_TYPE, POS, N)

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
    info(output_str)

    df = order_df.copy()
    df['pro_pct'] = (df.sell_price - df.buy_price) / df.buy_price
    df = df.loc[:, ['symbol', 'pro_pct']]
    df = df.groupby(by='symbol').sum()
    buy_stock_count = len(df)

    score_sr = pd.Series({
        'START': start_date,
        'END': end_date,
        'STRATEGY': S_TYPE,
        'POS': POS,
        'BUY_N': N[0],
        'SELL_N': N[1],
        'X_DAY_RETURN': 250,
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
        'MISS_SIGNAL': miss_buy,
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

    return POS, N, score_sr


def when_done(r):
    # info('when_done')
    # info(r.result())
    res = r.result()
    # info(res[2])
    info('done %s %s' % (res[2]['BUY_N'], res[2]['SELL_N']))
    global score_df
    score_df = score_df.append(res[2], ignore_index=True)
    return r.result()


def work2(pos, n):
    return pos, n


def main():
    s_type = ['TURTLE']
    # s_type = ['MA']

    # pos_list = [x * 5 for x in range(4, 6)]
    # pos_list = [10, 20, 30, 40, 50]
    # pos_list = [50]
    pos_list = [1]

    n_list = [(30, 60), (30, 90), (30, 180), (60, 90), (60, 180), (90, 180)]
    n_list = [(90, 180, 1)] * 10
    # n_list = [(90, 180, 0)] * 10
    # n_list = [(90, 180, round(0.1 * x, 1)) for x in range(1, 21)]
    n_list = list(itertools.product([x * 10 for x in range(1, 21)], [x * 10 for x in range(1, 21)]))
    # n_list = list(itertools.product([x * 10 for x in range(1, 3)], [x * 10 for x in range(1, 3)]))
    # n_list = [t for t in n_list if t[0] < t[1]]

    print(s_type)
    print(pos_list)
    print(n_list)
    params = itertools.product(s_type, pos_list, n_list)

    with ProcessPoolExecutor(1) as pool:
        for t, pos, n in params:
            info('submit %s %s %s' % (t, pos, n))
            future_result = pool.submit(work, t, pos, n)
            future_result.add_done_callback(when_done)

    print(score_df.loc[:, ['POS', 'BUY_N', 'SELL_N', 'RETURN_ALGO', 'RETURN_BENC', 'MAXDROPDOWN_ALGO', 'MAXDROPDOWN_BENC', 'WINRATE_ORDER', 'WINRATE_YEARLY']])
    print(score_df.describe())
    csv_file = '../database/%s.csv' % time.strftime('%Y%m%d-%H%M%S')
    score_df.to_csv(csv_file, index=False)


if __name__ == '__main__':
    set_log(INFO)
    main()
    # POS, N, score_sr = work('TURTLE', 1, (20, 190))
    # print(score_sr)
