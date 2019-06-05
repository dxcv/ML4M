# -*- coding: utf-8 -*-
# @Author: Kai Zhang
# @Date:   2018-05-11 16:56:29


class Strategy(object):
    """策略基类"""

    def __init__(self, symbol):
        super(Strategy, self).__init__()
        self.symbol = symbol

    def buy_signal(self, date):
        """
        买入信号
        """
        if True:
            return True
        return False

    def sell_signal(self, date):
        """
        卖出信号
        """
        if True:
            return True
        return False


if __name__ == '__main__':
    symbol = 'TSLA'
    strategy = Strategy(symbol)
