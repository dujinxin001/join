# !/usr/bin/env python
# -*- coding: utf-8 -*-
from gmsdk.api import StrategyBase
from gmsdk import to_dict


class MyStrategy(StrategyBase):
    def __init__(self, *args, **kwargs):
        super(MyStrategy, self).__init__(*args, **kwargs)
        self.oc = True

    def on_bar(self, bar):
        if self.oc:
            self.open_long(bar.exchange, bar.sec_id, 0, 10000)
        else:
            self.close_long(bar.exchange, bar.sec_id, 0, 10000)
        self.oc = not self.oc

    def on_backtest_finish(self, indicator):
        print('backtest finished', to_dict(indicator))

if __name__ == '__main__':
    mystrategy = MyStrategy(
        username='dujinxin1330@163.com',
        password='11250822--juejin',
        strategy_id='a8985ef4-c5df-11e6-a339-185e0f571106',
        subscribe_symbols='SHSE.600000.bar.daily',
        mode=4,
        td_addr='localhost:8001')
    ret = mystrategy.backtest_config(
        start_time='2016-04-15 9:00:00',
        end_time='2016-12-15 15:00:00',
        initial_cash=1000000,
        transaction_ratio=1,
        commission_ratio=0,
        slippage_ratio=0,
        price_type=1,
        bench_symbol='SHSE.000300')#基准=沪深300
    print('config status: ', ret)
    ret = mystrategy.run()
    print('exit code: ', ret)
