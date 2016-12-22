# -*- coding: utf-8 -*-

from gmsdk.api import StrategyBase

class Mystrategy():
    global g
    
    def __init__(self):
        self.exchange = ''          ## 交易所代码
        self.sec_id = ''            ## 证券ID

        self.bar_type = 0           ## bar类型
        self.strtime = ''           ## 可视化时间
        self.utc_time = 0.0         ## 行情时间戳

        self.open = 0.0             ## 开盘价
        self.high = 0.0             ## 最高价
        self.low = 0.0              ## 最低价
        self.close = 0.0            ## 收盘价
        self.volume = 0.0           ## 成交量
        self.amount = 0.0           ## 成交额

        self.position = 0.0         ## 仓位
        self.settle_price = 0.0     ## 结算价
        self.upper_limit = 0.0      ## 涨停价
        self.lower_limit = 0.0      ## 跌停价

    def on_login(self):
        self.param=['111','222','333']

    def on_error(self, code, msg):
        print(self.param[-2])

    def on_tick(self, tick):
        pass

    def on_execrpt(self, res):
        pass

    def on_order_status(self, order):
        pass

    def on_order_new(self, res):
        pass

    def on_order_filled(self, res):
        pass

    def on_order_partiall_filled(self, res):
        pass

    def on_order_stop_executed(self, res):
        pass

    def on_order_canceled(self, res):
        pass

    def on_order_cancel_rejected(self, res):
        pass


if __name__ == '__main__':
    myStrategy = Mystrategy(
        username='-',
        password='-',
        strategy_id='a52419e4-c5e4-11e6-8aa7-185e0f571106',
        subscribe_symbols='SHSE.000300.tick',
        mode=4,
        td_addr='localhost:8001'
    )
    myStrategy.backtest_config(
        start_time='2016-01-01 09:00:00',
        end_time='2016-12-19 15:00:00',
        initial_cash=150000,
        transaction_ratio=1,
        commission_ratio=0.0016,
        slippage_ratio=0.004,
        price_type=1)
    ret = myStrategy.run()
    print('exit code: ', ret)