# -*- coding: utf-8 -*-

from gmsdk.api import StrategyBase

class Mystrategy(StrategyBase):
    def __init__(self, *args, **kwargs):
        super(Mystrategy, self).__init__(*args, **kwargs)


    def on_login(self):
        pass

    def on_error(self, code, msg):
        pass

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