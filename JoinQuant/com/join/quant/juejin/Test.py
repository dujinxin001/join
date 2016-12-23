# -*- coding: utf-8 -*-
from com.join.quant.juejin.process_xsz import xiaoshizhi
from gmsdk.api import StrategyBase

class Mystrategy(StrategyBase):
    global xiaoshizhi
    def __init__(self, *args, **kwargs):
        super(Mystrategy, self).__init__(*args, **kwargs)
        self.xiaoshizhi=xiaoshizhi(self)
        self.xiaoshizhi.initialize()
        self.xiaoshizhi.before_trading_start();
    def on_login(self):
        print('登录')
        pass

    def on_error(self, code, msg):
        pass

    def on_tick(self, tick):
        print('on_tick')
    def on_bar(self, bar):
        self.xiaoshizhi.handle_data(bar)
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
    myStrategy = Mystrategy(config_file='xsz_backtest.ini', config_file_encoding='UTF-8')
    ret = myStrategy.run()
    print('exit code: ', ret)