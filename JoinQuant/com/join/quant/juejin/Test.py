import datetime
from com.join.quant.juejin.process_xsz import xiaoshizhi
from gmsdk.api import StrategyBase
import numpy as np
import pandas as pd
from _operator import index

class Mystrategy(StrategyBase):
    global xiaoshizhi
    def __init__(self, *args, **kwargs):
        super(Mystrategy, self).__init__(*args, **kwargs)
        self.xiaoshizhi=xiaoshizhi(self)
        #self.xiaoshizhi.initialize()
        #self.xiaoshizhi.before_trading_start();
    def on_login(self):
        print('登录')
        pass

    def on_error(self, code, msg):
        pass

    def on_tick(self, tick):
        print('on_tick')
    def on_bar(self, bar):
        strBarDate= datetime.datetime.fromtimestamp(bar.utc_time);
        startDate=strBarDate- datetime.timedelta(days=20)
        startDate=startDate.strftime('%Y-%m-%d')
        endDate=strBarDate.strftime('%Y-%m-%d')
        list=self.get_market_index('SHSE.600000,SHSE.600010,SHSE.600399', startDate, endDate)
        [print(m) for m in list] 
        #self.xiaoshizhi.handle_data(bar,startDate,endDate)
    def on_execrpt(self, res):
        pass

    def on_order_status(self, order):
        pass

    def on_order_new(self, res):
        print('on_order_new')
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
    strBarDate=1481263500.0
    strBarDate= datetime.datetime.fromtimestamp(strBarDate)
    startDate=strBarDate- datetime.timedelta(days=20)
    startDate=startDate.strftime('%Y-%m-%d')
    endDate=strBarDate.strftime('%Y-%m-%d')
    listaa=myStrategy.get_market_index('SHSE.600000,SHSE.600010,SHSE.600399', startDate, endDate)
    dic={(m.pub_date,m.symbol):m.market_value for m in listaa}
    print(dic.values())
    index=set([a[0] for a in dic.keys()])
    columns=set([a[1] for a in dic.keys()])
    df = pd.DataFrame(dic.values(), index=index ,columns=columns)
    print(df)
    #[print(m.symbol+','+m.pub_date+','+str(m.market_value)) for m in listaa] 
    #ret = myStrategy.run()
    print('exit code: ')