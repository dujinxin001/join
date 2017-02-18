import datetime
from com.join.quant.juejin.process_xsz import xiaoshizhi
from gmsdk.api import StrategyBase
import numpy as np
import pandas as pd
from _operator import index
import logging as log
import tushare as ts

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
        #list=self.get_market_index('SHSE.600000,SHSE.600010,SHSE.600399', startDate, endDate)
        #[print(m) for m in list] 
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
    #startDate=strBarDate- datetime.timedelta(minutes=1)
    startDate=strBarDate.strftime('%Y-%m-%d')
    endDate=strBarDate.strftime('%Y-%m-%d')
    listaa=myStrategy.get_market_index('SHSE.603520', '2016-03-09', '2016-03-09')
    list2=myStrategy.get_financial_index('SHSE.603520', '2016-01-09', '2016-03-09')
    list_bar=myStrategy.get_bars('SHSE.600791',60,'2016-11-17 14:47:00', '2016-11-17 14:49:00')
    lis_ts=ts.get_stock_basics()
    print(lis_ts['timeToMarket'][-1])
    for a in list_bar:
        log.info(list_bar[0].close)
        log.info(a.close )
    dic={}
    for m in listaa:
        #print(m.symbol+','+str(m.market_value))
        if m.symbol in dic.keys():
            a={m.pub_date:m}
            dic[m.symbol].update(a)
        else:
            a={m.pub_date:m}
            #array.append(list(m.pub_date+','+str(m.market_value)))
            dic[m.symbol]=a
           
    #dic2={m.symbol:{m.pub_date:m.market_value} for m in listaa if m.symbol==m2.symbol for m2 in listaa}
    #print(dic)
    #index=set([a[0] for a in dic2.keys()])
    #columns=set([a[1] for a in dic2.keys()])
    #print(index)
    #print(columns)
    df = pd.DataFrame(dic)
    log.info(df)
    #[print(m.symbol+','+m.pub_date+','+str(m.market_value)) for m in listaa] 
    #ret = myStrategy.run()
    print('exit code: ')