import tradestat
import talib as tl
from sqlalchemy import desc
import numpy as np
import pandas as pd

# 初始化函数，设定要操作的股票、基准等等
def initialize(context):
    # 定义一个全局变量, 保存要操作的股票
    # 000905.XSHG    中证500
    # 000300.XSHG    沪深300
    g.security = set_feasible_stocks(context)
    #g.security = ['SH000905.XG']
    #g.security = get_index_stocks('000300.XSHG')
    g.buy_list=[]
    g.portfolio_stock=[]
    g.stocks_max_price=dict()
   
    #设定沪深300作为基准
    set_benchmark('000300.XSHG')
    set_param()

# 每个单位时间(如果按天回测,则每天调用一次,如果按分钟,则每分钟调用一次)调用一次
def handle_data(context, data):
    #if cash<2000  : return                   #没钱直接跳过 
    stocks = g.security
    # 获取股票的收盘价
    cash = context.portfolio.cash
   
             # 获得当前时间
    hour = context.current_dt.hour
    minute = context.current_dt.minute
    buy_list=g.buy_list
    
#    if minute%60!=45 :return



 

    #个股止损
    for stock_zs in context.portfolio.positions.keys():
        #跳过清仓股
        if context.portfolio.positions[stock_zs].total_amount==0: continue
        #跳过停牌股
        is_paused=get_price(stock_zs, context.current_dt-datetime.timedelta(minutes=2), context.current_dt, '1m', ['paused'],False)['paused'][0]
        if is_paused :continue
        #更新最大price
        price_date=get_price(stock_zs, context.current_dt-datetime.timedelta(days=1), context.current_dt, '1m', ['high', 'close','paused'],True)

        high_now=price_date['high'].max()
        price_now=price_date['close'][-1]
        max_price=g.stocks_max_price[stock_zs]
        #log.info("high_now is %s--"%(high_now)+"max_price is %s--"%(max_price))
        if high_now>max_price :
            
            g.stocks_max_price[stock_zs]=high_now 
        elif context.portfolio.positions[stock_zs].total_amount==0:
            g.stocks_max_price[stock_zs]==-999
        
        log.info("high:%s,price_now:%s"%(g.stocks_max_price[stock_zs],price_now))
           
        #个股止损
        if price_now/g.stocks_max_price[stock_zs]<=0.9 :
            order_target(stock_zs, 0)
            g.stocks_max_price[stock_zs]==-999
            print "sell",stock_zs,":gegu"
    
    #没钱，结束         
    if  context.portfolio.cash<2000 : return   
       

    #选股票
    #log.info("stocklist --------------------------")
    fall_price=[]
    for stock in stocks:
        close_prices=attribute_history(stock, 70, '1d', ['close'],True)['close']
        fall_price.append((stock,close_prices.max()/close_prices[-1]-1))
    
    fall_price=sorted(fall_price,key=lambda item:item[1],reverse=True)
    stocks=fall_price[:200]
    
    buy_list=[]
    for stock in stocks :
        #value_money_fall=money_fall(stock[0],20,60,160)
        value_buy_macd=buy_macd(stock[0])
        if value_buy_macd>0:
            buy_list.append(stock[0])
    
    print buy_list
    #买入
    count =0
    count_buy=0
    while count_buy <4 and count<len(buy_list):
        stock=buy_list[count]
        order_value(stock, 10000)
        print stock+"^^^^^^^^^^^^^^^^^buy"
        count_buy=count_buy+1
        g.stocks_max_price[stock]= \
        get_price(stock, context.current_dt-datetime.timedelta(days=1), context.current_dt, '1m', ['high'],True)['high'].max()
        count=count+1
           
            
            
            
            
            
            
                
            
def set_feasible_stocks(context):
    # 得到是否停牌信息的dataframe，停牌的1，未停牌得0
    suspened_info_df = get_price(list(get_all_securities('stock').index),start_date=context.current_dt,end_date=context.current_dt,frequency='daily', fields='paused')['paused'].T
    # 过滤停牌股票 返回dataframe
    unsuspened_index = suspened_info_df.iloc[:,0]<1
    # 得到当日未停牌股票的代码list:
    unsuspened_stocks = suspened_info_df[unsuspened_index].index
    return unsuspened_stocks            
    


   
    
    

        

#判断缩量 
#返回值1代表 160日、60日、20日成交额均值依次减小，中短线缩量比较彻底
#返回值0.5代表 60日、160日、20日成交额均值依次减小，短线缩量比较彻底
#返回值0代表其他情况，还未缩量
def money_fall(security,n1=20,n2=60,n3=160):
    money_data = attribute_history(security, n3+2, '1d', ['money','high','open','close'],True)
    money_MA_n1_1=money_data['money'][-n1:].mean()
    money_MA_n1_2=money_data['money'][-n1-1:-2].mean()
    money_MA_n1_3=money_data['money'][-n1-2:-3].mean()
    
    money_MA_n2_1=money_data['money'][-n2:].mean()
    money_MA_n2_2=money_data['money'][-n2-1:-2].mean()
    money_MA_n2_3=money_data['money'][-n2-2:-3].mean()
    
    money_MA_n3_1=money_data['money'][-n3:].mean()
    money_MA_n3_2=money_data['money'][-n3-1:-2].mean()
    money_MA_n3_3=money_data['money'][-n3-2:-3].mean()
    
    if not (money_MA_n3_1>money_MA_n3_2 and money_MA_n3_2>=money_MA_n3_3): return 0
    
    if  money_MA_n1_1<money_MA_n2_1 and money_MA_n2_1<money_MA_n3_1 \
    and money_MA_n1_2<money_MA_n2_2 and money_MA_n2_2<money_MA_n3_2 \
    and money_MA_n1_3<money_MA_n2_3 and money_MA_n2_3<money_MA_n3_3 :
        return 1
    elif money_MA_n1_1<money_MA_n3_1 and money_MA_n3_1<money_MA_n2_1 \
    and  money_MA_n1_2<money_MA_n3_2 and money_MA_n3_2<money_MA_n2_2 \
    and  money_MA_n1_3<money_MA_n3_3 and money_MA_n3_3<money_MA_n2_3 :
        return 0.5
        
    return 0

#-----------------------------------------------------------------------end  



       
#大盘28止损，用于清仓，建仓时不需要使用28指数  
def market_stop_loss_by_28_index(context):
    # 回看指数前20天的涨幅
    gr_index2 = get_growth_rate(g.index2)
    gr_index8 = get_growth_rate(g.index8)
    flag28=False
   
    if gr_index2 <= g.index_growth_rate and gr_index8 <= g.index_growth_rate:
        flag28=True
    else: flag28=False
    
    return flag28
    
# 获取股票n日以来涨幅，根据当前价计算
# n 默认20日
def get_growth_rate(security, n=20):
    lc = get_close_price(security, n)
    #c = data[security].close
    c = get_close_price(security, 1)
    
    if not isnan(lc) and not isnan(c) and lc != 0:
        return (c - lc) / lc
    else:
        log.error("数据非法, security: %s, %d日收盘价: %f, 当前价: %f" %(security, n, lc, c))
        return 0

# 获取股票n日以来涨幅，根据当前价计算
# n 默认20日
def get_growth_rate_before(security, n=20):
    lc = get_close_price(security, n+1)
    #c = data[security].close
    c = get_close_price(security, 2)
    
    if not isnan(lc) and not isnan(c) and lc != 0:
        return (c - lc) / lc
    else:
        log.error("数据非法, security: %s, %d日收盘价: %f, 当前价: %f" %(security, n, lc, c))
        return 0
        
# 获取前n个单位时间当时的收盘价
def get_close_price(security, n, unit='1d'):
    return attribute_history(security, n, unit, ('close'), True)['close'][0]
    
    
#计算macd 参考了软猫克鲁大神的帖子
def MACD_CN(close, fastperiod, slowperiod, signalperiod) :
    macdDIFF, macdDEA, macd = tl.MACDEXT(close, fastperiod=fastperiod, fastmatype=1, slowperiod=slowperiod, slowmatype=1, signalperiod=signalperiod, signalmatype=1)
    macd = macd * 2
    return macdDIFF, macdDEA, macd
    


#结合缩量，选择买入时机  
#返回值n表示买入系数n,n越大可买入越多。
def buy_macd(index2):
    hData = attribute_history(index2, 60, unit='1d', fields=('close'), skip_paused=True)
    close = hData['close']
    close = np.array(close, dtype='f8')
    wDif, wDea, wMacd =  MACD_CN(close, 12, 26, 9)
    if wMacd[-1] > 0 and  wMacd[-2]<=0 :
        return 1
    return 0

def wMacd_macd(index2):
    hData = attribute_history(index2, 60, unit='1d', fields=('close'), skip_paused=True)
    close = hData['close']
    close = np.array(close, dtype='f8')
    wDif, wDea, wMacd =  MACD_CN(close, 12, 26, 9)
    return wMacd[-1]
   
    
def KDJ_CN(high, low, close, fastk_period, slowk_period, fastd_period) :
    kValue, dValue = tl.STOCHF(high, low, close, fastk_period, fastd_period=1, fastd_matype=0)
    
    kValue = np.array(map(lambda x : SMA_CN(kValue[:x], slowk_period), range(1, len(kValue) + 1)))
    dValue = np.array(map(lambda x : SMA_CN(kValue[:x], fastd_period), range(1, len(kValue) + 1)))
    
    jValue = 3 * kValue - 2 * dValue
    
    func = lambda arr : np.array([0 if x < 0 else (100 if x > 100 else x) for x in arr])
    
    kValue = func(kValue)
    dValue = func(dValue)
    jValue = func(jValue)
    return kValue, dValue, jValue
    
    
def set_param():
   
    
    # 配置二八指数
    #g.index2 = '000300.XSHG'  # 沪深300指数，表示二，大盘股
    #g.index8 = '000905.XSHG'  # 中证500指数，表示八，小盘股
    g.index2 = '000016.XSHG'  # 上证50指数
    g.index8 = '399333.XSHE'  # 中小板R指数
    #g.index8 = '399006.XSHE'  # 创业板指数
    
    # 判定调仓的二八指数20日增幅
    #g.index_growth_rate = 0.00
    g.index_growth_rate = 0.01

    # 配置是否根据大盘历史价格止损
    # 大盘指数前130日内最高价超过最低价2倍，则清仓止损
    # 注：关闭此止损，收益增加，但回撤会增加
    g.is_market_stop_loss_by_price = True
    if g.is_market_stop_loss_by_price:
        # 配置价格止损判定指数，默认为上证指数，可修改为其他指数
        g.index_4_stop_loss_by_price = '000001.XSHG'

    # 配置三黑鸦判定指数，默认为上证指数，可修改为其他指数
    g.index_4_stop_loss_by_3_black_crows = '000001.XSHG'

    # 配置是否开启大盘三黑鸦止损
    # 个人认为针对大盘判断三黑鸦效果并不好，首先有效三只乌鸦难以判断，准确率实际来看也不好，
    # 其次，分析历史行情看一般大盘出现三只乌鸦的时候，已经严重滞后了，使用其他止损方式可能会更好
    g.is_market_stop_loss_by_3_black_crows = True
    if g.is_market_stop_loss_by_3_black_crows:
        g.dst_drop_minute_count = 60

    # 是否根据28指数值实时进行止损
    g.is_market_stop_loss_by_28_index = False
    if g.is_market_stop_loss_by_28_index:
        # 配置当日28指数连续为跌的分钟计数达到指定值则止损
        g.dst_minute_count_28index_drop = 120

    # 配置是否个股止损
    g.is_stock_stop_loss = False
    # 配置是否个股止盈
    g.is_stock_stop_profit = False   
    