<<<<<<< HEAD
'''
二八小市值择时买卖
=======
import math
import numpy as np
import pandas as pd
import datetime
from com.join.quant.juejin.blacklist import get_blacklist
from com.join.quant.juejin.GObject import MyClass
import logging as log
import tushare as ts
>>>>>>> branch 'master' of https://github.com/dujinxin001/join.git

配置指定频率的调仓日，在调仓日每日指定时间，计算所选大盘指数和所选小盘指数当前的20日涨
幅，如果2个指数的20日涨幅有一个为正，则进行选股调仓，之后如此循环往复。

止损策略：

    大盘止损：(可选)
        1. 每分钟取大盘前160日的最低价和最高价，如果最高大于最低的两倍则清仓，停止交易。
        2. 每分钟判断大盘是否呈现三只黑鸦止损，如果是则当天清仓并停止交易，第二天停止交
           易一天。

    个股止损：(可选)
        每分钟判断个股是否从持仓后的最高价回撤幅度，如果超过个股回撤阈值，则平掉该股持仓

    二八止损：(必需)
        每日指定时间，计算沪深300指数和中证500指数当前的20日涨幅，如果2个指数涨幅都为负，
        则清仓，重置调仓计数，待下次调仓条件满足再操作

版本：v2.0.7
日期：2016.11.15
作者：Morningstar
'''
#enable_profile()

from collections import OrderedDict
from blacklist import *
import math
import tradestat
import talib as tl
import shipane_sdk


def initialize(context):
    log_section('initialize', '初始化', sep=False)

    # 设置系统参数
    set_sys()
    # 设置常数
    set_const()
    # 设置策略参数
    set_param()
    # 设置过滤器
    set_filter()
    # 设置止损器
    set_stop_loss()
    # 设置缓存参数
    set_cache()
    # 加载统计模块
    g.trade_stat = tradestat.trade_stat()

    # 打印自定义信息
    # log.info('')
    # 打印策略参数
    log_param()
    # 打印使用的过滤器
    log_filter()
    # 打印使用的止损器
    log_stop_loss()


def process_initialize(context):
    log_section('process_initialize','进程重新启动')
    # 创建 JoinQuantExecutor 对象
    # 可选参数包括：host, port, key, client, timeout 等
    # 请将下面的 IP 替换为实际 IP
    g.__executor = shipane_sdk.JoinQuantExecutor(
        host='106.15.37.132',
        port=11788,
        key='dujinxin77510312ea',
        client=''
    )
    log.info('实盘易初始化成功')


def after_code_changed(context):
    log_section('after_code_changed','代码发生修改')
    


def before_trading_start(context):
    log_section('before_trading_start', '盘前处理:又开始赚钱了')

    reset_day_param()
    # 盘前就判断三黑鸦状态，因为判断的数据为前4日
    g.cache['is_last_day_3_crows'] = _is_3_crows(
        g.param['index_3_crows'][g.VALUE])
    if g.cache['is_last_day_3_crows']:
        log.info("==> 前4日已经构成三黑鸦形态")


def after_trading_end(context):
    log_section('after_trading_end', '盘后处理：大赚收钱')

    g.trade_stat.report(context)
    # 得到当前未完成订单
    orders = get_open_orders()
    for _order in orders.values():
        log.info("取消未完成的订单: %s" % (_order.order_id))


def handle_data(context, data):
    '''
    按分钟回测
    '''
    # for key in g.stop_loss_minute:
    #     g.stop_loss_minute[key][g.FUNC](context, data)
    hour = context.current_dt.hour
    minute = context.current_dt.minute
    #进行所有止损器判断，执行止损
    for stop in g.stop_loss_minute:
        if hour==9 and minute==31:
            log.info("执行止损检查")
        stop[g.FUNC](context, data)

    # 每天指定时间检查是否调仓并处理
    adjust_position_time = g.param['adjust_position_time'][g.VALUE]
    if hour == adjust_position_time[g.HOUR] and minute == adjust_position_time[g.MINUTE] and not g.cache['stop_trade']:
        _adjust_position(context, data)


def _adjust_position(context, data):
    log.info("调仓日计数 [%d]" % (g.cache['day_count']))

    # 回看指数前20天的涨幅
    gr_index_l = get_growth_rate(g.param['index_l'][g.VALUE])
    gr_index_s = get_growth_rate(g.param['index_s'][g.VALUE])
    log.info("当前%s指数的20日涨幅 [%.2f%%]" % (
        get_security_info(g.param['index_l'][g.VALUE]).display_name, gr_index_l * 100))
    log.info("当前%s指数的20日涨幅 [%.2f%%]" % (
        get_security_info(g.param['index_s'][g.VALUE]).display_name, gr_index_s * 100))

    if gr_index_l <= g.param['index_growth_rate'][g.VALUE] and gr_index_s <= g.param['index_growth_rate'][g.VALUE]:
        position_clear(context)
        g.cache['day_count'] = 0
    else:
        if g.cache['day_count'] % g.param['period'][g.VALUE] == 0:
            log.info("==> 满足条件进行调仓")
            if gr_index_l > g.param['index_growth_rate'][g.VALUE] and gr_index_s < g.param['index_growth_rate'][g.VALUE]:
                stock_list = g.cache['stock_list']
                g.cache['stock_list']=filter_by_growth_rate(stock_list, context, data)
            buy_stocks = pick_stocks(context, data)
            if len(buy_stocks)==0:
                log.info("选股后无买股票: %s" %(buy_stocks))
                position_adjust(context, buy_stocks)
                g.cache['day_count']=0
                return
            log.info("选股后可买股票: %s" % (buy_stocks))
            position_adjust(context, buy_stocks)
        g.cache['day_count'] += 1


# 按月运行
# run_monthly(func, monthday, time='open', reference_security)
# 按周运行
# run_weekly(func, weekday, time='open', reference_security)
# 每天内何时运行
# run_daily(func, time='open', reference_security)


#### initialize ####

# 设置系统参数
def set_sys():
    # 以上证指数为基准
    # 设定上证指数作为基准
    #set_benchmark('000001.XSHG')
    # 设定沪深300指数作为基准
    set_benchmark('000300.XSHG')
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003,
                             close_commission=0.0003, close_today_commission=0, min_commission=5), type='stock')
    # 设定滑点为百分比
    set_slippage(PriceRelatedSlippage(0.004))
    # 使用真实价格回测,回测请注释
    set_option('use_real_price', True)

# 设置常数
def set_const():
    g.VALUE = 0
    g.FUNC = 0
    g.DESCR = 1
    g.HOUR = 0
    g.MINUTE = 1

# 设置策略参数
def set_param():
    p = {}

    p['period'] = (4, '调仓频率，单位：日')
    p['adjust_position_time'] = ((14, 49), '配置调仓时间（24小时分钟制）')
    p['pick_by_pe'] = (False, '是否根据PE选股')
    p['pick_by_eps'] = (True, '是否根据EPS选股')
    p['pick_stock_count'] = (100, '备选股票数目')
    p['filter_gem'] = (True, '是否过滤创业板股票')
    p['filter_blacklist'] = (True, '是否过滤黑名单股票，回测建议关闭，模拟运行时开启')
    p['is_filter_new']=(False,'是否过滤新股')
    p['is_rank_stock'] = (True, '是否对股票评分')
    p['rank_stock_count'] = (20, '参与评分的股票数目')  # 评分的股票数目不能大于备选股票数目
    p['index_l'] = ('000016.XSHG', '大盘股指数')  # 上证50指数
    p['index_s']=('399678.XSHE','小盘股指数') # 中证500指数
    p['buy_stock_count'] = (2, '买入股票数目')
    p['index_growth_rate'] = (0.005, '判定调仓的二八指数n日增幅')  # n = 20
    p['index_3_crows'] = ('000300.XSHG', '判定三黑鸦的指数')
    p['index_price'] = ('399678.XSHE', '判定价格止损的指数')
    p['is_stock_stop_loss']=(False,'是否个股止损')
    p['is_stock_stop_profit']=(False,'是否个股止盈')
    p['is_market_stop_loss_by_price'] = (True,'是否根据大盘历史价格止损')
    p['is_market_stop_loss_by_28_index']=(False,'是否根据28指数值实时进行止损')
    p['is_market_stop_loss_by_3_black_crows']=(True,'是否进行三黑鸦止损')

    g.param = p

# 设置过滤器
def set_filter():
    # g.filter={}
    g.filter = []
    #func_register(g.filter, filter_market_time, '指数MACD过滤')
    #func_register(g.filter,filter_by_growth_rate, '过滤n日增长率为负的股票')
    #func_register(g.filter, filter_by_20, '20日线过滤器')
    #func_register(g.filter, filter_old_stock, '过滤上市超过一年的')
    func_register(g.filter, filter_by_query, '查询财务数据库过滤')
    func_register(g.filter, filter_gem, '过滤创业版股票')
    func_register(g.filter, filter_paused, '过滤停牌股票')
    func_register(g.filter, filter_st, '过滤ST及其他具有退市标签的股票')
    func_register(g.filter, filter_limitup, '过滤涨停的股票')
    func_register(g.filter, filter_limitdown, '过滤跌停的股票')
    func_register(g.filter,filter_blacklist, '过滤黑名单股票')
    func_register(g.filter,filter_new, '过滤新股')
    #func_register(g.filter,filter_by_growth_rate, '过滤n日增长率为负的股票')
    #func_register(g.filter, filter_by_20, '20日线过滤器')
    #func_register(g.filter,filter_by_chaodie, '超跌过滤器')
    #func_register(g.filter, delect_stock, 'delect_stock')
    #func_register(g.filter, filter_by_rank_0, '市值评分过滤器')
    func_register(g.filter, filter_by_rank, '评分过滤器')
    #func_register(g.filter, cow_stock_value, '脉冲过滤')


def set_stop_loss():
    g.stop_loss_day = []
    g.stop_loss_minute = []
    
    func_register(g.stop_loss_minute, stop_loss_by_price, '大盘价格止损')
    func_register(g.stop_loss_minute, stop_loss_by_3_crows, '三黑鸦止损')
    func_register(g.stop_loss_minute,stop_loss_by_index_l,'二八止损')
    func_register(g.stop_loss_minute,stop_loss_by_stock,'个股止损')
    func_register(g.stop_loss_minute,stop_profit_by_stock,'个股止盈')


def set_cache():
    c = {}

    # 调仓日计数器，单位：日
    c['day_count'] = 0

    # 缓存股票持仓后的最高价
    c['last_high'] = {}

    df = get_fundamentals(query(valuation.code))
    c['stock_list'] = list(df['code'])
    #c['stock_list'] =get_index_stocks('000300.XSHG')

    # 缓存当日个股250天内最大的3日涨幅，避免当日反复获取，每日盘后清空
    c['pct_change'] = {}
    # 缓存指数下跌分钟数
    c['minute_count_index_ls_drop'] = 0
    c['minute_count_cur_drop'] = 0
    c['is_last_day_3_crows'] = False
    c['is_day_stop_loss_by_price'] = False
    c['stop_trade'] = False  # 暂停当天交易
    g.cache = c


def reset_day_param():
    '''
    重置当日参数，仅针对需要当日需要重置的参数
    '''
    log.info("=>盘后重置当日参数")
    df = get_fundamentals(query(valuation.code))
    g.cache['stock_list'] = list(df['code'])
    #g.cache['stock_list']=get_index_stocks('000300.XSHG')
    # 重置当日大盘价格止损状态
    g.cache['is_day_stop_loss_by_price'] = False

    # 重置三黑鸦状态
    g.cache['is_last_day_3_crows'] = False
    g.cache['minute_count_cur_drop'] = 0
    g.cache['minute_count_index_ls_drop'] = 0

    # 清空当日个股250天内最大的3日涨幅的缓存
    g.cache['pct_change'].clear()

    g.cache['stop_trade'] = False  # 暂停当天交易


#### pick & filter ####


def pick_stocks(context, data):
    '''
    选取指定数目的小市值股票，再进行过滤，最终挑选指定可买数目的股票
    '''
    stock_list = g.cache['stock_list']

    # for key in g.filter.keys():
    #     stock_list = g.filter[key][g.FUNC](stock_list, context, data)
    for filter in g.filter:
        stock_list = filter[g.FUNC](stock_list, context, data)
        if len(stock_list)==0:
           log.info("股票被过滤没了")
           return stock_list

    # 选取指定可买数目的股票
    return stock_list[:g.param['buy_stock_count'][g.VALUE]]


def cow_stock_value(stock_list, context, data):
    log.info("=>开始执行脉冲过滤")
    df = get_fundamentals(query(
                                valuation.code, valuation.pb_ratio, valuation.circulating_market_cap
                            ).filter(
                                valuation.code.in_(stock_list),
                                valuation.circulating_market_cap <= 100
                            ))
    log.info("=>开始执行脉冲过滤2")
    df.index = df['code']
    del df['code']
    s_fall = fall_money_day_3line(df.index.tolist(), 120, 20, 60, 160)
    log.info("=>开始执行脉冲过滤3")
    s_cross = money_5_cross_60(df.index.tolist(), 120)
    log.info("=>开始执行脉冲过滤4")
    df = pd.concat([df, s_fall, s_cross], axis=1, join='inner')
    log.info("=>开始执行脉冲过滤5")
    df.columns = ['pb', 'cap', 'fall', 'cross']
    df['score'] = df['fall'] * df['cross'] / (df['pb']*(df['cap']**0.5))
    df.sort(['score'], ascending=True, inplace=True)
    log.info("=>开始执行脉冲过滤6")
    log.info("=>结束执行脉冲过滤%s" %df)
    return df.index.tolist()


def money_5_cross_60(security_list,n, n1=5, n2=60):
    def money_5_cross_60_count(money, n, n1, n2):
        i = 0
        count = 0
        while i < n :
            money_MA60 = money[i+1:n2+i].mean()
            money_MA60_before = money[i:n2-1+i].mean()
            money_MA5 = money[i+1+n2-n1:n2+i].mean()
            money_MA5_before = money[i+n2-n1:n2-1+i].mean()
            if (money_MA60_before-money_MA5_before)*(money_MA60-money_MA5) < 0 : 
                count=count+1
            i = i + 1
        return count

    df = history(n+n2+1, unit='1d', field='money', security_list=security_list, skip_paused=True)
    s = df.apply(money_5_cross_60_count, args=(n,n1,n2,))
    return s

def fall_money_day_3line(security_list,n, n1=20, n2=60, n3=160):
    def fall_money_count(money, n, n1, n2, n3):
        i = 0
        count = 0
        while i < n:
            money_MA200 = money[i:n3-1+i].mean()
            money_MA60 = money[i+n3-n2:n3-1+i].mean()
            money_MA20 = money[i+n3-n1:n3-1+i].mean()
            if money_MA20 <= money_MA60 and money_MA60 <= money_MA200 :
                count = count + 1
            i = i + 1
        return count

    df = history(n+n3, unit='1d', field='money', security_list=security_list, skip_paused=True)
    log.info("=>1111111111111")
    s = df.apply(fall_money_count, args=(n,n1,n2,n3,))
    log.info("=>222222222")
    return s


def filter_by_query(stock_list, context, data):
    '''
    查询财务数据库过滤
    '''
    log.info("=>开始执行财务条件过滤")
    pe_min = 0
    pe_max = 200
    eps_min = 0

    q = query(valuation.code).filter(valuation.code.in_(stock_list))
    #q=query(valuation).filter(valuation.market_cap>=50,valuation.code.in_(stock_list))
    if g.param['pick_by_pe'][g.VALUE]:
        q = q.filter(
            valuation.pe_ratio > pe_min,
            valuation.pe_ratio < pe_max
        )
    if g.param['pick_by_eps'][g.VALUE]:
        q = q.filter(indicator.eps > eps_min)
        
    df = get_fundamentals(
        q.order_by(valuation.market_cap.asc()
                   ).limit(
            g.param['pick_stock_count'][g.VALUE]
        ))

    return list(df['code'])


# def filter_by_pe(stock_list, context, data):
#     '''
#     过滤不在PE范围内股票
#     '''
#     min=0
#     max=200
#     df=get_fundamentals(query(
#         valuation.code
#     ).filter(
#         valuation.code.in_(stock_list),
#         valuation.pe_ratio > min,
#         valuation.pe_ratio < max)
#     )
#     return df['code']


# def filter_by_eps(stock_list, context, data):
#     '''
#     过滤不在EPS范围内股票
#     '''
#     min=0
#     # max = 200
#     df=get_fundamentals(query(
#         valuation.code
#     ).filter(
#         valuation.code.in_(stock_list),
#         indicator.eps > min,
#     )
#     return df['code']


def filter_paused(stock_list, context, data):
    '''
    过滤停牌股票
    '''
    log.info("=>开始执行过滤停牌的股票")
    current_data = get_current_data()
    return [stock for stock in stock_list if not current_data[stock].paused]


def filter_st(stock_list, context, data):
    '''
    过滤ST及其他具有退市标签的股票
    '''
    log.info("=>开始执行过滤ST及其他具有退市标签的股票")
    current_data = get_current_data()
    return [stock for stock in stock_list
            if not current_data[stock].is_st
            and not current_data[stock].name.startswith('退')]


def filter_gem(stock_list, context, data):
    '''
    过滤创业版股票
    '''
    if g.param['filter_gem'][g.VALUE]:
        log.info("=>开始执行过滤创业板股票")
        return [stock for stock in stock_list if stock[0:3] != '300']
    return stock_list


def filter_limitup(stock_list, context, data):
    '''
    过滤涨停的股票
    '''
    log.info("=>开始执行过滤涨停的股票")
    threshold = 1.00
    # last_prices = history(1, unit='1m', field='close',
    #                       security_list=stock_list)

    # 已存在于持仓的股票即使涨停也不过滤，避免此股票再次可买，但因被过滤而导致选择别的股票
    # return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
    #         or last_prices[stock][-1] < data[stock].high_limit * threshold]
    return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
            or data[stock].close < data[stock].high_limit * threshold]


def filter_limitdown(stock_list, context, data):
    '''
    过滤跌停的股票
    '''
    log.info("=>开始执行过滤跌停的股票")
    threshold = 1.00
    # last_prices = history(1, unit='1m', field='close',
    #                       security_list=stock_list)

    # return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
    #         or last_prices[stock][-1] > data[stock].low_limit * threshold]
    return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
            or data[stock].close > data[stock].low_limit * threshold]


def filter_by_growth_rate(stock_list, context, data):
    '''
    过滤n日增长率为负的股票
    '''
    log.info("=>开始过滤n日增长率为负的股票")
    n = 25
    return [stock for stock in stock_list if get_growth_rate(stock, n) > 0]


def filter_blacklist(stock_list, context, data):
    '''
    过滤黑名单股票
    '''
    if g.param['filter_blacklist'][g.VALUE]:
        log.info("=>开始过滤黑名单的股票")
        blacklist = get_blacklist()
        return [stock for stock in stock_list if stock not in blacklist]
    return stock_list

def filter_new(stock_list, context, data):
    '''
    过滤新股
    '''
    if g.param['is_filter_new'][g.VALUE]:
        log.info("=>开始过滤新股")
        delta = 60
        start_date = context.current_dt.date() - datetime.timedelta(delta)
        return [stock for stock in stock_list if get_security_info(stock).start_date < start_date]
    return stock_list


def filter_old_stock(stock_list, context, data):
    tmpList = []
    for stock in stock_list :
        days_public=(context.current_dt.date() - get_security_info(stock).start_date).days
        # 上市未超过1年
        if days_public < 365:
            tmpList.append(stock)
    return tmpList

def filter_by_20(stock_list, context, data):
    log.info("=>开始进行股票20日线过滤%s" %stock_list)
    return [stock for stock in stock_list if  get_growth_rate(stock,25)>0]
    #return [stock for stock in stock_list if (data[stock].mavg(20, field='close')-f_ref(stock,20))/20>0]
    '''
    for stock in stock_list:
        ene=0.91*data[stock].mavg(10, field='close')
        log.info("%s的ene值为：%s"%(stock,ene))
        if data[stock].low<=ene:
            stock_list2.append(stock);
    return stock_list2
    '''
    #return [stock for stock in stock_list if ]
       
def f_ref(index,N):
    h = attribute_history(index, N, '240m', ('close'))
    return h['close'][0:N].mean()

def filter_by_rank_0(stock_list, context, data):
    '''
    评分过滤器
    '''
    log.info("=>开始进行股票市值评分%s" %stock_list)
    if len(stock_list) >g.param['rank_stock_count'][g.VALUE]:
        stock_list = stock_list[:g.param['rank_stock_count'][g.VALUE]]
    if len(stock_list) > 0:
        dst_stocks = {}
        for stock in stock_list:
            q = query(valuation).filter(valuation.code == stock)
            df = get_fundamentals(q)
            log.info("股票%s的总市值%s"%(stock,df['market_cap'][0]))
            #h = attribute_history(stock, 5, unit='1d', fields=('money'), skip_paused=True)
            #low_price_130 = h.low.min()
            #high_price_130 = h.high.max()
    
            avg_5 = data[stock].mavg(5, field='money')
            log.info("股票%s的5日平均成交额%s"%(stock,avg_5))
            score = (2.2*math.log(df['market_cap'][0]))+(1.22*math.log(avg_5))
            # score = ((cur_price-low_price_130) + (cur_price-high_price_130) +
            # (cur_price-avg_15)) / cur_price
            dst_stocks[stock] = score
    
<<<<<<< HEAD
        df = pd.DataFrame(dst_stocks.values(), index=dst_stocks.keys())
        df.columns = ['score']
        df = df.sort(columns='score', ascending=True)
        log.info("<=个股市值评分结束")
        return df.index


def filter_by_rank(stock_list, context, data):
    '''
    评分过滤器
    '''
    if g.param['is_rank_stock'][g.VALUE]: 
        log.info("=>开始进行股票评分%s" %stock_list)
        if len(stock_list) >g.param['rank_stock_count'][g.VALUE]:
            stock_list = stock_list[:g.param['rank_stock_count'][g.VALUE]]
=======
    def set_cache(self):
        c = {}
    
        # 调仓日计数器，单位：日
        c['day_count'] = 0
    
        # 缓存股票持仓后的最高价
        c['last_high'] = {}
    
        c['stock_list']=[]
        #c['stock_list']=self.get_all_symbol(1, 1,now_date)
    
        # 缓存当日个股250天内最大的3日涨幅，避免当日反复获取，每日盘后清空
        c['pct_change'] = {}
        # 缓存指数下跌分钟数
        c['minute_count_index_ls_drop'] = 0
        c['minute_count_cur_drop'] = 0
        c['is_last_day_3_crows'] = False
        c['is_day_stop_loss_by_price'] = False
        c['stop_trade'] = False  # 暂停当天交易
    
        self.g.cache = c
    
    
    def reset_day_param(self,now_date):
        '''
                        重置当日参数，仅针对需要当日需要重置的参数
        '''
        log.info("=>重置当日参数")
        self.g.cache['stock_list']=self.get_all_symbol(1, 1,now_date)
        # 重置当日大盘价格止损状态
        self.g.cache['is_day_stop_loss_by_price'] = False
    
        # 重置三黑鸦状态
        self.g.cache['is_last_day_3_crows'] = False
        self.g.cache['minute_count_cur_drop'] = 0
        self.g.cache['minute_count_index_ls_drop'] = 0
    
        # 清空当日个股250天内最大的3日涨幅的缓存
        self.g.cache['pct_change'].clear()
    
        self.g.cache['stop_trade'] = False  # 暂停当天交易
    
    
    #### pick & filter ####
    
    
    def pick_stocks(self,now_date):
        '''
                        选取指定数目的小市值股票，再进行过滤，最终挑选指定可买数目的股票
        '''
        stock_list = self.g.cache['stock_list']
        # for key in self.g.filter.keys():
        #     stock_list = self.g.filter[key][self.g.FUNC](stock_list, context, data)
        for filter in self.g.filter:
            stock_list = filter[self.g.FUNC](stock_list,now_date)
            if len(stock_list) == 0:
                log.info("股票被过滤没了")
                return stock_list
    
        # 选取指定可买数目的股票
        return stock_list[:self.g.param['buy_stock_count'][self.g.VALUE]]
    
    
    def filter_by_query(self,stock_list,now_date):
        '''
                        查询财务数据库过滤
        '''
        log.info("=>开始执行财务条件过滤%s,%s" % (','.join(stock_list), len(stock_list)))
        pe_min = 0
        pe_max = 200
        eps_min = 0
        #过滤PE
        if self.g.param['pick_by_pe'][self.g.VALUE]:
            market_list=self.strategy.get_market_index(','.join(stock_list),now_date,now_date)
            stock_list=[market.symbol for market in market_list if market.pe_ratio>pe_min and market.pe_ratio<pe_max]
        
        #过滤EPS
        if self.g.param['pick_by_eps'][self.g.VALUE]:
            financial_list=self.strategy.get_financial_index(','.join(stock_list),self.pre_days(now_date, -90),now_date)
            stock_list=[financial.symbol for financial in financial_list if financial.eps>eps_min]
            
        #市值排序
        market_list=self.strategy.get_market_index(','.join(stock_list),self.pre_days(now_date, 0),self.pre_days(now_date, 0))
        market_dict={market.symbol:market.market_value for market in market_list}
        
        df = pd.DataFrame(list(market_dict.values()), index=market_dict.keys())
        df.columns = ['market_score']
        df = df.sort(columns='market_score', ascending=True)
        stock_list=list(df.index)
        stock_list=stock_list[0:self.g.param['pick_stock_count'][self.g.VALUE]]
        log.info("=>结束执行财务条件过滤%s" % stock_list)
        return stock_list
    
    def filter_paused(self,stock_list,now_date):
        '''
                        过滤停牌股票
        '''
        log.info("=>开始执行过滤停牌的股票%s" % stock_list)
        list_bar=self.strategy.get_bars(','.join(stock_list),60,now_date,now_date)
        dic={}
        for bar in list_bar:
            key=bar.exchange+'.'+bar.sec_id
            if key in dic.keys():
                a={bar.strendtime :bar.close}
                dic[key].update(a)
            else:
                a={bar.strendtime :bar.close}
                dic[key]=a
        df = pd.DataFrame(dic)
        
        list_daily=self.strategy.get_dailybars(','.join(stock_list),self.pre_days(now_date, 0),self.pre_days(now_date, 0))
        dic2={}
        for daily in list_daily:
            key=daily.exchange+'.'+daily.sec_id
            if key in dic2.keys():
                a={daily.strtime  :daily.close}
                dic2[key].update(a)
            else:
                a={bar.strtime  :daily.close}
                dic2[key]=a
        df2 = pd.DataFrame(dic2)
        #stock_list=[stock for stock in stock_list  if len(df[stock])>0 and len(df2[stock])>0]
        stock_list=list(set(list(df.columns)) & set(list(df2.columns)))
        return stock_list
    
    
    def filter_st(self,stock_list,now_date):
        '''
                        过滤ST及其他具有退市标签的股票
        '''
        log.info("=>开始执行过滤ST及其他具有退市标签的股票%s" % stock_list)
        list_sh=self.strategy.get_instruments('SHSE',1,1)
        list_sz=self.strategy.get_instruments('SZSE', 1, 1)
        list_all=list(set(list_sh).union(set(list_sz)))
        tui_list=[]
        for instrument in list_all:
            if instrument.sec_name.startswith('退') or instrument.sec_name.startswith('st') or instrument.sec_name.startswith('ST') or instrument.sec_name.startswith('*'):
                tui_list.append(instrument.symbol);
        return [stock for stock in stock_list
                if stock not in tui_list]
    
    
    def filter_gem(self,stock_list,now_date):
        '''
                        过滤创业版股票
        '''
        if self.g.param['filter_gem'][self.g.VALUE]:
            log.info("=>开始执行过滤创业板股票%s" % stock_list)
            return [stock for stock in stock_list if not stock.split('.')[1].startswith('300')]
        return stock_list
    
    
    def filter_limitup(self,stock_list,now_date):
        '''
                        过滤涨停的股票
        '''
        log.info("=>开始执行过滤涨停的股票%s" % stock_list) 
        threshold = 1.00
        # 已存在于持仓的股票即使涨停也不过滤，避免此股票再次可买，但因被过滤而导致选择别的股票
        list_bar=self.strategy.get_bars(','.join(stock_list),60,self.pre_minute(now_date, -3),self.pre_minute(now_date, -1))
        dic={}
        for bar in list_bar:
            key=bar.exchange+'.'+bar.sec_id
            if key in dic.keys():
                a={bar.strendtime :bar.close}
                dic[key].update(a)
            else:
                a={bar.strendtime :bar.close}
                dic[key]=a
        df = pd.DataFrame(dic)
        df=df.fillna(method='pad')
        list_daily=self.strategy.get_dailybars(','.join(stock_list),self.pre_days(now_date,0),self.pre_days(now_date, 0))
        dic2={}
        for daily in list_daily:
            key=daily.exchange+'.'+daily.sec_id
            if key in dic2.keys():
                a={daily.strtime  :daily.pre_close}
                dic2[key].update(a)
            else:
                a={bar.strtime  :daily.pre_close}
                dic2[key]=a
        df2 = pd.DataFrame(dic2)
        df2=df2.fillna(method='pad')
        stock_list=[stock for stock in stock_list  
                    if stock in [position.exchange +'.'+position.sec_id for position in self.strategy.get_positions()] 
                    or df[stock][-1] < df2[stock][-1] * (threshold+0.097)]
        return stock_list
    
    
    def filter_limitdown(self,stock_list,now_date):
        '''
                        过滤跌停的股票
        '''
        log.info("=>开始执行过滤跌停的股票%s" % stock_list)
        threshold = 1.00
        list_bar=self.strategy.get_bars(','.join(stock_list),60,self.pre_minute(now_date, -3),self.pre_minute(now_date, -1))
        dic={}
        for bar in list_bar:
            key=bar.exchange+'.'+bar.sec_id
            if key in dic.keys():
                a={bar.strendtime :bar.close}
                dic[key].update(a)
            else:
                a={bar.strendtime :bar.close}
                dic[key]=a
        df = pd.DataFrame(dic)
        df=df.fillna(method='pad')
        list_daily=self.strategy.get_dailybars(','.join(stock_list),self.pre_days(now_date, 0),self.pre_days(now_date, 0))
        dic2={}
        for daily in list_daily:
            key=daily.exchange+'.'+daily.sec_id
            if key in dic2.keys():
                a={daily.strtime  :daily.pre_close}
                dic2[key].update(a)
            else:
                a={bar.strtime  :daily.pre_close}
                dic2[key]=a
        df2 = pd.DataFrame(dic2)
        df2=df2.fillna(method='pad')
        stock_list=[stock for stock in stock_list  
                    if stock in [position.exchange +'.'+position.sec_id for position in self.strategy.get_positions()] 
                    or df[stock][-1] > df2[stock][-1] * (threshold-0.097)]
        return stock_list
    
    def filter_by_growth_rate(self,stock_list):
        '''
                        过滤n日增长率为负的股票
        '''
        n = 20
        return [stock for stock in stock_list if self.get_growth_rate(stock, n) > 0]
    
    
    def filter_blacklist(self,stock_list,now_date):
        '''
                        过滤黑名单股票
        '''
        if self.g.param['filter_blacklist'][self.g.VALUE]:
            log.info("=>开始过滤黑名单的股票%s" % stock_list)
            blacklist = get_blacklist()
            return [stock for stock in stock_list if stock not in blacklist]
        return stock_list
    
    def filter_new(self,stock_list):
        '''
                        过滤新股
        
        if self.g.param['is_filter_new'][self.g.VALUE]:
            log.info("=>开始过滤新股%s" % stock_list)
            delta = 60
            start_date = context.current_dt.date() - datetime.timedelta(delta)
            return [stock for stock in stock_list if get_security_info(stock).start_date < start_date]
        return stock_list
        '''
        pass
    
    
    def filter_by_rank_0(self,stock_list, context, data):
        '''
                        评分过滤器
        '''
        log.info("=>开始进行股票市值评分%s" % stock_list)
>>>>>>> branch 'master' of https://github.com/dujinxin001/join.git
        if len(stock_list) > 0:
            dst_stocks = {}
            for stock in stock_list:
                h = attribute_history(stock, 130, unit='1d', fields=('close', 'high', 'low'), skip_paused=True)
                low_price_130 = h.low.min()
                high_price_130 = h.high.max()
        
                avg_15 = data[stock].mavg(15, field='close')
                cur_price = data[stock].close
        
                # avg_15 = h['close'][-15:].mean()
                # cur_price = get_close_price(stock, 1, '1m')
        
                score = (cur_price - low_price_130)+(cur_price - high_price_130)+(cur_price - avg_15)
                # score = ((cur_price-low_price_130) + (cur_price-high_price_130) +
                # (cur_price-avg_15)) / cur_price
                dst_stocks[stock] = score
        
            df = pd.DataFrame(dst_stocks.values(), index=dst_stocks.keys())
            df.columns = ['score']
            df = df.sort(columns='score', ascending=True)
            log.info("<=个股评分结束")
            return df.index
<<<<<<< HEAD
=======
    
    
    def filter_by_rank(self,stock_list,now_date):
        '''
                        评分过滤器
        '''
        if self.g.param['is_rank_stock'][self.g.VALUE]: 
            log.info("=>开始进行股票评分%s" % stock_list)
            if len(stock_list) > self.g.param['rank_stock_count'][self.g.VALUE]:
                stock_list = stock_list[:self.g.param['rank_stock_count'][self.g.VALUE]]
            if len(stock_list) > 0:
                dst_stocks = {}
                list_bar=self.strategy.get_bars(','.join(stock_list),60,self.pre_minute(now_date, -3),self.pre_minute(now_date, -1))
                bar_dic={}
                for bar in list_bar:
                    key=bar.exchange+'.'+bar.sec_id
                    if key in bar_dic.keys():
                        a={bar.strendtime :bar.close}
                        bar_dic[key].update(a)
                    else:
                        a={bar.strendtime :bar.close}
                        bar_dic[key]=a
                bar_df = pd.DataFrame(bar_dic)               
                bar_df=bar_df.fillna(method='pad')
                
                dailybars=self.strategy.get_dailybars(','.join(stock_list),self.pre_days(now_date, -200),self.pre_days(now_date, -1))
                daily_dic={}
                for dailybar in dailybars:
                    key=dailybar.exchange+'.'+dailybar.sec_id
                    if key in daily_dic.keys():
                        a={dailybar.strtime :dailybar}
                        daily_dic[key].update(a)
                    else:
                        a={dailybar.strtime :dailybar}
                        daily_dic[key]=a
                df_daily = pd.DataFrame(daily_dic)
                df_daily=df_daily.fillna(method='pad')
                df_daily=df_daily.fillna(method='bfill')
                #log.info(df)
                for stock in stock_list:
                    #log.info(df[stock])
                    #dailybars=self.strategy.get_last_n_dailybars(stock, 130,self.pre_days(now_date, -1))
                    #dailybars=self.reversed_list(dailybars, 130)
                    #h = attribute_history(stock, 130, unit='1d', fields=('close', 'high', 'low'), skip_paused=True)
                    low_price=[]
                    high_price=[]
                    close_price=[]
                    dailybars_130=self.reversed_list(list(df_daily[stock]), 130)
                    for dailybar in dailybars_130:
                        #if dailybar!= 0:
                        low_price.append(dailybar.low)
                        high_price.append(dailybar.high)
                        close_price.append(dailybar.close)
                    low_price_130 = min(low_price)
                    high_price_130 = max(high_price)
                    avg_15 =  np.array(close_price)[-15:].mean()
                    #cur_price = self.strategy.get_last_n_bars(stock,60,1,self.pre_minute(now_date, -1))[0].close
                    cur_price=bar_df[stock][-1]
                    score = (cur_price - low_price_130) + (cur_price - high_price_130) + (cur_price - avg_15)
                    dst_stocks[stock] = score
                df = pd.DataFrame(list(dst_stocks.values()), index=dst_stocks.keys())
                df.columns = ['score']
                df = df.sort(columns='score', ascending=True)
                stock_list=list(df.index)
                log.info("<=个股评分结束%s"%stock_list)
                return stock_list
    
    #### stop loss ####
    
    def stop_loss_by_price(self,now_date):
        '''
                        大盘指数前130日内最高价超过最低价2倍，则清仓止损
                        基于历史数据判定，因此若状态满足，则当天都不会变化
                        增加此止损，回撤降低，收益降低
        '''
        if self.g.param['is_market_stop_loss_by_price'][self.g.VALUE]:
            index = self.g.param['index_price'][self.g.VALUE]
            if not self.g.cache['is_day_stop_loss_by_price']:
                dailybars=self.strategy.get_last_n_dailybars(index, 160,self.pre_days(now_date, -1))
                #dailybars=self.reversed_list(dailybars, 160)
                low_price=[]
                high_price=[]
                for dailybar in dailybars:
                    low_price.append(dailybar.low)
                    high_price.append(dailybar.high)
                low_price = min(low_price)
                high_price = max(high_price)
                # if high_price > 2 * low_price:
                if high_price > 2.2 * low_price \
                        and dailybars[0].close < dailybars[4].close \
                        and dailybars[0].close > dailybars[100].close:
                    # 当日第一次输出日志
                    log.info("==> 大盘止损，%s指数前130日内最高价超过最低价2倍, 最高价: %f, 最低价: %f" % (index, high_price, low_price))
                    self.g.cache['is_day_stop_loss_by_price'] = True
        
            if self.g.cache['is_day_stop_loss_by_price']:
                self.position_clear()
                self.g.cache['day_count'] = 0
                self.g.cache['stop_trade'] = True  # 暂停当天交易
    
        return self.g.cache['is_day_stop_loss_by_price']
    
    
    def stop_loss_by_3_crows(self,now_date):
        '''
                            前日三黑鸦，累计当日大盘指数涨幅<0的分钟计数
                            如果分钟计数超过值n，则开始进行三黑鸦止损
                            避免无效三黑鸦乱止损
        minute
        '''
        # 配置三黑鸦判定指数，默认为上证指数，可修改为其他指数
        index = self.g.param['index_3_crows'][self.g.VALUE]
        # 配置是否开启大盘三黑鸦止损
        # 个人认为针对大盘判断三黑鸦效果并不好，首先有效三只乌鸦难以判断，准确率实际来看也不好，
        # 其次，分析历史行情看一般大盘出现三只乌鸦的时候，已经严重滞后了，使用其他止损方式可能会更好
        n = 60
        if self.g.param['is_market_stop_loss_by_3_black_crows'][self.g.VALUE]: 
            if self.g.cache['is_last_day_3_crows']:
                if self.get_growth_rate(index, 1,now_date) < 0:
                    self.g.cache['minute_count_cur_drop'] += 1
                if self.g.cache['minute_count_cur_drop'] >= n:
                    if self.g.cache['minute_count_cur_drop'] == n:
                        log.info("==> 当日%s增幅 < 0 已超过%d分钟，执行三黑鸦止损" % (index, n))
        
                    self.position_clear()
                    self.g.cache['day_count'] = 0
                    self.g.cache['stop_trade'] = True  # 暂停当天交易
                    return True
            return False
        return False
    
    
    def _is_3_crows(self,stock,now_date):
        # talib.CDL3BLACKCROWS
    
        # 三只乌鸦说明来自百度百科
        # 1. 连续出现三根阴线，每天的收盘价均低于上一日的收盘
        # 2. 三根阴线前一天的市场趋势应该为上涨
        # 3. 三根阴线必须为长的黑色实体，且长度应该大致相等
        # 4. 收盘价接近每日的最低价位
        # 5. 每日的开盘价都在上根K线的实体部分之内；
        # 6. 第一根阴线的实体部分，最好低于上日的最高价位
        #
        # 算法
        # 有效三只乌鸦描述众说纷纭，这里放宽条件，只考虑1和2
        # 根据前4日数据判断
        # 3根阴线跌幅超过4.5%（此条件忽略）
        log.info("=>盘前判断三黑鸭状态")
        dailybars=self.strategy.get_last_n_dailybars(stock, 4,self.pre_days(now_date, -1))
        h_close=[]
        h_open=[]
        for dailybar in dailybars:
            h_close.append(dailybar.close)
            h_open.append(dailybar.open)
        if len(h_close) < 4 or len(h_open) < 4:
            return False
    
        # 一阳三阴
        if h_close[3] > h_open[3] \
                and (h_close[0] < h_open[0] and h_close[1] < h_open[1] and h_close[2] < h_open[2]):
            # and (h_close[-1] < h_close[-2] and h_close[-2] < h_close[-3]) \
            # and h_close[-1] / h_close[-4] - 1 < -0.045:
            return True
        return False
    
    
    def stop_loss_by_index_l(self,now_date):
        '''
                        二八止损
        minute
        '''
        if self.g.param['is_market_stop_loss_by_28_index'][self.g.VALUE]:
            count = 120
            # 回看指数前20天的涨幅
            gr_index_l = self.get_growth_rate(self.g.param['index_l'][self.g.VALUE],20,now_date)
            gr_index_s = self.get_growth_rate(self.g.param['index_s'][self.g.VALUE],20,now_date)
            if gr_index_l <= self.g.param['index_growth_rate'][self.g.VALUE] and gr_index_s <= self.g.param['index_growth_rate'][self.g.VALUE]:
                log.info("=>开始执行二八指数止损")
                if (self.g.cache['minute_count_index_ls_drop'] == 0):
                    log.info("当前二八指数的20日涨幅同时低于[%.2f%%], %s指数: [%.2f%%], %s指数: [%.2f%%]"
                             % (self.g.param['index_growth_rate'][self.g.VALUE] * 100, self.g.param['index_l'][self.g.VALUE], gr_index_l * 100, self.g.param['index_s'][self.g.VALUE], gr_index_s * 100))
                self.g.cache['minute_count_index_ls_drop'] += 1
            else:
                # 不连续状态归零
                if self.g.cache['minute_count_index_ls_drop'] < count:
                    self.g.cache['minute_count_index_ls_drop'] = 0
        
            if self.g.cache['minute_count_index_ls_drop'] >= count:
                if self.g.cache['minute_count_index_ls_drop'] == count:
                    log.info("==> 当日%s指数和%s指数的20日增幅低于[%.2f%%]已超过%d分钟，执行28指数止损"
                             % (self.g.param['index_l'][self.g.VALUE], self.g.param['index_s'][self.g.VALUE], self.g.param['index_growth_rate'][self.g.VALUE] * 100, count))
                self.position_clear()
                self.g.cache['day_count'] = 0
                self.g.cache['stop_trade'] = True  # 暂停当天交易
                return True
            return False
        return False
    
    
    def stop_loss_by_stock(self,now_date):
        '''
                        个股止损
        minute 级别
        '''
        if self.g.param['is_stock_stop_loss'][self.g.VALUE]:
            log.info("=>开始进行个股止损")
            for position in self.strategy.get_positions():
                symbol=position.exchange+'.'+position.sec_id 
                cur_price = self.strategy.get_last_n_bars(symbol,60,1,self.pre_minute(now_date, -1))[0].close
                if self.g.cache['last_high'][symbol] < cur_price:
                    self.g.cache['last_high'][symbol] = cur_price
                threshold = self._get_stop_loss_threshold(symbol, self.g.param['period'][self.g.VALUE],now_date)
                # log.debug("个股止损阈值, stock: %s, threshold: %f" %(stock, threshold))
                if cur_price < self.g.cache['last_high'][symbol] * (1 - threshold):
                    log.info("==> 个股止损, stock: %s, cur_price: %f, last_high: %f, threshold: %f"
                             % (symbol, cur_price, self.g.cache['last_high'][symbol], threshold))
                    if self.position_close(position):
                        self.g.cache['day_count'] = 0
    
    
    def stop_profit_by_stock(self,now_date):
        '''
                        个股止盈
        minute 级别
        '''
        if self.g.param['is_stock_stop_profit'][self.g.VALUE]:
            log.info("=>开始进行个股止盈")
            for position in self.strategy.get_positions():
                symbol=position.exchange+'.'+position.sec_id 
                cur_price = self.strategy.get_last_n_bars(symbol,60,1,self.pre_minute(now_date, -1))[0].close
                threshold = self._get_stop_profit_threshold(
                    symbol, self.g.param['period'][self.g.VALUE],now_date)
                # log.debug("个股止盈阈值, stock: %s, threshold: %f" %(stock, threshold))
                if cur_price > position.avg_cost * (1 + threshold):
                    log.info("==> 个股止盈, stock: %s, cur_price: %f, avg_cost: %f, threshold: %f"
                             % (symbol, cur_price, self.g.cache['last_high'][symbol], threshold))
                    if self.position_close(position):
                        self.g.cache['day_count'] = 0
    
    
    def _get_stop_loss_threshold(self,security, n,now_date):
        '''
                        计算个股回撤止损阈值
                        即个股在持仓n天内能承受的最大跌幅
                        算法：(个股250天内最大的n日跌幅 + 个股250天内平均的n日跌幅)/2
                        返回正值
        '''
        pct_change = self._get_pct_change(security, 250, n,now_date)
        # log.debug("pct of security [%s]: %s", pct)
        maxd = pct_change.min()
        # maxd = pct[pct<0].min()
        avgd = pct_change.mean()
        # avgd = pct[pct<0].mean()
        # maxd和avgd可能为正，表示这段时间内一直在增长，比如新股
        bstd = (maxd + avgd) / 2
    
        # 数据不足时，计算的bstd为nan
        if not math.isnan(bstd):
            if bstd != 0:
                return abs(bstd)
            else:
                # bstd = 0，则 maxd <= 0
                if maxd < 0:
                    # 此时取最大跌幅
                    return abs(maxd)
    
        return 0.099  # 默认配置回测止损阈值最大跌幅为-9.9%，阈值高貌似回撤降低
    
    
    def _get_stop_profit_threshold(self,security, n=3):
        '''
        计算个股止盈阈值
        算法：个股250天内最大的n日涨幅
        返回正值
        '''
        pct_change = self._get_pct_change(security, 250, n)
        maxr = pct_change.max()
    
        # 数据不足时，计算的maxr为nan
        # 理论上maxr可能为负
        if (not math.isnan(maxr)) and maxr != 0:
            return abs(maxr)
        return 0.30  # 默认配置止盈阈值最大涨幅为30%
    
    
    def _get_pct_change(self,symbol, n, m,now_date):
        '''
                        获取个股前n天的m日增幅值序列
                        增加缓存避免当日多次获取数据
        '''
        pct_change = None
        if symbol in self.g.cache['pct_change'].keys():
            pct_change = self.g.cache['pct_change'][symbol]
        else:
            dailybars=self.strategy.get_last_n_dailybars(symbol,n,self.pre_days(now_date, -1))
            #dailybars=self.reversed_list(dailybars, n)
            close_price=[]
            keys=[]
            for dailybar in dailybars:
                keys.append(dailybar.strtime)
                close_price.append(dailybar.close)
            df = pd.DataFrame(close_price, index=keys)
            df.columns = ['close']
            pct_change = df['close'].pct_change(m)  # 3日的百分比变比（即3日涨跌幅）
            self.g.cache['pct_change'][symbol] = pct_change
        return pct_change
    
    
    #### trade ####
    
    
    def position_open(self,stock, amout,now_date):
        '''
                        开仓，买入指定价值的证券
                        报单成功并成交（包括全部成交或部分成交，此时成交量大于0），返回True
                        报单失败或者报单成功但被取消（此时成交量等于0），返回False
        '''
        log.info("==>买入股票%s"%stock)
        tick=self.strategy.get_last_n_bars(stock,60,1,self.pre_minute(now_date, -1))
        #tick=self.strategy.get_last_n_ticks(stock,1,now_date)
        cur_price =tick[0].close
        value=int(amout/cur_price);
        value=int(value/100)
        value=int(value*100)
        openId=stock.split(".")
        order=self.strategy.open_long_sync(openId[0],openId[1],0,value)
        #order = self._order_target_value(security, value)
        #if order != None and order.filled > 0:
        # 报单成功并有成交则初始化最高价
        # cur_price = order.price
        self.g.cache['last_high'][stock] = cur_price
        return True
        #return False
    
    
    def position_close(self,position):
        '''
                        平仓，卖出指定持仓
                        平仓成功并全部成交，返回True
                        报单失败或者报单成功但被取消（此时成交量等于0），或者报单非全部成交，返回False
        '''
        # order = _order_target_value(security, 0)  # 可能会因停牌失败
        log.info("==>卖出股票%s"%position.sec_id)
        order=self.strategy.close_long_sync(position.exchange,position.sec_id,0,position.volume)  # 卖出可用仓位
        #if order.status> 0:
            # 只要有成交，无论全部成交还是部分成交，则统计盈亏
            #self.g.trade_stat.watch(security, order.filled,
            #                  position.avg_cost, position.price)
>>>>>>> branch 'master' of https://github.com/dujinxin001/join.git


#来自最大回撤抄底发
def filter_by_chaodie(stock_list, context, data):
    log.info("=>开始超跌过滤%s" %stock_list)
    fall_price=[]
    for stock in stock_list:
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
    log.info("<=结束超跌过滤%s" %buy_list)
    return buy_list


##来自基金B二版
def delect_stock(stock_list, context, data):
    log.info("=>delect_stock%s" %stock_list)
    hour = context.current_dt.hour
    minute=context.current_dt.minute
    if hour>=13:
        hour=hour-1.5
    ti=(hour-9)*60+(minute-30)
    stock_list2=stock_list
    for stock in stock_list:
        count=30
        current_price=data[stock].close
        ma5=data[stock].mavg(5,'close')
        hiday = attribute_history(stock, 1, '1d', ['close','volume'],df=False)
        if current_price<ma5 or((current_price-hiday['close'][0])/hiday['close'][0])*100>9 \
            or ((current_price-hiday['close'][0])/hiday['close'][0])*100<-2 :
            stock_list2.remove(stock)
            continue
        #为了节省时间把上面这段先判断，过滤掉一半。
        hi=attribute_history(stock, ti, '1m', fields=('close', 'high', 'volume', 'money'),df=False)
        maxp=max(hi['high'])
        if ((maxp-current_price)/current_price)*100>1 \
            or sum(hi['volume'])<0.5*hiday['volume'] \
            or 100*(current_price-sum(hi['money'])/sum(hi['volume']))/hiday['close']>2:
            stock_list2.remove(stock)
            continue
        while count<ti:
            mp=sum(hi['money'][:count])/sum(hi['volume'][:count])
            if hi['close'][count]<mp :
                stock_list2.remove(stock)
                break
            count+=1
    log.info("=>结束delect_stock%s" %stock_list2)
    return stock_list2



#指数macd择时
# 以minute为单位取数据为了信号及时有效，否则以day取数据，有滞后
def filter_market_time(stock_list, context, data):
    #先计算周线
    log.info("==> 判断MACD止损")
    index = g.param['index_price'][g.VALUE]
    hData = attribute_history(index, 130, unit='1200m', fields=('close'), skip_paused=True)
    close = hData['close']
    close = np.array(close, dtype='f8')
    
    wDif, wDea, wMacd =  MACD_CN(close, 12, 26, 9)
    
    if wMacd[-1] <= 0:
        stock_list=[]
    else:
        hData = attribute_history(index, 130, unit='240m', fields=('close'), skip_paused=True)
        close = hData['close']
        close = np.array(close, dtype='f8')
        dDif, dDea, dMacd =  MACD_CN(close, 12, 26, 9)
        if dMacd[-1] <= 0:
            stock_list=[]
    log.info("==> 结束MACD止损")
    return stock_list

#### stop loss ####

def stop_loss_by_price(context, data):
    '''
    大盘指数前130日内最高价超过最低价2倍，则清仓止损
    基于历史数据判定，因此若状态满足，则当天都不会变化
    增加此止损，回撤降低，收益降低
    '''
    if g.param['is_market_stop_loss_by_price'][g.VALUE]:
        index = g.param['index_price'][g.VALUE]
        if not g.cache['is_day_stop_loss_by_price']:
            h = attribute_history(index, 160, unit='1d', fields=('close', 'high', 'low'), skip_paused=True)
            low_price = h.low.min()
            high_price = h.high.max()
            # if high_price > 2 * low_price:
            if high_price > 2.2 * low_price \
                    and h['close'][-1] < h['close'][-4] \
                    and h['close'][-1] > h['close'][-100]:
                # 当日第一次输出日志
                log.info("==> 大盘止损，%s指数前130日内最高价超过最低价2倍, 最高价: %f, 最低价: %f" % (
                    get_security_info(index).display_name, high_price, low_price))
                g.cache['is_day_stop_loss_by_price'] = True
    
        if g.cache['is_day_stop_loss_by_price']:
            position_clear(context)
            g.cache['day_count'] = 0
            g.cache['stop_trade'] = True  # 暂停当天交易

    return g.cache['is_day_stop_loss_by_price']


def stop_loss_by_3_crows(context, data):
    '''
    前日三黑鸦，累计当日大盘指数涨幅<0的分钟计数
    如果分钟计数超过值n，则开始进行三黑鸦止损
    避免无效三黑鸦乱止损
    minute
    '''
    # 配置三黑鸦判定指数，默认为上证指数，可修改为其他指数
    index = g.param['index_3_crows'][g.VALUE]
    # 配置是否开启大盘三黑鸦止损
    # 个人认为针对大盘判断三黑鸦效果并不好，首先有效三只乌鸦难以判断，准确率实际来看也不好，
    # 其次，分析历史行情看一般大盘出现三只乌鸦的时候，已经严重滞后了，使用其他止损方式可能会更好
    n = 60
    if g.param['is_market_stop_loss_by_3_black_crows'][g.VALUE]: 
        if g.cache['is_last_day_3_crows']:
            if get_growth_rate(index, 1) < 0:
                g.cache['minute_count_cur_drop'] += 1
            if g.cache['minute_count_cur_drop'] >= n:
                if g.cache['minute_count_cur_drop'] == n:
                    log.info("==> 当日%s增幅 < 0 已超过%d分钟，执行三黑鸦止损" %
                             (get_security_info(index).display_name, n))
    
                position_clear(context)
                g.cache['day_count'] = 0
                g.cache['stop_trade'] = True  # 暂停当天交易
                return True
        return False
    return False


def _is_3_crows(stock):
    # talib.CDL3BLACKCROWS

    # 三只乌鸦说明来自百度百科
    # 1. 连续出现三根阴线，每天的收盘价均低于上一日的收盘
    # 2. 三根阴线前一天的市场趋势应该为上涨
    # 3. 三根阴线必须为长的黑色实体，且长度应该大致相等
    # 4. 收盘价接近每日的最低价位
    # 5. 每日的开盘价都在上根K线的实体部分之内；
    # 6. 第一根阴线的实体部分，最好低于上日的最高价位
    #
    # 算法
    # 有效三只乌鸦描述众说纷纭，这里放宽条件，只考虑1和2
    # 根据前4日数据判断
    # 3根阴线跌幅超过4.5%（此条件忽略）
    log.info("=>盘前判断三黑鸭状态")
    h = attribute_history(stock, 4, '1d', ('close', 'open'),
                          skip_paused=True, df=False)
    h_close = list(h['close'])
    h_open = list(h['open'])

    if len(h_close) < 4 or len(h_open) < 4:
        return False

    # 一阳三阴
    if h_close[-4] > h_open[-4] \
            and (h_close[-1] < h_open[-1] and h_close[-2] < h_open[-2] and h_close[-3] < h_open[-3]):
        # and (h_close[-1] < h_close[-2] and h_close[-2] < h_close[-3]) \
        # and h_close[-1] / h_close[-4] - 1 < -0.045:
        return True
    return False


def stop_loss_by_index_l(context, data):
    '''

    二八止损
    minute
    '''
    if g.param['is_market_stop_loss_by_28_index'][g.VALUE]:
        count = 120
        # 回看指数前20天的涨幅
        gr_index_l = get_growth_rate(g.param['index_l'][g.VALUE])
        gr_index_s = get_growth_rate(g.param['index_s'][g.VALUE])
    
        if gr_index_l <= g.param['index_growth_rate'][g.VALUE] and gr_index_s <= g.param['index_growth_rate'][g.VALUE]:
            log.info("=>开始执行二八指数止损")
            if (g.cache['minute_count_index_ls_drop'] == 0):
                log.info("当前二八指数的20日涨幅同时低于[%.2f%%], %s指数: [%.2f%%], %s指数: [%.2f%%]"
                         % (g.param['index_growth_rate'][g.VALUE] * 100, get_security_info(g.param['index_l'][g.VALUE]).display_name, gr_index_l * 100, get_security_info(g.param['index_s'][g.VALUE]).display_name, gr_index_s * 100))
    
                # log.info("当前%s指数的20日涨幅 [%.2f%%]" %(get_security_info(g.param['index_l'][g.VALUE]).display_name, gr_index_l*100))
                # log.info("当前%s指数的20日涨幅 [%.2f%%]"
                # %(get_security_info(g.param['index_s'][g.VALUE]).display_name,
                # gr_index_s*100))
            g.cache['minute_count_index_ls_drop'] += 1
        else:
            # 不连续状态归零
            if g.cache['minute_count_index_ls_drop'] < count:
                g.cache['minute_count_index_ls_drop'] = 0
    
        if g.cache['minute_count_index_ls_drop'] >= count:
            if g.cache['minute_count_index_ls_drop'] == count:
                log.info("==> 当日%s指数和%s指数的20日增幅低于[%.2f%%]已超过%d分钟，执行28指数止损"
                         % (get_security_info(g.param['index_l'][g.VALUE]).display_name, get_security_info(g.param['index_s'][g.VALUE]).display_name, g.param['index_growth_rate'][g.VALUE] * 100, count))
    
            position_clear(context)
            g.cache['day_count'] = 0
            g.cache['stop_trade'] = True  # 暂停当天交易
            return True
    
        return False
    return False


def stop_loss_by_stock(context, data):
    '''
    个股止损
    minute 级别
    '''
    if g.param['is_stock_stop_loss'][g.VALUE]:
        log.info("=>开始进行个股止损")
        for stock in context.portfolio.positions.keys():
            cur_price = data[stock].close
    
            if g.cache['last_high'][stock] < cur_price:
                g.cache['last_high'][stock] = cur_price
    
            threshold = _get_stop_loss_threshold(stock, g.param['period'][g.VALUE])
            # log.debug("个股止损阈值, stock: %s, threshold: %f" %(stock, threshold))
            if cur_price < g.cache['last_high'][stock] * (1 - threshold):
                log.info("==> 个股止损, stock: %s, cur_price: %f, last_high: %f, threshold: %f"
                         % (stock, cur_price, g.cache['last_high'][stock], threshold))
    
                position = context.portfolio.positions[stock]
                if position_close(position):
                    g.cache['day_count'] = 0


def stop_profit_by_stock(context, data):
    '''
    个股止盈
    minute 级别
    '''
    if g.param['is_stock_stop_profit'][g.VALUE]:
        log.info("=>开始进行个股止盈")
        for stock in context.portfolio.positions.keys():
            position = context.portfolio.positions[stock]
            cur_price = data[stock].close
            threshold = _get_stop_profit_threshold(
                stock, g.param['period'][g.VALUE])
            # log.debug("个股止盈阈值, stock: %s, threshold: %f" %(stock, threshold))
            if cur_price > position.avg_cost * (1 + threshold):
                log.info("==> 个股止盈, stock: %s, cur_price: %f, avg_cost: %f, threshold: %f"
                         % (stock, cur_price, g.cache['last_high'][stock], threshold))
    
                position = context.portfolio.positions[stock]
                if position_close(position):
                    g.cache['day_count'] = 0


def _get_stop_loss_threshold(security, n=3):
    '''
    计算个股回撤止损阈值
    即个股在持仓n天内能承受的最大跌幅
    算法：(个股250天内最大的n日跌幅 + 个股250天内平均的n日跌幅)/2
    返回正值
    '''
    pct_change = _get_pct_change(security, 250, n)
    # log.debug("pct of security [%s]: %s", pct)
    maxd = pct_change.min()
    # maxd = pct[pct<0].min()
    avgd = pct_change.mean()
    # avgd = pct[pct<0].mean()
    # maxd和avgd可能为正，表示这段时间内一直在增长，比如新股
    bstd = (maxd + avgd) / 2

    # 数据不足时，计算的bstd为nan
    if not isnan(bstd):
        if bstd != 0:
            return abs(bstd)
        else:
            # bstd = 0，则 maxd <= 0
            if maxd < 0:
                # 此时取最大跌幅
                return abs(maxd)

    return 0.099  # 默认配置回测止损阈值最大跌幅为-9.9%，阈值高貌似回撤降低


def _get_stop_profit_threshold(security, n=3):
    '''
    计算个股止盈阈值
    算法：个股250天内最大的n日涨幅
    返回正值
    '''
    pct_change = _get_pct_change(security, 250, n)
    maxr = pct_change.max()

    # 数据不足时，计算的maxr为nan
    # 理论上maxr可能为负
    if (not isnan(maxr)) and maxr != 0:
        return abs(maxr)
    return 0.30  # 默认配置止盈阈值最大涨幅为30%


def _get_pct_change(security, n, m):
    '''
    获取个股前n天的m日增幅值序列
    增加缓存避免当日多次获取数据
    '''
    pct_change = None
    if security in g.cache['pct_change'].keys():
        pct_change =g.cache['pct_change'][security]
    else:
        h = attribute_history(security, n, unit='1d',
                              fields=('close'), skip_paused=True)
        pct_change = h['close'].pct_change(m)  # 3日的百分比变比（即3日涨跌幅）
        g.cache['pct_change'][security] = pct_change
    return pct_change


#### trade ####


def position_open(security, value):
    '''
    开仓，买入指定价值的证券
    报单成功并成交（包括全部成交或部分成交，此时成交量大于0），返回True
    报单失败或者报单成功但被取消（此时成交量等于0），返回False
    '''
    order = _order_target_value(security, value)
    if order != None and order.filled > 0:
        # 报单成功并有成交则连接实盘并初始化最高价
        g._executor.execute(order,g.param['buy_stock_count'][g.VALUE])
        cur_price = get_close_price(security, 1, '1m')
        # cur_price = order.price
        g.cache['last_high'][security] = cur_price
        return True
    return False


def position_close(position):
    '''
    平仓，卖出指定持仓
    平仓成功并全部成交，返回True
    报单失败或者报单成功但被取消（此时成交量等于0），或者报单非全部成交，返回False
    '''
    security = position.security
    # order = _order_target_value(security, 0)  # 可能会因停牌失败
    order = _order(security, -position.closeable_amount)  # 卖出可用仓位
    if order:
        if order.filled > 0:
            # 只要有成交，无论全部成交还是部分成交，则统计盈亏
            g.trade_stat.watch(security, order.filled,
                               position.avg_cost, position.price)

        if order.status == OrderStatus.held:
            # 全部成交则连接实盘并删除相关证券的最高价缓存
            g._executor.execute(order,g.param['buy_stock_count'][g.VALUE])
            if security in g.cache['last_high']:
                g.cache['last_high'].pop(security)
            else:
                log.warn("last high price of %s not found" % (security))
            return True
    return False


def position_clear(context):
    '''
    清空卖出所有持仓
    '''
    if context.portfolio.positions:
        log.info("==> 清仓，卖出所有股票")
        for stock in context.portfolio.positions.keys():
            position = context.portfolio.positions[stock]
            position_close(position)


def position_adjust(context, buy_stocks):
    '''
    根据待买股票创建或调整仓位
    对于因停牌等原因没有卖出的股票则继续持有
    始终保持持仓数目为g.buy_stock_count
    '''
    for stock in context.portfolio.positions.keys():
        if stock not in buy_stocks:
            log.info("stock [%s] in position is not buyable" % (stock))
            position = context.portfolio.positions[stock]
            position_close(position)
        else:
            log.info("stock [%s] is already in position" % (stock))

    # 根据股票数量分仓
    # 此处只根据可用金额平均分配购买，不能保证每个仓位平均分配
    position_count = len(context.portfolio.positions)
    count = g.param['buy_stock_count'][g.VALUE]
    if len(buy_stocks) > position_count:
        value = context.portfolio.cash / (len(buy_stocks) - position_count)
        for stock in buy_stocks:
            if context.portfolio.positions[stock].total_amount == 0:
                if position_open(stock, value):
                    if len(context.portfolio.positions) == count:
                        break


def _order(security, amount):
    '''
    自定义下单
    根据Joinquant文档，当前报单函数都是阻塞执行，报单函数（如order_target）返回即表示报单完成
    报单成功返回报单（不代表一定会成交），否则返回None
    '''
    if amount < 0:
        log.info("Selling out %s : %f" % (security, -amount))
    elif amount > 0:
        log.info("Buying in %s : %f" % (security, amount))

    # 如果股票停牌，创建报单会失败，order_target_value 返回None
    # 如果股票涨跌停，创建报单会成功，order_target_value 返回Order，但是报单会取消
    # 部成部撤的报单，聚宽状态是已撤，此时成交量>0，可通过成交量判断是否有成交
    return order(security, amount)


def _order_target_value(security, value):
    '''
    自定义下单
    根据Joinquant文档，当前报单函数都是阻塞执行，报单函数（如order_target_value）返回即表示报单完成
    报单成功返回报单（不代表一定会成交），否则返回None
    '''
    if value == 0:
        log.info("Selling out %s" % (security))
    else:
        log.info("Order %s to value %f" % (security, value))

    # 如果股票停牌，创建报单会失败，order_target_value 返回None
    # 如果股票涨跌停，创建报单会成功，order_target_value 返回Order，但是报单会取消
    # 部成部撤的报单，聚宽状态是已撤，此时成交量>0，可通过成交量判断是否有成交
    return order_target_value(security, value)

#### utils ####


def get_growth_rate(security, n=20):
    '''
    获取股票n日以来涨幅，根据当前价计算
    '''
    lc = get_close_price(security, n)
    c = get_close_price(security, 1, '1m')

    if not isnan(lc) and not isnan(c) and lc != 0:
        return (c - lc) / lc
    else:
        log.error("数据非法, security: %s, %d日收盘价: %f, 当前价: %f" %
                  (security, n, lc, c))
        return 0


def get_close_price(security, n, unit='1d'):
    '''
    获取前n个单位时间当时的收盘价
    '''
    close = 0
    while(n > 0):  # 如果前n日数据为nan，则取n-1日数据，直至n为1
        close = attribute_history(security, n, unit, ('close'))['close'][0]
        if isnan(close):
            n -= 1
        else:
            break
    return close


def func_register(register, func, descr=''):
    register.append((func, descr))

def remove_func_register(register, func, descr=''):
    obj=(func, descr)
    if obj in register:
        register.remove(obj)

#### log ####


def log_section(name, descr, sep=True, sep_char='-', sep_count=50):
    if sep:
        log.info(sep_char * sep_count)
    log.info('%s - %s' % (name, descr))


def log_param():
    log_section('param', '策略参数')
    for key in g.param:
        log.info('%s : %s' %
                 (g.param[key][g.DESCR], str(g.param[key][g.VALUE])))


def log_filter():
    log.info('filter - 过滤器:')
    for filter in g.filter:
        log.info(' + ' + filter[g.DESCR])


def log_stop_loss():
    log.info('stop_loss - 止损器:')
    for stop in g.stop_loss_minute:
        log.info(' + ' + stop[g.DESCR])
    for stop in g.stop_loss_day:
        log.info(' + ' + stop[g.DESCR])
        
###macd###
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


#计算macd 参考了软猫克鲁大神的帖子
def MACD_CN(close, fastperiod, slowperiod, signalperiod) :
    macdDIFF, macdDEA, macd = tl.MACDEXT(close, fastperiod=fastperiod, fastmatype=1, slowperiod=slowperiod, slowmatype=1, signalperiod=signalperiod, signalmatype=1)
    macd = macd * 2
    return macdDIFF, macdDEA, macd

