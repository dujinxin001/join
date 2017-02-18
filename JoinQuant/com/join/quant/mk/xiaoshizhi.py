from collections import OrderedDict
from blacklist import *
import math
import tradestat
import datetime
import talib as tl
import numpy as np
import pandas as pd
import copy


def init(context):
    log_section('initialize', '初始化', sep=False)

    # 设置系统参数
    set_sys(context)
    # 设置常数
    set_const(context)
    # 设置策略参数
    set_param(context)
    # 设置过滤器
    set_filter(context)
    # 设置止损器
    set_stop_loss(context)
    # 设置缓存参数
    set_cache(context)
    # 加载统计模块
    context.trade_stat = tradestat.trade_stat()

    # 打印自定义信息
    # log.info('')
    # 打印策略参数
    log_param(context)
    # 打印使用的过滤器
    log_filter(context)
    # 打印使用的止损器
    log_stop_loss(context)
    
    #收盘后执行函数
    #scheduler.run_daily(after_trading_end, time_rule=market_close(hour=0, minute=0))
    #
    #scheduler.run_daily(position_adjust, time_rule=market_open(hour=0, minute=231))


def process_initialize(context):
    log_section('process_initialize','进程重新启动')


def after_code_changed(context):
    log_section('after_code_changed','代码发生修改')


def before_trading(context):
    log_section('before_trading', '盘前处理:又开始赚钱了')

    reset_day_param(context)
    # 盘前就判断三黑鸦状态，因为判断的数据为前4日
    context.cache['is_last_day_3_crows'] = _is_3_crows(
        context.param['index_3_crows'][context.VALUE])
    if context.cache['is_last_day_3_crows']:
        logger.info("==> 前4日已经构成三黑鸦形态")


def after_trading(context):
    log_section('after_trading', '盘后处理：大赚收钱')
    # 得到当前未完成订单
    orders = get_open_orders()
    for order_id in orders:
        logger.info("本次订单列表: %s" % (order_id))
        logger.info("执行统计")
        order=get_order(order_id)
        if order.status==ORDER_STATUS.FILLED:
            context.trade_stat.watch(order.order_book_id, order.filled_quantity,
                           order.avg_price,  history_bars(order.order_book_id,1, '1m', 'close'))
    context.trade_stat.report(context)

def handle_bar(context, bar_dict):
    '''
    按分钟回测
    '''
    # for key in g.stop_loss_minute:
    #     g.stop_loss_minute[key][g.FUNC](context, data)
    hour = context.now.hour
    minute = context.now.minute
    #进行所有止损器判断，执行止损
    for stop in context.stop_loss_minute:
        if hour==9 and minute==31:
            logger.info("执行止损检查")
        stop[context.FUNC](context, bar_dict)

    # 每天指定时间检查是否调仓并处理
    adjust_position_time = context.param['adjust_position_time'][context.VALUE]
    if hour == adjust_position_time[context.HOUR] and minute == adjust_position_time[context.MINUTE] and not context.cache['stop_trade']:
        _adjust_position(context, bar_dict)
    if len(context.cache['sell_failed_stocks']) and context.cache['stop_trade']:
        position_clear(context,bar_dict)
    if hour == adjust_position_time[context.HOUR] and minute > adjust_position_time[context.MINUTE] and not context.cache['stop_trade']:
        if (context.cache['day_count']-1) % context.param['period'][context.VALUE] == 0:
            logger.info('循环')
            position_adjust2(context,bar_dict)
    

def _adjust_position(context, bar_dict):
    logger.info("调仓日计数 [%d]" % (context.cache['day_count']))

    # 回看指数前20天的涨幅
    gr_index_l = get_growth_rate(context.param['index_l'][context.VALUE])
    gr_index_s = get_growth_rate(context.param['index_s'][context.VALUE])
    logger.info("当前%s指数的20日涨幅 [%.2f%%]" % (
        instruments(context.param['index_l'][context.VALUE]).symbol, gr_index_l * 100))
    logger.info("当前%s指数的20日涨幅 [%.2f%%]" % (
        instruments(context.param['index_s'][context.VALUE]).symbol, gr_index_s * 100))

    if gr_index_l <= context.param['index_growth_rate'][context.VALUE] and gr_index_s <= context.param['index_growth_rate'][context.VALUE]:
        position_clear(context,bar_dict)
        context.cache['day_count'] = 0
        context.cache['stop_trade']=True
    else:
        if context.cache['day_count'] % context.param['period'][context.VALUE] == 0:
            logger.info("==> 满足条件进行调仓")
            buy_stocks = pick_stocks(context, bar_dict)
            context.cache['buy_stocks']=list(buy_stocks)
            if len(context.cache['buy_stocks'])==0:
                logger.info("选股后无买股票: %s" %(context.cache['buy_stocks']))
                context.cache['day_count']=0
                return
            logger.info("选股后可买股票: %s" % (context.cache['buy_stocks']))
            position_adjust2(context,bar_dict)
        context.cache['day_count'] += 1


# 按月运行
# run_monthly(func, monthday, time='open', reference_security)
# 按周运行
# run_weekly(func, weekday, time='open', reference_security)
# 每天内何时运行
# run_daily(func, time='open', reference_security)


#### initialize ####

# 设置系统参数
def set_sys(context):
    # 设定沪深300指数作为基准
    #context.benchmark ='000300.XSHG'
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    # context.commission = 0.03
    # 设定滑点为百分比
    #context.slippage=0.4
    pass


# 设置常数
def set_const(context):
    context.VALUE = 0
    context.FUNC = 0
    context.DESCR = 1
    context.HOUR = 0
    context.MINUTE = 1

# 设置策略参数
def set_param(context):
    p = {}

    p['period'] = (3, '调仓频率，单位：日')
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
    p['index_s']=('399678.XSHG','小盘股指数') # 中证500指数
    p['buy_stock_count'] = (2, '买入股票数目')
    p['index_growth_rate'] = (0.005, '判定调仓的二八指数n日增幅')  # n = 20
    p['index_3_crows'] = ('000300.XSHG', '判定三黑鸦的指数')
    p['index_price'] = ('000905.XSHG', '判定价格止损的指数')
    p['is_stock_stop_loss']=(False,'是否个股止损')
    p['is_stock_stop_profit']=(False,'是否个股止盈')
    p['is_market_stop_loss_by_price'] = (True,'是否根据大盘历史价格止损')
    p['is_market_stop_loss_by_28_index']=(False,'是否根据28指数值实时进行止损')
    p['is_market_stop_loss_by_3_black_crows']=(True,'是否进行三黑鸦止损')

    context.param = p

# 设置过滤器
def set_filter(context):
    # g.filter={}
    context.filter = []
    #func_register(g.filter, filter_market_time, '指数MACD过滤')
    func_register(context.filter, filter_by_query, '查询财务数据库过滤')
    func_register(context.filter, filter_gem, '过滤创业版股票')
    func_register(context.filter, filter_paused, '过滤停牌股票')
    func_register(context.filter, filter_st, '过滤ST及其他具有退市标签的股票')
    func_register(context.filter, filter_limitup, '过滤涨停的股票')
    func_register(context.filter, filter_limitdown, '过滤跌停的股票')
    func_register(context.filter,filter_blacklist, '过滤黑名单股票')
    #func_register(context.filter,filter_by_growth_rate, '过滤n日增长率为负的股票')
    func_register(context.filter,filter_new, '过滤新股')
    #func_register(context.filter,filter_by_chaodie, '超跌过滤器')
    #func_register(context.filter, delect_stock, 'delect_stock')
    #func_register(context.filter, filter_by_rank_0, '市值评分过滤器')
    func_register(context.filter, filter_by_rank, '评分过滤器')


def set_stop_loss(context):
    context.stop_loss_day = []
    context.stop_loss_minute = []
    
    func_register(context.stop_loss_minute, stop_loss_by_price, '大盘价格止损')
    func_register(context.stop_loss_minute, stop_loss_by_3_crows, '三黑鸦止损')
    func_register(context.stop_loss_minute,stop_loss_by_index_l,'二八止损')
    func_register(context.stop_loss_minute,stop_loss_by_stock,'个股止损')
    func_register(context.stop_loss_minute,stop_profit_by_stock,'个股止盈')


def set_cache(context):
    c = {}

    # 调仓日计数器，单位：日
    c['day_count'] = 0

    # 缓存股票持仓后的最高价
    c['last_high'] = {}

    df = get_fundamentals(query)
    c['stock_list'] = df.columns.values

    # 缓存当日个股250天内最大的3日涨幅，避免当日反复获取，每日盘后清空
    c['pct_change'] = {}
    # 缓存指数下跌分钟数
    c['minute_count_index_ls_drop'] = 0
    c['minute_count_cur_drop'] = 0
    c['is_last_day_3_crows'] = False
    c['is_day_stop_loss_by_price'] = False
    c['stop_trade'] = False  # 暂停当天交易
    c['buy_stocks']=[]    #缓存可买入股票
    c['selling_stocks']=[] #缓存已经下卖单股票
    c['buying_stocks']=[] #缓存已经下买单股票
    c['sell_failed_stocks']=[]#缓存卖出失败的股票
    context.cache = c


def reset_day_param(context):
    '''
    重置当日参数，仅针对需要当日需要重置的参数
    '''
    logger.info("=>盘后重置当日参数")
    df = get_fundamentals(query())
    context.cache['stock_list'] =df.columns.values
    # 重置当日大盘价格止损状态
    context.cache['is_day_stop_loss_by_price'] = False

    # 重置三黑鸦状态
    context.cache['is_last_day_3_crows'] = False
    context.cache['minute_count_cur_drop'] = 0
    context.cache['minute_count_index_ls_drop'] = 0

    # 清空当日个股250天内最大的3日涨幅的缓存
    context.cache['pct_change'].clear()

    context.cache['stop_trade'] = False  # 暂停当天交易
    context.cache['buy_stocks']=[]
    context.cache['selling_stocks']=[] #清空已经下卖单股票
    context.cache['buying_stocks']=[] #清空已经下买单股票
    context.cache['sell_failed_stocks']=[] #清空卖出失败的股票
    
    


#### pick & filter ####


def pick_stocks(context, data):
    '''
    选取指定数目的小市值股票，再进行过滤，最终挑选指定可买数目的股票
    '''
    stock_list = context.cache['stock_list']

    # for key in g.filter.keys():
    #     stock_list = g.filter[key][g.FUNC](stock_list, context, data)
    for filter in context.filter:
        stock_list = filter[context.FUNC](stock_list, context, data)
        if len(stock_list)==0:
           logger.info("股票被过滤没了")
           return stock_list

    # 选取指定可买数目的股票
    return stock_list[:context.param['buy_stock_count'][context.VALUE]]


def filter_by_query(stock_list, context, data):
    '''
    查询财务数据库过滤
    '''
    logger.info("=>开始执行财务条件过滤")
    pe_min = 0
    pe_max = 200
    eps_min = 0

    #stock_list=['300029.XSHE']
    q = query(fundamentals.eod_derivative_indicator.market_cap).filter(fundamentals.stockcode.in_(stock_list))
    if context.param['pick_by_pe'][context.VALUE]:
        q = q.filter(
            fundamentals.eod_derivative_indicator.pe_ratio > pe_min,
            fundamentals.eod_derivative_indicator.pe_ratio < pe_max
        )
    if context.param['pick_by_eps'][context.VALUE]:
        q = q.filter(fundamentals.financial_indicator.earnings_per_share > eps_min)

    df = get_fundamentals(
        q.order_by(fundamentals.eod_derivative_indicator.market_cap.asc()
                   ).limit(
            context.param['pick_stock_count'][context.VALUE]
        ))
    logger.info("=>结束执行财务条件过滤")
    return list(df.columns.values)


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
    logger.info("=>开始执行过滤停牌的股票")
    return [stock for stock in stock_list if not data[stock].suspended]


def filter_st(stock_list, context, data):
    '''
    过滤ST及其他具有退市标签的股票
    '''
    logger.info("=>开始执行过滤ST及其他具有退市标签的股票")
    return [stock for stock in stock_list
            if not is_st_stock(stock,1)
            and not data[stock].symbol.startswith('退') and not data[stock].symbol.startswith('*')]


def filter_gem(stock_list, context, data):
    '''
    过滤创业版股票
    '''
    if context.param['filter_gem'][context.VALUE]:
        logger.info("=>开始执行过滤创业板股票")
        return [stock for stock in stock_list if stock[0:3] != '300']
    return stock_list


def filter_limitup(stock_list, context, data):
    '''
    过滤涨停的股票
    '''
    logger.info("=>开始执行过滤涨停的股票")
    threshold = 1.00
    stock_list2=[]
    # 已存在于持仓的股票即使涨停也不过滤，避免此股票再次可买，但因被过滤而导致选择别的股票
    # return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
    #         or last_prices[stock][-1] < data[stock].high_limit * threshold]
    for stock in stock_list:
        if stock in context.portfolio.positions.keys() or round(data[stock].close,2) < round(data[stock].limit_up * threshold,2):
            stock_list2.append(stock)
    return stock_list2
            
    #return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
    #        or data[stock].last < data[stock].limit_up * threshold]


def filter_limitdown(stock_list, context, data):
    '''
    过滤跌停的股票
    '''
    logger.info("=>开始执行过滤跌停的股票")
    threshold = 1.00

    # return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
    #         or last_prices[stock][-1] > data[stock].low_limit * threshold]
    return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
            or data[stock].last > data[stock].limit_down * threshold]


def filter_by_growth_rate(stock_list, context, data):
    '''
    过滤n日增长率为负的股票
    '''
    n = 20
    return [stock for stock in stock_list if get_growth_rate(stock, n) > 0]


def filter_blacklist(stock_list, context, data):
    '''
    过滤黑名单股票
    '''
    if context.param['filter_blacklist'][context.VALUE]:
        logger.info("=>开始过滤黑名单的股票")
        blacklist = get_blacklist()
        return [stock for stock in stock_list if stock not in blacklist]
    return stock_list

def filter_new(stock_list, context, data):
    '''
    过滤新股
    '''
    if context.param['is_filter_new'][context.VALUE]:
        logger.info("=>开始过滤新股")
        delta = 60
        start_date = context.now.date() - datetime.timedelta(delta)
        return [stock for stock in stock_list if instruments(stock).listed_date < start_date]
    return stock_list


def filter_by_rank_0(stock_list, context, data):
    '''
    评分过滤器
    '''
    #logger.info("=>开始进行股票市值评分%s" %stock_list)
    if len(stock_list) > 0:
        dst_stocks = {}
        for stock in stock_list:
            q = query(valuation).filter(fundamentals.code == stock)
            df = get_fundamentals(q)
            #logger.info("股票%s的总市值%s"%(stock,df['market_cap'][0]))
    
            avg_5 = data[stock].mavg(5, field='money')
            #logger.info("股票%s的5日平均成交额%s"%(stock,avg_5))
            score = (2.2*math.log(df['market_cap'][0]))+(1.22*math.log(avg_5))
            # score = ((cur_price-low_price_130) + (cur_price-high_price_130) +
            # (cur_price-avg_15)) / cur_price
            dst_stocks[stock] = score
    
        df = pd.DataFrame(data=list(dst_stocks.values()), index=dst_stocks.keys())
        df.columns = ['score']
        df = df.sort(columns='score', ascending=True)
        logger.info("<=个股市值评分结束")
        return df.index


def filter_by_rank(stock_list, context, data):
    '''
    评分过滤器
    '''
    if context.param['is_rank_stock'][context.VALUE]: 
        logger.info("=>开始进行评分%s" %stock_list)
        if len(stock_list) >context.param['rank_stock_count'][context.VALUE]:
            stock_list = stock_list[:context.param['rank_stock_count'][context.VALUE]]
        if len(stock_list) > 0:
            dst_stocks = {}
            for stock in stock_list:
                h_low = history_bars(stock,130, '1d','low')
                h_high = history_bars(stock,130, '1d','high')
                h_close = history_bars(stock,130, '1d','close')
                low_price_130 = min(h_low)
                high_price_130 = max(h_high)
        
                avg_15 = h_close[-15:].mean()
                cur_price = data[stock].close
        
                #logger.info("=>%s130日最低价:%s"%(instruments(stock).symbol,low_price_130))
                #logger.info("=>%s130日最高价:%s"%(instruments(stock).symbol,high_price_130))
                #logger.info("=>%s15日平均价:%s"%(instruments(stock).symbol,avg_15))
                #logger.info("=>%s当前价:%s"%(instruments(stock).symbol,cur_price))
                # avg_15 = h['close'][-15:].mean()
                # cur_price = get_close_price(stock, 1, '1m')
        
                score = (cur_price - low_price_130)+(cur_price - high_price_130)+(cur_price - avg_15)
                # score = ((cur_price-low_price_130) + (cur_price-high_price_130) +
                # (cur_price-avg_15)) / cur_price
                #logger.info("=>%s评分结果:%s"%(instruments(stock).symbol,score))
                dst_stocks[stock] = score
        
            df = pd.DataFrame(data=list(dst_stocks.values()), index=dst_stocks.keys(),columns=['score'])
            df.columns = ['score']
            df = df.sort(columns='score', ascending=True)
            logger.info("<=个股评分结束")
            return df.index
            


#来自最大回撤抄底发
def filter_by_chaodie(stock_list, context, data):
    logger.info("=>开始超跌过滤%s" %stock_list)
    fall_price=[]
    for stock in stock_list:
        close_prices=history_bars(stock,70, '1d', 'close')[stock]
        fall_price.append((stock,close_prices.max()/close_prices[-1]-1))
    
    fall_price=sorted(fall_price,key=lambda item:item[1],reverse=True)
    stocks=fall_price[:200]
    
    buy_list=[]
    for stock in stocks :
        #value_money_fall=money_fall(stock[0],20,60,160)
        value_buy_macd=buy_macd(stock[0])
        if value_buy_macd>0:
            buy_list.append(stock[0])
    logger.info("<=结束超跌过滤%s" %buy_list)
    return buy_list


##来自基金B二版
def delect_stock(stock_list, context, data):
    return stock_list



#指数macd择时
# 以minute为单位取数据为了信号及时有效，否则以day取数据，有滞后
def filter_market_time(stock_list, context, data):
    #先计算周线
    logger.info("==> 判断MACD止损")
    index = context.param['index_price'][context.VALUE]
    hData = history_bars(index,130, '1200m', 'close')
    close = hData['close']
    close = np.array(close, dtype='f8')
    
    wDif, wDea, wMacd =  MACD_CN(close, 12, 26, 9)
    
    if wMacd[-1] <= 0:
        stock_list=[]
    else:
        hData = history_bars(index,130, '240m', 'close')
        close = hData['close']
        close = np.array(close, dtype='f8')
        dDif, dDea, dMacd =  MACD_CN(close, 12, 26, 9)
        if dMacd[-1] <= 0:
            stock_list=[]
    logger.info("==> 结束MACD止损")
    return stock_list

#### stop loss ####

def stop_loss_by_price(context, data):
    '''
    大盘指数前130日内最高价超过最低价2倍，则清仓止损
    基于历史数据判定，因此若状态满足，则当天都不会变化
    增加此止损，回撤降低，收益降低
    '''
    if context.param['is_market_stop_loss_by_price'][context.VALUE]:
        index = context.param['index_price'][context.VALUE]
        if not context.cache['is_day_stop_loss_by_price']:
            h_close = history_bars(index,160, '1d', 'close')
            h_low = history_bars(index,160, '1d', 'low')
            h_high = history_bars(index,160, '1d','high')
            #low_price = h_low.low.min()
            #high_price = h_high.high.max()
            low_price = min(h_low)
            high_price = max(h_high)
            # if high_price > 2 * low_price:
            if high_price > 2.2 * low_price \
                    and h_close[-1] < h_close[-4] \
                    and h_close[-1] > h_close[-100]:
                # 当日第一次输出日志
                logger.info("==> 大盘止损，%s指数前130日内最高价超过最低价2倍, 最高价: %f, 最低价: %f" % (
                    instruments(index).symbol, high_price, low_price))
                context.cache['is_day_stop_loss_by_price'] = True
    
        if context.cache['is_day_stop_loss_by_price']:
            position_clear(context,data)
            context.cache['day_count'] = 0
            context.cache['stop_trade'] = True  # 暂停当天交易

    return context.cache['is_day_stop_loss_by_price']


def stop_loss_by_3_crows(context, data):
    '''
    前日三黑鸦，累计当日大盘指数涨幅<0的分钟计数
    如果分钟计数超过值n，则开始进行三黑鸦止损
    避免无效三黑鸦乱止损
    minute
    '''
    # 配置三黑鸦判定指数，默认为上证指数，可修改为其他指数
    index = context.param['index_3_crows'][context.VALUE]
    # 配置是否开启大盘三黑鸦止损
    # 个人认为针对大盘判断三黑鸦效果并不好，首先有效三只乌鸦难以判断，准确率实际来看也不好，
    # 其次，分析历史行情看一般大盘出现三只乌鸦的时候，已经严重滞后了，使用其他止损方式可能会更好
    n = 60
    if context.param['is_market_stop_loss_by_3_black_crows'][context.VALUE]: 
        if context.cache['is_last_day_3_crows']:
            if get_growth_rate(index, 1) < 0:
                context.cache['minute_count_cur_drop'] += 1
            if context.cache['minute_count_cur_drop'] >= n:
                if context.cache['minute_count_cur_drop'] == n:
                    logger.info("==> 当日%s增幅 < 0 已超过%d分钟，执行三黑鸦止损" %
                             (instruments(index).symbol, n))
    
                position_clear(context,data)
                context.cache['day_count'] = 0
                context.cache['stop_trade'] = True  # 暂停当天交易
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
    logger.info("=>盘前判断三黑鸭状态")
    h_close=history_bars(stock,4,'1d','close')
    h_open=history_bars(stock,4,'1d','open')
    #h_close = list(h['close'])
    #h_open = list(h['open'])

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
    if context.param['is_market_stop_loss_by_28_index'][context.VALUE]:
        count = 120
        # 回看指数前20天的涨幅
        gr_index_l = get_growth_rate(context.param['index_l'][context.VALUE])
        gr_index_s = get_growth_rate(context.param['index_s'][context.VALUE])
    
        if gr_index_l <= context.param['index_growth_rate'][context.VALUE] and gr_index_s <= context.param['index_growth_rate'][context.VALUE]:
            logger.info("=>开始执行二八指数止损")
            if (context.cache['minute_count_index_ls_drop'] == 0):
                logger.info("当前二八指数的20日涨幅同时低于[%.2f%%], %s指数: [%.2f%%], %s指数: [%.2f%%]"
                         % (context.param['index_growth_rate'][context.VALUE] * 100, instrument(context.param['index_l'][context.VALUE]).symbol, gr_index_l * 100, instrument(g.param['index_s'][context.VALUE]).symbol, gr_index_s * 100))
    
                # log.info("当前%s指数的20日涨幅 [%.2f%%]" %(get_security_info(g.param['index_l'][g.VALUE]).display_name, gr_index_l*100))
                # log.info("当前%s指数的20日涨幅 [%.2f%%]"
                # %(get_security_info(g.param['index_s'][g.VALUE]).display_name,
                # gr_index_s*100))
            context.cache['minute_count_index_ls_drop'] += 1
        else:
            # 不连续状态归零
            if context.cache['minute_count_index_ls_drop'] < count:
                context.cache['minute_count_index_ls_drop'] = 0
    
        if context.cache['minute_count_index_ls_drop'] >= count:
            if context.cache['minute_count_index_ls_drop'] == count:
                logger.info("==> 当日%s指数和%s指数的20日增幅低于[%.2f%%]已超过%d分钟，执行28指数止损"
                         % (instrument(context.param['index_l'][context.VALUE]).symbol, instrument(g.param['index_s'][context.VALUE]).symbol, context.param['index_growth_rate'][context.VALUE] * 100, count))
    
            position_clear(context,data)
            context.cache['day_count'] = 0
            context.cache['stop_trade'] = True  # 暂停当天交易
            return True
    
        return False
    return False


def stop_loss_by_stock(context, data):
    '''
    个股止损
    minute 级别
    '''
    if context.param['is_stock_stop_loss'][context.VALUE]:
        logger.info("=>开始进行个股止损")
        for stock in context.portfolio.positions.keys():
            cur_price = data[stock].close
    
            if context.cache['last_high'][stock] < cur_price:
                context.cache['last_high'][stock] = cur_price
    
            threshold = _get_stop_loss_threshold(context,stock, context.param['period'][context.VALUE])
            # log.debug("个股止损阈值, stock: %s, threshold: %f" %(stock, threshold))
            if cur_price < context.cache['last_high'][stock] * (1 - threshold):
                logger.info("==> 个股止损, stock: %s, cur_price: %f, last_high: %f, threshold: %f"
                         % (stock, cur_price, g.cache['last_high'][stock], threshold))
    
                position = context.portfolio.positions[stock]
                if position_close(position):
                    context.cache['day_count'] = 0


def stop_profit_by_stock(context, data):
    '''
    个股止盈
    minute 级别
    '''
    if context.param['is_stock_stop_profit'][context.VALUE]:
        logger.info("=>开始进行个股止盈")
        for stock in context.portfolio.positions.keys():
            position = context.portfolio.positions[stock]
            cur_price = data[stock].close
            threshold = _get_stop_profit_threshold(
                stock, context.param['period'][context.VALUE])
            # log.debug("个股止盈阈值, stock: %s, threshold: %f" %(stock, threshold))
            if cur_price > position.avg_cost * (1 + threshold):
                logger.info("==> 个股止盈, stock: %s, cur_price: %f, avg_cost: %f, threshold: %f"
                         % (stock, cur_price, context.cache['last_high'][stock], threshold))
    
                position = context.portfolio.positions[stock]
                if position_close(position):
                    context.cache['day_count'] = 0


def _get_stop_loss_threshold(context,security, n=3):
    '''
    计算个股回撤止损阈值
    即个股在持仓n天内能承受的最大跌幅
    算法：(个股250天内最大的n日跌幅 + 个股250天内平均的n日跌幅)/2
    返回正值
    '''
    pct_change = _get_pct_change(context,security, 250, n)
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
    if (not math.isnan(maxr)) and maxr != 0:
        return abs(maxr)
    return 0.30  # 默认配置止盈阈值最大涨幅为30%


def _get_pct_change(context,security, n, m):
    '''
    获取个股前n天的m日增幅值序列
    增加缓存避免当日多次获取数据
    '''
    pct_change = None
    if security in context.cache['pct_change'].keys():
        pct_change =context.cache['pct_change'][security]
    else:
        h = history_bars(security,n,'1d','close')
        pct_change = h.pct_change(m)  # 3日的百分比变比（即3日涨跌幅）
        context.cache['pct_change'][security] = pct_change
    return pct_change


#### trade ####


def position_open(context,stock, value,bar_dict):
    '''
    开仓，买入指定价值的证券
    报单成功并成交（包括全部成交或部分成交，此时成交量大于0），返回True
    报单失败或者报单成功但被取消（此时成交量等于0），返回False
    '''
    order = _order_target_value(stock, value,bar_dict)
    #if order != None and order.filled_quantity > 0:
    logger.info("buyorder=%s"%order)
    if order.status==ORDER_STATUS.FILLED:
        # 报单成功并有成交则初始化最高价
        context.cache['buying_stocks'].append(stock)
        logger.info("订单成交，均价=%s"%order.avg_price)
        cur_price = get_close_price(stock, 1, '1m')
        # cur_price = order.price
        context.cache['last_high'][stock] = cur_price
        return True
    return False


def position_close(context,stock,position,bar_dict):
    '''
    平仓，卖出指定持仓
    平仓成功并全部成交，返回True
    报单失败或者报单成功但被取消（此时成交量等于0），或者报单非全部成交，返回False
    '''
    # order = _order_target_value(security, 0)  # 可能会因停牌失败
    order = _order(stock, -position.sellable,bar_dict )  # 卖出可用仓位
    #logger.info("orderId=%s"% orderId)
    #order=get_order(orderId)
    logger.info("sellorder=%s"% order)
    #if order.filled_quantity > 0:
    if order.status==ORDER_STATUS.FILLED:
        # 只要有成交，无论全部成交还是部分成交，则统计盈亏
        context.cache['selling_stocks'].append(stock)
        logger.info("下单成功，均价=%s"%order.avg_price)
        # 全部成交则删除相关证券的最高价缓存
        logger.info("全部成交则删除相关证券的最高价缓存")
        if stock in context.cache['last_high']:
            context.cache['last_high'].pop(stock)
        else:
            logger.warn("last high price of %s not found" % (stock))
        return True
    #if order.status == ORDER_STATUS.REJECTED:
        
    return False


def position_clear(context,bar_dict):
    '''
    清空卖出所有持仓
    '''
    if context.portfolio.positions:
        for stock in context.portfolio.positions.keys():
            position = context.portfolio.positions[stock]
            if position.sellable>0 and not bar_dict[stock].suspended:
                logger.info("==> 清仓，卖出所有股票")
                if not position_close(context,stock,position,bar_dict):
                    if stock not in context.cache['sell_failed_stocks']:
                        context.cache['sell_failed_stocks'].append(stock)
                else:
                    if stock in context.cache['sell_failed_stocks']:
                        context.cache['sell_failed_stocks'].remove(stock)

def position_adjust2(context ,bar_dict):
    positions_keys=context.portfolio.positions.keys()
    buy_stocks=copy.deepcopy(context.cache['buy_stocks'])
    logger.info("过滤前positions_keys=%s" % positions_keys)
    logger.info("过滤前buy_stocks=%s" % buy_stocks)
    res_list = list(set(positions_keys)&set(buy_stocks))
    logger.info("剩余资金=%s" % context.portfolio.cash)
    if len(res_list):
        for res in res_list:
            positions_keys.remove(res)
            buy_stocks.remove(res)
    logger.info("过滤完positions_keys=%s" % positions_keys)
    logger.info("过滤完buy_stocks=%s" % buy_stocks)    
    for stock in positions_keys:
        position = context.portfolio.positions[stock]
        logger.info("%s当前position=%s" %(stock,position))
        logger.info("%s当前持仓数目=%s" %(stock,position.quantity))
        logger.info("%s当前可卖数目=%s" %(stock,position.sellable))
        if stock not in context.cache['selling_stocks'] and position.sellable>0:
            if bar_dict[stock].suspended:
                continue;
            is_sell=position_close(context,stock,position,bar_dict)
            if not is_sell:
                logger.info("%s卖出失败" %(stock))
                #context.cache['selling_stocks'].remove(stock)
        elif position.quantity==0 and stock in context.cache['selling_stocks']:
                context.cache['selling_stocks'].remove(stock)
                
                
    if not len(context.cache['selling_stocks']):
        value = context.portfolio.cash /len(buy_stocks);
        for stock in buy_stocks:
            if stock not in context.cache['buying_stocks']:
                logger.info("开始买入股票=%s"%stock)
                if position_open(context,stock, value,bar_dict):
                    logger.info("下单买入成功%s"%stock)
       
        

def position_adjust(context ,bar_dict):
    '''
    根据待买股票创建或调整仓位
    对于因停牌等原因没有卖出的股票则继续持有
    始终保持持仓数目为g.buy_stock_count
    '''
    position_count = len(context.portfolio.positions)
    buy_count=len(context.cache['buy_stocks'])
    logger.info("position_count=%s" % position_count)
    logger.info("buy_count=%s" % buy_count)
    logger.info("剩余资金=%s" % context.portfolio.cash)
    positions_keys=context.portfolio.positions.keys()
    if position_count>=buy_count:
        for stock in positions_keys:
            if stock not in context.cache['buy_stocks']:
                position = context.portfolio.positions[stock]
                logger.info("%s当前position=%s" %(stock,position))
                logger.info("%s当前持仓数目=%s" %(stock,position.quantity))
                logger.info("%s当前可卖数目=%s" %(stock,position.sellable))
                if stock not in context.cache['selling_stocks'] and position.sellable>0:
                    if not position_close(context,stock,position,bar_dict):
                        position_count=position_count-1
                else:
                    if position.quantity==0:
                        if stock in context.cache['selling_stocks']:
                            context.cache['selling_stocks'].remove(stock)
                        position_count=position_count-1
                    
    #if position_count<buy_count:
    if len(context.cache['selling_stocks'])==0:
        value = context.portfolio.cash / (buy_count - position_count)
        for stock in context.cache['buy_stocks']:
            if stock not in positions_keys:
                if stock not in context.cache['buying_stocks']:
                    logger.info("开始买入股票=%s"%stock)
                    if position_open(context,stock, value,bar_dict):
                        logger.info("下单买入成功%s"%stock)
    

def _order(stock, amount,bar_dict):
    '''
    自定义下单
    根据Joinquant文档，当前报单函数都是阻塞执行，报单函数（如order_target）返回即表示报单完成
    报单成功返回报单（不代表一定会成交），否则返回None
    '''
    price=0
    if amount < 0:
        price= bar_dict[stock].limit_down
        logger.info("Selling out %s : %f" % (stock, -amount))
    elif amount > 0:
        price=bar_dict[stock].limit_up
        logger.info("Buying in %s : %f" % (stock, amount))

    # 如果股票停牌，创建报单会失败，order_target_value 返回None
    # 如果股票涨跌停，创建报单会成功，order_target_value 返回Order，但是报单会取消
    # 部成部撤的报单，聚宽状态是已撤，此时成交量>0，可通过成交量判断是否有成交
    return get_order(order_target_percent(stock, 0,style=LimitOrder(price)))


def _order_target_value(stock, value,bar_dict):
    '''
    自定义下单
    根据Joinquant文档，当前报单函数都是阻塞执行，报单函数（如order_target_value）返回即表示报单完成
    报单成功返回报单（不代表一定会成交），否则返回None
    '''
    price= bar_dict[stock].limit_up
    if value == 0:
        logger.info("Selling out %s" % (stock))
    else:
        logger.info("Order %s to value %f" % (stock, value))

    # 如果股票停牌，创建报单会失败，order_target_value 返回None
    # 如果股票涨跌停，创建报单会成功，order_target_value 返回Order，但是报单会取消
    # 部成部撤的报单，聚宽状态是已撤，此时成交量>0，可通过成交量判断是否有成交
    return get_order(order_value(stock, value,style=LimitOrder(price)))

#### utils ####


def get_growth_rate(security, n=20):
    '''
    获取股票n日以来涨幅，根据当前价计算
    '''
    lc = get_close_price(security, n)
    c = get_close_price(security, 1, '1m')

    if not math.isnan(lc) and not math.isnan(c) and lc != 0:
        return (c - lc) / lc
    else:
        logger.error("数据非法, security: %s, %d日收盘价: %f, 当前价: %f" %
                  (security, n, lc, c))
        return 0


def get_close_price(security, n, unit='1d'):
    '''
    获取前n个单位时间当时的收盘价
    '''
    close = 0
    while(n > 0):  # 如果前n日数据为nan，则取n-1日数据，直至n为1
        close = history_bars(security,n, unit, 'close')[0]
        if math.isnan(close):
            n -= 1
        else:
            break
    return close


def func_register(register, func, descr=''):
    register.append((func, descr))


#### log ####


def log_section(name, descr, sep=True, sep_char='-', sep_count=50):
    if sep:
        logger.info(sep_char * sep_count)
    logger.info('%s - %s' % (name, descr))


def log_param(context):
    log_section('param', '策略参数')
    for key in context.param:
        logger.info('%s : %s' %
                 (context.param[key][context.DESCR], str(context.param[key][context.VALUE])))


def log_filter(context):
    logger.info('filter - 过滤器:')
    for filter in context.filter:
        logger.info(' + ' + filter[context.DESCR])


def log_stop_loss(context):
    logger.info('stop_loss - 止损器:')
    for stop in context.stop_loss_minute:
        logger.info(' + ' + stop[context.DESCR])
    for stop in context.stop_loss_day:
        logger.info(' + ' + stop[context.DESCR])
        
###macd###
#结合缩量，选择买入时机  
#返回值n表示买入系数n,n越大可买入越多。
def buy_macd(index2):
    close = history_bars(index2,60, '1d','close')[index2]
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

