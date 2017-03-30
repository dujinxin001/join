#enable_profile()

from collections import OrderedDict
#from blacklist import *
import math
#import tradestat
import talib as tl
import pandas as pd


def init(context):
    log_section('init', '初始化', sep=False)

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
    #context.trade_stat = tradestat.trade_stat()

    # 打印自定义信息
    # log.info('')
    # 打印策略参数
    log_param(context)
    # 打印使用的过滤器
    log_filter(context)
    # 打印使用的止损器
    log_stop_loss(context)
    
    task.daily(filter_by_query,time_rule=market_open(hour=5, minute=19))


def before_trade(context):
    log_section('before_trading_start', '盘前处理:又开始赚钱了')

    reset_day_param(context)
    # 盘前就判断三黑鸦状态，因为判断的数据为前4日
    context.cache['is_last_day_3_crows'] = _is_3_crows(
        context.param['index_3_crows'][context.VALUE])
    if context.cache['is_last_day_3_crows']:
        logger.info("==> 前4日已经构成三黑鸦形态")


def after_trading_end(context):
    log_section('after_trading_end', '盘后处理：大赚收钱')

    context.trade_stat.report(context)
    # 得到当前未完成订单
    orders = get_open_order()
    for _order in orders.values():
        logger.info("取消未完成的订单: %s" % (_order.security))

def handle_data(context, data):
    '''
    按分钟回测
    '''
    hour = context.now.hour
    minute = context.now.minute
    #进行所有止损器判断，执行止损
    for stop in context.stop_loss_minute:
        if hour==9 and minute==31:
            logger.info("执行止损检查")
        stop[context.FUNC](context, data)

    # 每天指定时间检查是否调仓并处理
    adjust_position_time = context.param['adjust_position_time'][context.VALUE]
    if hour == adjust_position_time[context.HOUR] and minute == adjust_position_time[context.MINUTE] and not context.cache['stop_trade']:
        _adjust_position(context, data)


def _adjust_position(context, data):
    logger.info("调仓日计数 [%d]" % (context.cache['day_count']))

    # 回看指数前20天的涨幅
    gr_index_l = get_growth_rate(context.param['index_l'][context.VALUE])
    gr_index_s = get_growth_rate(context.param['index_s'][context.VALUE])
    logger.info("当前%s指数的20日涨幅 [%.2f%%]" % (
        get_securities(context.param['index_l'][context.VALUE]).symbol, gr_index_l * 100))
    logger.info("当前%s指数的20日涨幅 [%.2f%%]" % (
        get_securities(context.param['index_s'][context.VALUE]).symbol, gr_index_s * 100))

    if gr_index_l <= context.param['index_growth_rate'][context.VALUE] and gr_index_s <= context.param['index_growth_rate'][context.VALUE]:
        position_clear(context)
        context.cache['day_count'] = 0
    else:
        if context.cache['day_count'] % context.param['period'][context.VALUE] == 0:
            logger.info("==> 满足条件进行调仓")
            if gr_index_l > context.param['index_growth_rate'][context.VALUE] and gr_index_s < context.param['index_growth_rate'][context.VALUE]:
                stock_list = context.cache['stock_list']
                context.cache['stock_list']=filter_by_growth_rate(stock_list, context, data)
            buy_stocks = pick_stocks(context, data)
            if len(buy_stocks)==0:
                logger.info("选股后无买股票: %s" %(buy_stocks))
                position_adjust(context, buy_stocks)
                context.cache['day_count']=0
                return
            logger.info("选股后可买股票: %s" % (buy_stocks))
            position_adjust(context, buy_stocks)
        context.cache['day_count'] += 1


#### initialize ####

# 设置系统参数
def set_sys(context):
    context.set_benchmark("399300.SZ")
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    context.set_commission(0.0003)
    # 设定滑点为百分比
    context.set_slippage(0.004)

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
    p['period'] = (4, '调仓频率，单位：日')
    p['adjust_position_time'] = ((14, 49), '配置调仓时间（24小时分钟制）')
    p['pick_by_pe'] = (False, '是否根据PE选股')
    p['pick_by_eps'] = (True, '是否根据EPS选股')
    p['pick_stock_count'] = (100, '备选股票数目')
    p['filter_gem'] = (True, '是否过滤创业板股票')
    p['filter_blacklist'] = (False, '是否过滤黑名单股票，回测建议关闭，模拟运行时开启')
    p['is_filter_new']=(False,'是否过滤新股')
    p['is_rank_stock'] = (True, '是否对股票评分')
    p['rank_stock_count'] = (20, '参与评分的股票数目')  # 评分的股票数目不能大于备选股票数目
    p['index_l'] = ('000016.SH', '大盘股指数')  # 上证50指数
    p['index_s']=('399678.SZ','小盘股指数') # 中证500指数
    p['buy_stock_count'] = (2, '买入股票数目')
    p['index_growth_rate'] = (0.005, '判定调仓的二八指数n日增幅')  # n = 20
    p['index_3_crows'] = ('399300.SZ', '判定三黑鸦的指数')
    p['index_price'] = ('399678.SZ', '判定价格止损的指数')
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
    #func_register(context.filter, filter_by_query, '查询财务数据库过滤')
    func_register(context.filter, filter_gem, '过滤创业版股票')
    func_register(context.filter, filter_paused, '过滤停牌股票')
    func_register(context.filter, filter_st, '过滤ST及其他具有退市标签的股票')
    func_register(context.filter, filter_limitup, '过滤涨停的股票')
    func_register(context.filter, filter_limitdown, '过滤跌停的股票')
    func_register(context.filter,filter_blacklist, '过滤黑名单股票')
    func_register(context.filter,filter_new, '过滤新股')
    func_register(context.filter, filter_by_rank, '评分过滤器')

def set_stop_loss(context):
    context.stop_loss_day = []
    context.stop_loss_minute = []
    
    func_register(context.stop_loss_minute, stop_loss_by_price, '大盘价格止损')
    func_register(context.stop_loss_minute, stop_loss_by_3_crows, '三黑鸦止损')
    #func_register(context.stop_loss_minute,stop_loss_by_index_l,'二八止损')
    #func_register(context.stop_loss_minute,stop_loss_by_stock,'个股止损')
    #func_register(context.stop_loss_minute,stop_profit_by_stock,'个股止盈')


def set_cache(context):
    c = {}
    # 调仓日计数器，单位：日
    c['day_count'] = 0
    # 缓存股票持仓后的最高价
    c['last_high'] = {}
    df = get_fundamentals(query())
    c['stock_list'] = df.columns.values
    #c['stock_list']=filter_by_query(context)
    # 缓存当日个股250天内最大的3日涨幅，避免当日反复获取，每日盘后清空
    c['pct_change'] = {}
    # 缓存指数下跌分钟数
    c['minute_count_index_ls_drop'] = 0
    c['minute_count_cur_drop'] = 0
    c['is_last_day_3_crows'] = False
    c['is_day_stop_loss_by_price'] = False
    c['stop_trade'] = False  # 暂停当天交易
    context.cache = c


def reset_day_param(context):
    '''
    重置当日参数，仅针对需要当日需要重置的参数
    '''
    logger.info("=>盘后重置当日参数")
    df = get_fundamentals(query())
    context.cache['stock_list'] =df.columns.values
    #g.cache['stock_list']=get_index_stocks('000300.XSHG')
    #context.cache['stock_list']=filter_by_query(context)
    # 重置当日大盘价格止损状态
    context.cache['is_day_stop_loss_by_price'] = False
    # 重置三黑鸦状态
    context.cache['is_last_day_3_crows'] = False
    context.cache['minute_count_cur_drop'] = 0
    context.cache['minute_count_index_ls_drop'] = 0
    # 清空当日个股250天内最大的3日涨幅的缓存
    context.cache['pct_change'].clear()
    context.cache['stop_trade'] = False  # 暂停当天交易

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

def filter_by_query(context,data):
    '''
    查询财务数据库过滤
    '''
    logger.info("=>开始执行财务条件过滤")
    pe_min = 0
    pe_max = 200
    eps_min = 0

    q = query()
    #q=query(valuation).filter(valuation.market_cap>=50,valuation.code.in_(stock_list))
    if context.param['pick_by_pe'][context.VALUE]:
        q = q.filter(
            fundamentals.equity_valuation_indicator.pe_ratio > pe_min,
            fundamentals.equity_valuation_indicator.pe_ratio < pe_max
        )
    if context.param['pick_by_eps'][context.VALUE]:
        q = q.filter(fundamentals.financial_analysis_indicator.earnings_per_share > eps_min)
        
    df = get_fundamentals(
        q.order_by(fundamentals.equity_valuation_indicator.market_cap_2.asc()
                   ).limit(
            context.param['pick_stock_count'][context.VALUE]
        ))
    logger.info("=>结束执行财务条件过滤")
    context.cache['stock_list']=list(df.columns.values)

def filter_paused(stock_list, context, data):
    '''
    过滤停牌股票
    '''
    logger.info("=>开始执行过滤停牌的股票")
    return [stock for stock in stock_list if data[stock].sf!=1]

def filter_st(stock_list, context, data):
    '''
    过滤ST及其他具有退市标签的股票
    '''
    logger.info("=>开始执行过滤ST及其他具有退市标签的股票")
    return [stock for stock in stock_list
            if not is_st(stock)]


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
    stock_list2=[]
    for stock in stock_list:
        yesterday = get_history(2,'1d', 'close')[stock].values[-1]
        zt = round(1.10 * yesterday,2)
        if stock in context.portfolio.positions.keys() or round(data[stock].last,2) < zt:
            stock_list2.append(stock)
    return stock_list2

def filter_limitdown(stock_list, context, data):
    '''
    过滤跌停的股票
    '''
    logger.info("=>开始执行过滤跌停的股票")
    stock_list2=[]
    for stock in stock_list:
        yesterday = get_history(2,'1d', 'close')[stock].values[-1]
        dt = round(0.99 * yesterday,2)
        if stock in context.portfolio.positions.keys() or round(data[stock].last,2) > dt:
            stock_list2.append(stock)
    return stock_list2


def filter_by_growth_rate(stock_list, context, data):
    '''
    过滤n日增长率为负的股票
    '''
    logger.info("=>开始过滤n日增长率为负的股票")
    n = 25
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
        start_date = context.current_dt.date() - datetime.timedelta(delta)
        return [stock for stock in stock_list if get_securities(stock).listed_date < start_date]
    return stock_list

def filter_by_rank(stock_list, context, data):
    '''
    评分过滤器
    '''
    if context.param['is_rank_stock'][context.VALUE]: 
        logger.info("=>开始进行股票评分%s" %stock_list)
        if len(stock_list) >context.param['rank_stock_count'][context.VALUE]:
            stock_list = stock_list[:context.param['rank_stock_count'][context.VALUE]]
        if len(stock_list) > 0:
            dst_stocks = {}
            for stock in stock_list:
                h_low = get_history(130, '1d', 'low')[stock]
                h_high = get_history(130, '1d','high')[stock]
                h_close = get_history(130, '1d','close')[stock].values
                low_price_130 = h_low.min()
                high_price_130 = h_high.max()
        
                #avg_15 = data[stock].get_mavg(15, field='minute')
                avg_15 = h_close[-15:].mean()
                cur_price = data[stock].close
        
                # avg_15 = h['close'][-15:].mean()
                # cur_price = get_close_price(stock, 1, '1m')
        
                score = (cur_price - low_price_130)+(cur_price - high_price_130)+(cur_price - avg_15)
                # score = ((cur_price-low_price_130) + (cur_price-high_price_130) +
                # (cur_price-avg_15)) / cur_price
                dst_stocks[stock] = score
        
            df = pd.DataFrame(data=list(dst_stocks.values()), index=dst_stocks.keys(),columns=['score'])
            df.columns = ['score']
            df = df.sort(columns='score', ascending=True)
            logger.info("<=个股评分结束")
            return df.index

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
            h_close = get_history(160, '1d', 'close')[index].values
            h_low = get_history(160, '1d', 'low')[index]
            h_high = get_history(160, '1d','high')[index]
            low_price =h_low.min()
            high_price = h_high.max()
            # if high_price > 2 * low_price:
            if high_price > 2.2 * low_price \
                    and h_close[-1] < h_close[-4] \
                    and h_close[-1] > h_close[-100]:
                # 当日第一次输出日志
                logger.info("==> 大盘止损，%s指数前130日内最高价超过最低价2倍, 最高价: %f, 最低价: %f" % (
                    get_securities(index).symbol, high_price, low_price))
                context.cache['is_day_stop_loss_by_price'] = True
    
        if context.cache['is_day_stop_loss_by_price']:
            position_clear(context)
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
                             (get_securities(index).symbol, n))
    
                position_clear(context)
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
    h_close=get_history(4,'1d','close')[stock]
    h_open=get_history(4,'1d','open')[stock]

    if len(h_close) < 4 or len(h_open) < 4:
        return False

    # 一阳三阴
    if h_close[-4] > h_open[-4] \
            and (h_close[-1] < h_open[-1] and h_close[-2] < h_open[-2] and h_close[-3] < h_open[-3]):
        # and (h_close[-1] < h_close[-2] and h_close[-2] < h_close[-3]) \
        # and h_close[-1] / h_close[-4] - 1 < -0.045:
        return True
    return False


#### trade ####
def position_open(context,security, value):
    '''
    开仓，买入指定价值的证券
    报单成功并成交（包括全部成交或部分成交，此时成交量大于0），返回True
    报单失败或者报单成功但被取消（此时成交量等于0），返回False
    '''
    order_id = _order_target_value(security, value)
    order=get_order(order_id)
    if order != None and order.filled_shares > 0:
        # 报单成功并有成交则初始化最高价
        cur_price = get_close_price(security, 1, '1m')
        # cur_price = order.price
        context.cache['last_high'][security] = cur_price
        return True
    return False

def position_close(context,position,stock):
    '''
    平仓，卖出指定持仓
    平仓成功并全部成交，返回True
    报单失败或者报单成功但被取消（此时成交量等于0），或者报单非全部成交，返回False
    '''
    # order = _order_target_value(security, 0)  # 可能会因停牌失败
    order_id = _order(stock, -position.sellable)  # 卖出可用仓位
    order=get_order(order_id)
    if order:
        #if order.filled_shares > 0:
            # 只要有成交，无论全部成交还是部分成交，则统计盈亏
            #context.trade_stat.watch(stock, order.filled_shares,
            #                   position.bought_value, position.sold_value)

        if order.filled_shares == order.quantity:
            # 全部成交则删除相关证券的最高价缓存
            if stock in context.cache['last_high']:
                context.cache['last_high'].pop(stock)
            else:
                logger.warn("last high price of %s not found" % (stock))
            return True
    return False


def position_clear(context):
    '''
    清空卖出所有持仓
    '''
    if context.portfolio.positions:
        logger.info("==> 清仓，卖出所有股票")
        for stock in context.portfolio.positions.keys():
            position = context.portfolio.positions[stock]
            position_close(context,position,stock)


def position_adjust(context, buy_stocks):
    '''
    根据待买股票创建或调整仓位
    对于因停牌等原因没有卖出的股票则继续持有
    始终保持持仓数目为g.buy_stock_count
    '''
    for stock in context.portfolio.positions.keys():
        if stock not in buy_stocks:
            logger.info("stock [%s] in position is not buyable" % (stock))
            position = context.portfolio.positions[stock]
            position_close(context,position,stock)
        else:
            logger.info("stock [%s] is already in position" % (stock))

    # 根据股票数量分仓
    # 此处只根据可用金额平均分配购买，不能保证每个仓位平均分配
    position_count = len(context.portfolio.positions)
    count = context.param['buy_stock_count'][context.VALUE]
    if len(buy_stocks) > position_count:
        value = context.portfolio.cash / (len(buy_stocks) - position_count)
        for stock in buy_stocks:
            if context.portfolio.positions[stock].bought_quantity == 0:
                if position_open(context,stock, value):
                    if len(context.portfolio.positions) == count:
                        break


def _order(security, amount):
    '''
    自定义下单
    根据Joinquant文档，当前报单函数都是阻塞执行，报单函数（如order_target）返回即表示报单完成
    报单成功返回报单（不代表一定会成交），否则返回None
    '''
    if amount < 0:
        logger.info("Selling out %s : %f" % (security, -amount))
    elif amount > 0:
        logger.info("Buying in %s : %f" % (security, amount))

    # 如果股票停牌，创建报单会失败，order_target_value 返回None
    # 如果股票涨跌停，创建报单会成功，order_target_value 返回Order，但是报单会取消
    # 部成部撤的报单，聚宽状态是已撤，此时成交量>0，可通过成交量判断是否有成交
    return order_share(security, amount)


def _order_target_value(security, value):
    '''
    自定义下单
    根据Joinquant文档，当前报单函数都是阻塞执行，报单函数（如order_target_value）返回即表示报单完成
    报单成功返回报单（不代表一定会成交），否则返回None
    '''
    if value == 0:
        logger.info("Selling out %s" % (security))
    else:
        logger.info("Order %s to value %f" % (security, value))

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
        close = get_history(n, unit, 'close')[security][0]
        if math.isnan(close):
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
        
