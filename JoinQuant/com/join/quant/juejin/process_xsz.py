import math
import numpy as np
import pandas as pd
import datetime
from com.join.quant.juejin.blacklist import get_blacklist
from com.join.quant.juejin.GObject import MyClass
import logging as log

class xiaoshizhi():
    global g
    global log
    def __init__(self,strategy):
        self.strategy=strategy
        self.g=MyClass()
        log.basicConfig(level=log.DEBUG) 
    def initialize(self):
        self.log_section('initialize', '初始化', sep=False)
        # 设置系统参数
        #self.set_sys()
        # 设置常数
        self.set_const()
        # 设置策略参数
        self.set_param()
        # 设置过滤器
        self.set_filter()
        # 设置止损器
        self.set_stop_loss()
        # 设置缓存参数
        self.set_cache()
        # 加载统计模块
        #self.g.trade_stat = tradestat.trade_stat()
    
        # 打印自定义信息
        # log.info('')
        # 打印策略参数
        self.log_param()
        # 打印使用的过滤器
        self.log_filter()
        # 打印使用的止损器
        self.log_stop_loss()
    
    
    def process_initialize(self):
        self.log_section('process_initialize', '进程重新启动')
    
    
    def after_code_changed(self):
        self.log_section('after_code_changed', '代码发生修改')
    
    
    def before_trading_start(self):
        self.log_section('before_trading_start', '盘前处理:又开始赚钱了')
    
        self.reset_day_param()
        # 盘前就判断三黑鸦状态，因为判断的数据为前4日
        self.g.cache['is_last_day_3_crows'] =  self._is_3_crows(self.g.param['index_3_crows'][self.g.VALUE])
        if self.g.cache['is_last_day_3_crows']:
            log.info("==> 前4日已经构成三黑鸦形态")
    
    
    def after_trading_end(self):
        self.log_section('after_trading_end', '盘后处理：大赚收钱')
        #self.self.g.trade_stat.report(context)
        # 得到当前未完成订单
        #orders = self.get_open_orders()
        #for _order in orders.values():
        #    log.info("取消未完成的订单: %s" % (_order.order_id))
    
    
    def handle_data(self,bar):
        '''
        按分钟回测
        '''
        # for key in self.g.stop_loss_minute:
        #     self.g.stop_loss_minute[key][self.g.FUNC](context, data)
        dt = datetime.datetime.utcfromtimestamp(bar.utc_time)
        dt = dt + datetime.timedelta(hours=8)
        hour = dt.hour
        minute = dt.minute
        if hour == 9 and minute == 30:
            self.before_trading_start()
        # 进行所有止损器判断，执行止损
        for stop in self.g.stop_loss_minute:
            if hour == 9 and minute == 31:
                log.info("执行止损检查")
            stop[self.g.FUNC]()
    
        # 每天指定时间检查是否调仓并处理
        adjust_position_time = self.g.param['adjust_position_time'][self.g.VALUE]
        if hour == adjust_position_time[self.g.HOUR] and minute == adjust_position_time[self.g.MINUTE] and not self.g.cache['stop_trade']:
            log.info("adjust_position_time[self.g.HOUR]=%s"%hour)
            log.info("adjust_position_time[self.g.MINUTE]=%s"% minute)
            self._adjust_position(dt)
        if hour == 15:
            self.after_trading_end()
    
    def _adjust_position(self,dt):
        log.info("调仓日计数 [%d]" % (self.g.cache['day_count']))
    
        # 回看指数前20天的涨幅
        gr_index_l = self.get_growth_rate(self.g.param['index_l'][self.g.VALUE],20,dt)
        gr_index_s = self.get_growth_rate(self.g.param['index_s'][self.g.VALUE],20,dt)
        log.info("当前%s指数的20日涨幅 [%.2f%%]" % (self.g.param['index_l'][self.g.VALUE], gr_index_l * 100))
        log.info("当前%s指数的20日涨幅 [%.2f%%]" % (self.g.param['index_s'][self.g.VALUE], gr_index_s * 100))
        #if gr_index_l <= self.g.param['index_growth_rate'][self.g.VALUE] and gr_index_s <= self.g.param['index_growth_rate'][self.g.VALUE]:
        if 1==2:
            self.position_clear()
            self.g.cache['day_count'] = 0
        else:
            if self.g.cache['day_count'] % self.g.param['period'][self.g.VALUE] == 0:
                log.info("==> 满足条件进行调仓")
                buy_stocks = self.pick_stocks()
                if len(buy_stocks) == 0:
                    log.info("选股后无买股票: %s" % (buy_stocks))
                    self.g.cache['day_count'] = 0
                    return
                log.info("选股后可买股票: %s" % (buy_stocks))
                self.position_adjust(buy_stocks)
            self.g.cache['day_count'] += 1
    
    
    # 按月运行
    # run_monthly(func, monthday, time='open', reference_security)
    # 按周运行
    # run_weekly(func, weekday, time='open', reference_security)
    # 每天内何时运行
    # run_daily(func, time='open', reference_security)
    
    
    #### initialize ####
    
    # 设置系统参数
    def set_sys(self):
        # 以上证指数为基准
        # 设定上证指数作为基准
        # set_benchmark('000001.XSHG')
        # 设定沪深300指数作为基准
        #self.set_benchmark('000300.XSHG')
        # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
        #self.set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003,
        #                         close_commission=0.0003, close_today_commission=0, min_commission=5), type='stock')
        # 设定滑点为百分比
        #self.set_slippage(PriceRelatedSlippage(0.004))
        # 使用真实价格回测,回测请注释
        # set_option('use_real_price', True)
        pass
    
    # 设置常数
    def set_const(self):
        self.g.VALUE = 0
        self.g.FUNC = 0
        self.g.DESCR = 1
        self.g.HOUR = 0
        self.g.MINUTE = 1
    
    # 设置策略参数
    def set_param(self):
        p = {}
    
        p['period'] = (3, '调仓频率，单位：日')
        p['adjust_position_time'] = ((9, 40), '配置调仓时间（24小时分钟制）')
        p['pick_by_pe'] = (False, '是否根据PE选股')
        p['pick_by_eps'] = (True, '是否根据EPS选股')
        p['pick_stock_count'] = (100, '备选股票数目')
        p['filter_gem'] = (True, '是否过滤创业板股票')
        p['filter_blacklist'] = (True, '是否过滤黑名单股票，回测建议关闭，模拟运行时开启')
        p['is_filter_new'] = (False, '是否过滤新股')
        p['is_rank_stock'] = (True, '是否对股票评分')
        p['rank_stock_count'] = (20, '参与评分的股票数目')  # 评分的股票数目不能大于备选股票数目
        p['index_l'] = ('SHSE.000016', '大盘股指数')  # 上证50指数
        p['index_s'] = ('SHSE.000905', '小盘股指数')  # 中证500指数
        p['buy_stock_count'] = (2, '买入股票数目')
        p['index_growth_rate'] = (0.005, '判定调仓的二八指数n日增幅')  # n = 20
        p['index_3_crows'] = ('SHSE.000300', '判定三黑鸦的指数')
        p['index_price'] = ('SHSE.000300', '判定价格止损的指数')
        p['is_stock_stop_loss'] = (False, '是否个股止损')
        p['is_stock_stop_profit'] = (False, '是否个股止盈')
        p['is_market_stop_loss_by_price'] = (True, '是否根据大盘历史价格止损')
        p['is_market_stop_loss_by_28_index'] = (False, '是否根据28指数值实时进行止损')
        p['is_market_stop_loss_by_3_black_crows'] = (True, '是否进行三黑鸦止损')
    
        self.g.param = p
    
    # 设置过滤器
    def set_filter(self):
        # self.g.filter={}
        self.g.filter = []
        # func_register(self.g.filter, filter_market_time, '指数MACD过滤')
        self.func_register(self.g.filter, self.filter_by_query, '查询财务数据库过滤')
        self.func_register(self.g.filter, self.filter_gem, '过滤创业版股票')
        #self.func_register(self.g.filter, self.filter_paused, '过滤停牌股票')
        self.func_register(self.g.filter, self.filter_st, '过滤ST及其他具有退市标签的股票')
        self.func_register(self.g.filter, self.filter_limitup, '过滤涨停的股票')
        self.func_register(self.g.filter, self.filter_limitdown, '过滤跌停的股票')
        self.func_register(self.g.filter, self.filter_blacklist, '过滤黑名单股票')
        # func_register(self.g.filter,filter_by_growth_rate, '过滤n日增长率为负的股票')
        #self.func_register(self.g.filter, self.filter_new, '过滤新股')
        # func_register(self.g.filter,filter_by_chaodie, '超跌过滤器')
        # func_register(self.g.filter, delect_stock, 'delect_stock')
        # func_register(self.g.filter, filter_by_rank_0, '市值评分过滤器')
        self.func_register(self.g.filter, self.filter_by_rank, '评分过滤器')
    
    
    def set_stop_loss(self):
        self.g.stop_loss_day = []
        self.g.stop_loss_minute = []
        
        self.func_register(self.g.stop_loss_minute, self.stop_loss_by_price, '大盘价格止损')
        self.func_register(self.g.stop_loss_minute, self.stop_loss_by_3_crows, '三黑鸦止损')
        self.func_register(self.g.stop_loss_minute, self.stop_loss_by_index_l, '二八止损')
        self.func_register(self.g.stop_loss_minute, self.stop_loss_by_stock, '个股止损')
        self.func_register(self.g.stop_loss_minute, self.stop_profit_by_stock, '个股止盈')
    
    
    def set_cache(self):
        c = {}
    
        # 调仓日计数器，单位：日
        c['day_count'] = 0
    
        # 缓存股票持仓后的最高价
        c['last_high'] = {}
    
        c['stock_list']=[]
        c['stock_list']=self.get_all_symbol(1, 1)
    
        # 缓存当日个股250天内最大的3日涨幅，避免当日反复获取，每日盘后清空
        c['pct_change'] = {}
        # 缓存指数下跌分钟数
        c['minute_count_index_ls_drop'] = 0
        c['minute_count_cur_drop'] = 0
        c['is_last_day_3_crows'] = False
        c['is_day_stop_loss_by_price'] = False
        c['stop_trade'] = False  # 暂停当天交易
    
        self.g.cache = c
    
    
    def reset_day_param(self):
        '''
                        重置当日参数，仅针对需要当日需要重置的参数
        '''
        log.info("=>盘后重置当日参数")
        self.g.cache['stock_list']=self.get_all_symbol(1, 1)
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
    
    
    def pick_stocks(self):
        '''
                        选取指定数目的小市值股票，再进行过滤，最终挑选指定可买数目的股票
        '''
        stock_list = self.g.cache['stock_list']
        # for key in self.g.filter.keys():
        #     stock_list = self.g.filter[key][self.g.FUNC](stock_list, context, data)
        for filter in self.g.filter:
            stock_list = filter[self.g.FUNC](stock_list)
            if len(stock_list) == 0:
                log.info("股票被过滤没了")
                return stock_list
    
        # 选取指定可买数目的股票
        return stock_list[:self.g.param['buy_stock_count'][self.g.VALUE]]
    
    
    def filter_by_query(self,stock_list):
        '''
                        查询财务数据库过滤
        '''
        log.info("=>开始执行财务条件过滤%s,%s" % (','.join(stock_list), len(stock_list)))
        pe_min = 0
        pe_max = 200
        eps_min = 0
        
        #过滤PE
        if self.g.param['pick_by_pe'][self.g.VALUE]:
            market_list=self.strategy.get_last_market_index(','.join(stock_list))
            pe_list=[]
            for market in market_list:
                if market.pe_ratio>pe_min and market.pe_ratio<pe_max:
                    pe_list.append(market.symbol);
            stock_list=pe_list
        
        #过滤EPS
        if self.g.param['pick_by_eps'][self.g.VALUE]:
            financial_list=self.strategy.get_last_financial_index(','.join(stock_list))
            eps_list=[]
            for financial in financial_list:
                if financial.eps>eps_min:
                    eps_list.append(financial.symbol)
            stock_list=eps_list
            
        #市值排序
        market_list=self.strategy.get_last_market_index(','.join(stock_list))
        market_dict={}
        for market in market_list:
            market_dict[market.symbol]=market.market_value
        df = pd.DataFrame(list(market_dict.values()), index=market_dict.keys())
        df.columns = ['market_score']
        df = df.sort(columns='market_score', ascending=True)
        stock_list=list(df.index)
        stock_list=stock_list[0:self.g.param['pick_stock_count'][self.g.VALUE]]
        log.info("=>结束执行财务条件过滤%s" % stock_list)
        return stock_list
    
    def filter_paused(self,stock_list):
        '''
                        过滤停牌股票
        '''
        log.info("=>开始执行过滤停牌的股票%s" % stock_list)
        list_all=self.get_all_symbol(1, 0)
        return [stock for stock in stock_list if stock not in list_all]
    
    
    def filter_st(self,stock_list):
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
    
    
    def filter_gem(self,stock_list):
        '''
                        过滤创业版股票
        '''
        if self.g.param['filter_gem'][self.g.VALUE]:
            log.info("=>开始执行过滤创业板股票%s" % stock_list)
            return [stock for stock in stock_list if not stock.split('.')[1].startswith('300')]
        return stock_list
    
    
    def filter_limitup(self,stock_list):
        '''
                        过滤涨停的股票
        '''
        log.info("=>开始执行过滤涨停的股票%s" % stock_list)
        threshold = 1.00
        stock_list2 = []
        position_list=[]
        # 已存在于持仓的股票即使涨停也不过滤，避免此股票再次可买，但因被过滤而导致选择别的股票
        for position in self.strategy.get_positions():
            position_list.append(position.exchange +'.'+position.sec_id)
        for stock in stock_list:
            tick=self.strategy.get_last_ticks(stock)
            if stock in position_list or tick[-1].last_price < (tick[-1].upper_limit * threshold):
                log.info("=>%s的现价：%s" % (stock, tick[-1].last_price))
                log.info("=>%s的涨停价：%s" % (stock, tick[-1].upper_limit * threshold))        
                stock_list2.append(stock)
        return stock_list2
    
    
    def filter_limitdown(self,stock_list):
        '''
                        过滤跌停的股票
        '''
        log.info("=>开始执行过滤跌停的股票%s" % stock_list)
        threshold = 1.00
        stock_list2 = []
        position_list=[]
        for position in self.strategy.get_positions():
            position_list.append(position.exchange +'.'+position.sec_id)
        for stock in stock_list:
            tick=self.strategy.get_last_ticks(stock)
            if stock in position_list or tick[-1].last_price > tick[-1].lower_limit * threshold:
                log.info("=>%s的现价：%s" % (stock, tick[-1].last_price))
                log.info("=>%s的跌停价：%s" % (stock, tick[-1].lower_limit  * threshold))        
                stock_list2.append(stock)
        return stock_list2
    
    
    def filter_by_growth_rate(self,stock_list):
        '''
                        过滤n日增长率为负的股票
        '''
        n = 20
        return [stock for stock in stock_list if self.get_growth_rate(stock, n) > 0]
    
    
    def filter_blacklist(self,stock_list):
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
        if len(stock_list) > 0:
            dst_stocks = {}
            for stock in stock_list:
                #q = query(valuation).filter(valuation.code == stock)
                #df = get_fundamentals(q)
                #log.info("股票%s的总市值%s" % (stock, df['market_cap'][0]))
                # h = attribute_history(stock, 5, unit='1d', fields=('money'), skip_paused=True)
                # low_price_130 = h.low.min()
                # high_price_130 = h.high.max()
        
                avg_5 = data[stock].mavg(5, field='money')
                log.info("股票%s的5日平均成交额%s" % (stock, avg_5))
                #score = (2.2 * math.log(df['market_cap'][0])) + (1.22 * math.log(avg_5))
                #dst_stocks[stock] = score
        
            df = pd.DataFrame(dst_stocks.values(), index=dst_stocks.keys())
            df.columns = ['score']
            df = df.sort(columns='score', ascending=True)
            log.info("<=个股市值评分结束")
            return df.index
    
    
    def filter_by_rank(self,stock_list):
        '''
                        评分过滤器
        '''
        if self.g.param['is_rank_stock'][self.g.VALUE]: 
            log.info("=>开始进行股票评分%s" % stock_list)
            if len(stock_list) > self.g.param['rank_stock_count'][self.g.VALUE]:
                stock_list = stock_list[:self.g.param['rank_stock_count'][self.g.VALUE]]
            if len(stock_list) > 0:
                dst_stocks = {}
                for stock in stock_list:
                    dailybars=self.strategy.get_last_n_dailybars(stock, 130)
                    #h = attribute_history(stock, 130, unit='1d', fields=('close', 'high', 'low'), skip_paused=True)
                    low_price=[]
                    high_price=[]
                    close_price=[]
                    for dailybar in dailybars:
                        low_price.append(dailybar.low)
                        high_price.append(dailybar.high)
                        close_price.append(dailybar.close)
                    low_price_130 = min(low_price)
                    high_price_130 = max(high_price)
                    avg_15 =  np.array(close_price)[-15:].mean()
                    cur_price = self.strategy.get_last_ticks(stock)[-1].last_price
                    score = (cur_price - low_price_130) + (cur_price - high_price_130) + (cur_price - avg_15)
                    dst_stocks[stock] = score
                df = pd.DataFrame(list(dst_stocks.values()), index=dst_stocks.keys())
                df.columns = ['score']
                df = df.sort(columns='score', ascending=True)
                log.info("<=个股评分结束")
                return list(df.index)
    
    #### stop loss ####
    
    def stop_loss_by_price(self):
        '''
        大盘指数前130日内最高价超过最低价2倍，则清仓止损
        基于历史数据判定，因此若状态满足，则当天都不会变化
        增加此止损，回撤降低，收益降低
        '''
        if self.g.param['is_market_stop_loss_by_price'][self.g.VALUE]:
            index = self.g.param['index_price'][self.g.VALUE]
            if not self.g.cache['is_day_stop_loss_by_price']:
                dailybars=self.strategy.get_last_n_dailybars(index, 160)
                low_price=[]
                high_price=[]
                for dailybar in dailybars:
                    low_price.append(dailybar.low)
                    high_price.append(dailybar.high)
                low_price = min(low_price);
                high_price = max(high_price)
                # if high_price > 2 * low_price:
                if high_price > 2.2 * low_price \
                        and dailybars[-1].close < dailybars[-4].close \
                        and dailybars[-1].close > dailybars[-100].close:
                    # 当日第一次输出日志
                    log.info("==> 大盘止损，%s指数前130日内最高价超过最低价2倍, 最高价: %f, 最低价: %f" % (index, high_price, low_price))
                    self.g.cache['is_day_stop_loss_by_price'] = True
        
            if self.g.cache['is_day_stop_loss_by_price']:
                self.position_clear()
                self.g.cache['day_count'] = 0
                self.g.cache['stop_trade'] = True  # 暂停当天交易
    
        return self.g.cache['is_day_stop_loss_by_price']
    
    
    def stop_loss_by_3_crows(self):
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
                if self.get_growth_rate(index, 1) < 0:
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
    
    
    def _is_3_crows(self,stock):
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
        dailybars=self.strategy.get_last_n_dailybars(stock, 4)
        h_close=[]
        h_open=[]
        for dailybar in dailybars:
            h_close.append(dailybar.close)
            h_open.append(dailybar.open)
        if len(h_close) < 4 or len(h_open) < 4:
            return False
    
        # 一阳三阴
        if h_close[-4] > h_open[-4] \
                and (h_close[-1] < h_open[-1] and h_close[-2] < h_open[-2] and h_close[-3] < h_open[-3]):
            # and (h_close[-1] < h_close[-2] and h_close[-2] < h_close[-3]) \
            # and h_close[-1] / h_close[-4] - 1 < -0.045:
            return True
        return False
    
    
    def stop_loss_by_index_l(self):
        '''
                        二八止损
        minute
        '''
        if self.g.param['is_market_stop_loss_by_28_index'][self.g.VALUE]:
            count = 120
            # 回看指数前20天的涨幅
            gr_index_l = self.get_growth_rate(self.g.param['index_l'][self.g.VALUE],20)
            gr_index_s = self.get_growth_rate(self.g.param['index_s'][self.g.VALUE],20)
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
    
    
    def stop_loss_by_stock(self):
        '''
                        个股止损
        minute 级别
        '''
        if self.g.param['is_stock_stop_loss'][self.g.VALUE]:
            log.info("=>开始进行个股止损")
            for position in self.strategy.get_positions():
                symbol=position.exchange+'.'+position.sec_id 
                cur_price = self.strategy.get_last_ticks(symbol)[0].last_price
                if self.g.cache['last_high'][symbol] < cur_price:
                    self.g.cache['last_high'][symbol] = cur_price
                threshold = self._get_stop_loss_threshold(symbol, self.g.param['period'][self.g.VALUE],3)
                # log.debug("个股止损阈值, stock: %s, threshold: %f" %(stock, threshold))
                if cur_price < self.g.cache['last_high'][symbol] * (1 - threshold):
                    log.info("==> 个股止损, stock: %s, cur_price: %f, last_high: %f, threshold: %f"
                             % (symbol, cur_price, self.g.cache['last_high'][symbol], threshold))
                    if self.position_close(position):
                        self.g.cache['day_count'] = 0
    
    
    def stop_profit_by_stock(self):
        '''
                        个股止盈
        minute 级别
        '''
        if self.g.param['is_stock_stop_profit'][self.g.VALUE]:
            log.info("=>开始进行个股止盈")
            for position in self.strategy.get_positions():
                symbol=position.exchange+'.'+position.sec_id 
                cur_price = self.strategy.get_last_ticks(symbol)[0].last_price
                threshold = self._get_stop_profit_threshold(
                    symbol, self.g.param['period'][self.g.VALUE],3)
                # log.debug("个股止盈阈值, stock: %s, threshold: %f" %(stock, threshold))
                if cur_price > position.avg_cost * (1 + threshold):
                    log.info("==> 个股止盈, stock: %s, cur_price: %f, avg_cost: %f, threshold: %f"
                             % (symbol, cur_price, self.g.cache['last_high'][symbol], threshold))
                    if self.position_close(position):
                        self.g.cache['day_count'] = 0
    
    
    def _get_stop_loss_threshold(self,security, n=3):
        '''
                        计算个股回撤止损阈值
                        即个股在持仓n天内能承受的最大跌幅
                        算法：(个股250天内最大的n日跌幅 + 个股250天内平均的n日跌幅)/2
                        返回正值
        '''
        pct_change = self._get_pct_change(security, 250, n)
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
    
    
    def _get_pct_change(self,symbol, n, m):
        '''
        获取个股前n天的m日增幅值序列
        增加缓存避免当日多次获取数据
        '''
        pct_change = None
        if symbol in self.g.cache['pct_change'].keys():
            pct_change = self.g.cache['pct_change'][symbol]
        else:
            dailybars=self.strategy.get_last_n_dailybars(symbol,n)
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
    
    
    def position_open(self,stock, amout):
        '''
                        开仓，买入指定价值的证券
                        报单成功并成交（包括全部成交或部分成交，此时成交量大于0），返回True
                        报单失败或者报单成功但被取消（此时成交量等于0），返回False
        '''
        tick=self.strategy.get_last_ticks(stock)
        cur_price =tick[-1].last_price
        value=int(amout/cur_price);
        value=int(value/100)
        value=int(value*100)
        openId=stock.split(".")
        order=self.strategy.open_long(openId[0],openId[1],0,value)
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
        order=self.strategy.close_long(position.exchange,position.sec_id,0,position.volume)  # 卖出可用仓位
        #if order.status> 0:
            # 只要有成交，无论全部成交还是部分成交，则统计盈亏
            #self.g.trade_stat.watch(security, order.filled,
            #                  position.avg_cost, position.price)

        #if order.status == OrderStatus.held:
            # 全部成交则删除相关证券的最高价缓存
        stock=position.exchange+'.'+position.sec_id
        if stock in self.g.cache['last_high']:
            self.g.cache['last_high'].pop(stock)
        else:
            log.warn("last high price of %s not found" % (stock))
        return True
    
    
    def position_clear(self):
        '''
                        清空卖出所有持仓
        '''
        if self.strategy.get_positions():
            log.info("==> 清仓，卖出所有股票")
            for position in self.strategy.get_positions():
                self.position_close(position)
    
    
    def position_adjust(self,buy_stocks):
        '''
                            根据待买股票创建或调整仓位
                            对于因停牌等原因没有卖出的股票则继续持有
                            始终保持持仓数目为self.g.buy_stock_count
        '''
        for position in self.strategy.get_positions():
            stock=position.exchange+'.'+position.sec_id 
            if stock not in buy_stocks:
                log.info("stock [%s] in position is not buyable" % (stock))
                self.position_close(position)
            else:
                log.info("stock [%s] is already in position" % (stock))
    
        # 根据股票数量分仓
        # 此处只根据可用金额平均分配购买，不能保证每个仓位平均分配
        position_count = len(self.strategy.get_positions())
        count = self.g.param['buy_stock_count'][self.g.VALUE]
        if count > position_count:
            cash=self.strategy.get_cash();
            value = cash.available/ (count - position_count)
            for stock in buy_stocks:
                #if context.portfolio.positions[stock].total_amount == 0:
                if self.position_open(stock, value):
                    if len(self.strategy.get_positions()) == count:
                        break
    
    
    def _order(self,security, amount):
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
        #return order(security, amount)
    
    
    def _order_target_value(self,stock, value):
        '''
                            自定义下单
                            根据Joinquant文档，当前报单函数都是阻塞执行，报单函数（如order_target_value）返回即表示报单完成
                            报单成功返回报单（不代表一定会成交），否则返回None
        '''
        if value == 0:
            log.info("Selling out %s" % (stock))
        else:
            log.info("Order %s to value %f" % (stock, value))
    
        # 如果股票停牌，创建报单会失败，order_target_value 返回None
        # 如果股票涨跌停，创建报单会成功，order_target_value 返回Order，但是报单会取消
        # 部成部撤的报单，聚宽状态是已撤，此时成交量>0，可通过成交量判断是否有成交
        #return order_target_value(security, value)
    
    #### utils ####
    
    
    def get_growth_rate(self,security, n,dt):
        '''
                        获取股票n日以来涨幅，根据当前价计算
        '''
        lc = self.get_close_price(security, n,'d',dt)
        c = self.get_close_price(security, 1, 'm',dt)
    
        if not math.isnan(lc) and not math.isnan(c) and lc != 0:
            return (c - lc) / lc
        else:
            log.error("数据非法, security: %s, %d日收盘价: %f, 当前价: %f" % 
                      (security, n, lc, c))
            return 0
    
    
    def get_close_price(self,security, n, unit,dt):
        '''
                            获取前n个单位时间当时的收盘价
        '''
        close = 0
        dtn=dt + datetime.timedelta(days=-n)
        dt1=dt + datetime.timedelta(days=-1)
        while(n > 0):  # 如果前n日数据为nan，则取n-1日数据，直至n为1
            #close = attribute_history(security, n, unit, ('close'))['close'][0]
            if unit=='d':
                #bars=self.strategy.get_dailybars(security, dt1,dtn)
                bars=self.strategy.get_last_n_dailybars(security, n)
                for bar in bars:
                    log.info('security的%s的值为%s'%(bar.strtime ,bar.close))
            elif unit=='m':
                #bars=self.strategy.get_bars(security,60, dt1,dtn)
                bars=self.strategy.get_last_n_bars(security, 60,n)
                for bar in bars:
                    log.info('security的当前值为%s'%(bar.close))
            close=bars[-1].close;  
            if math.isnan(close):
                n -= 1
            else:
                break
        return close
    
    
    def func_register(self,register, func, descr=''):
        register.append((func, descr))
    
    
    #### log ####
    
    
    def log_section(self,name, descr, sep=True, sep_char='-', sep_count=50):
        if sep:
            log.info(sep_char * sep_count)
        log.info('%s - %s' % (name, descr))
    
    
    def log_param(self):
        self.log_section('param', '策略参数')
        for key in self.g.param:
            log.info('%s : %s' % 
                     (self.g.param[key][self.g.DESCR], str(self.g.param[key][self.g.VALUE])))
    
    
    def log_filter(self):
        log.info('filter - 过滤器:')
        for filter in self.g.filter:
            log.info(' + ' + filter[self.g.DESCR])
    
    
    def log_stop_loss(self):
        log.info('stop_loss - 止损器:')
        for stop in self.g.stop_loss_minute:
            log.info(' + ' + stop[self.g.DESCR])
        for stop in self.g.stop_loss_day:
            log.info(' + ' + stop[self.g.DESCR])
    
    def get_all_symbol(self,t,s):
        list_sh=self.strategy.get_instruments('SHSE',t,s)
        list_sz=self.strategy.get_instruments('SZSE', t, s)
        list_all=list(set(list_sh).union(set(list_sz)))
        symbol_list=[]
        for instrument in list_all:
            if not instrument.symbol.split('.')[1].startswith('200') and not instrument.symbol.split('.')[1].startswith('900') :
                symbol_list.append(instrument.symbol)
        return symbol_list;  
    
