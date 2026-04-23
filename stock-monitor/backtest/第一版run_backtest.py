# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 23:38:05 2026

@author: GWD
 # 假设查询到的一个代码是 '000001.SZ'
 code = '000001.SZ'
 start = '2000-01-01'
 end = '2026-04-21'
 stats = backtest_signal_on_single_stock(code, start, end, generate_ma_golden_cross_signal)
"""

from backtest_engine import get_hist_data_from_db, backtest_signal_on_single_stock
from signal_evaluator import generate_macd_signal,generate_ma_golden_cross_signal, turtle_buy_signal,turtle_sell_signal
import json
import pandas as pd


with open('config_backtest.json', 'r', encoding='utf-8') as f:
    config = json.load(f)
   
codes = config['stock_pool']
start = config['start_date']
end = config['end_date']
hold_days = config.get('hold_days', 5)
# 定义中英文对照字典
name_map = {
    'Start': '开始日期',
    'End': '结束日期',
    'Period': '回测周期',
    'Start Value': '起始价值',
    'End Value': '最终价值',
    'Total Return [%]': '总回报率',
    'Benchmark Return [%]': '基准回报率',
    'Max Gross Exposure [%]': '最大总敞口',
    'Total Fees Paid': '总费用',
    'Max Drawdown [%]': '最大回撤',
    'Max Drawdown Duration': '最大回撤时间',
    'Total Trades': '总交易次数',
    'Total Closed Trades': '交易总次数',
    'Total Open Trades': '交易总次数',
    'Open Trade PnL': '交易盈亏',
    'Win Rate [%]': '胜率',
    'Best Trade [%]': '最佳交易',
    'Worst Trade [%]': '最差交易',
    'Avg Winning Trade [%]': '平均胜收益率',
    'Avg Losing Trade [%]': '平均亏收益率',
    'Avg Winning Trade Duration': '平均胜时间',
    'Avg Losing Trade Duration': '平均亏时间',
    'Profit Factor': '利润因子',
    'Expectancy': '期望值',
    'Sharpe Ratio': '夏普比率',
    'Calmar Ratio': '卡尔玛比率',
    'Omega Ratio': '欧米茄比率',
    'Sortino Ratio': '索提诺比率'
}
for code in codes:
    # 使用关键字参数，避免位置错位
    port = backtest_signal_on_single_stock(
        code, start, end,
        signal_func=turtle_buy_signal,
        exit_func=turtle_sell_signal,
        hold_days=hold_days
    )

    if port is not None:
        stats = port.stats()
        #print(stats)
        # 构建新索引：中文 (英文)
        new_index = [f"{name_map.get(idx, idx)} ({idx})" for idx in stats.index]
        stats_cn = stats.copy()
        stats_cn.index = new_index
        with pd.option_context('display.float_format', '{:.4f}'.format):
            print(stats_cn)
        print("\n" + "="*60 + "\n")
    
    else:
        print("回测失败，请检查代码或数据")
    
    
    
   
    
    