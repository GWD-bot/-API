# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 21:02:41 2026

@author: GWD
"""
# backtest_engine.py 示例开头
import pandas as pd
import sqlite3
import vectorbt as vbt
from datetime import datetime
import os

def get_hist_data_from_db(code, start_date, end_date):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(base_dir, 'monitor.db')
    # 使用绝对路径（注意：Windows路径中的反斜杠建议用正斜杠或双反斜杠）
    #db_path = r'C:\Users\GWD\monitor.db'
    #相对路径
    #db_path = os.path.join(os.path.dirname(__file__), 'monitor.db')
    conn = sqlite3.connect(db_path)
    #conn = sqlite3.connect(r'C:\Users\GWD\monitor.db')
    # 将传入的日期字符串（如 '2024-01-01'）转换为整数格式（如 20240101）
    start_int = int(start_date.replace('-', ''))
    end_int = int(end_date.replace('-', ''))
    query = """
        SELECT "交易日期", "开盘价", "最高价", "最低价", "收盘价", "成交量", "涨跌幅"
        FROM daily_full_snapshot
        WHERE "股票代码" = ? AND "交易日期" BETWEEN ? AND ?
        ORDER BY "交易日期"
    """
    df = pd.read_sql_query(query, conn, params=(code, start_int, end_int))
    conn.close()
    if df.empty:
        return None
    df['交易日期'] = pd.to_datetime(df['交易日期'], format='%Y%m%d')  # 转为 datetime 索引
    df = df.set_index('交易日期').sort_index()
    return df.rename(columns={
        '开盘价': 'Open', '最高价': 'High', '最低价': 'Low',
        '收盘价': 'Close', '成交量': 'Volume'
    })

def generate_sell_after_n_days(entries, hold_days=5):
    """
    根据买入信号生成固定持有期后的卖出信号。
    entries: 布尔序列，True 表示买入日
    hold_days: 持有天数（交易日）
    返回: 布尔序列，True 表示卖出日
    """
    # 找出所有买入位置
    buy_dates = entries[entries].index
    sell_series = pd.Series(False, index=entries.index)
    for date in buy_dates:
        # 找到买入日之后第 hold_days 个交易日的位置
        idx_pos = entries.index.get_loc(date)
        sell_idx = idx_pos + hold_days
        if sell_idx < len(entries):
            sell_series.iloc[sell_idx] = True
    # 如果有重叠的卖出信号，保持为 True 即可
    return sell_series


def backtest_signal_on_single_stock(code, start_date, end_date, signal_func, exit_func=None, hold_days=5):
    print(f"正在回测 {code} ...")
    df = get_hist_data_from_db(code, start_date, end_date)
    if df is None:
        print(f"get_hist_data_from_db 返回 None for {code}")
        return None
    print(f"获取到 {len(df)} 条数据")
    if len(df) < 55:
        print(f"数据不足55条，实际 {len(df)}")
        return None

    close = df['Close']
    entries = signal_func(df)
    print(f"买入信号数量: {entries.sum()}")
    if entries.sum() == 0:
        print("无买入信号，跳过")
        return None
    if exit_func is None:
        exits = generate_sell_after_n_days(entries, hold_days=hold_days)
    else:
        # 自定义卖出函数需要接收 df 和 entries，返回布尔 Series
        exits = exit_func(df, entries)
    #exits = generate_sell_after_n_days(entries, hold_days=hold_days)   # 使用传入的 hold_days
    print(f"卖出信号数量: {exits.sum()}")

    portfolio = vbt.Portfolio.from_signals(
        close, entries, exits,
        init_cash=100000,
        fees=0.001,
        freq='D'
        
    )
    #return portfolio.stats()
    return portfolio   # 返回 portfolio 对象，而不是 stats
def batch_backtest(codes, start_date, end_date, signal_func, exit_func=None, hold_days=5):
    """批量回测并汇总绩效（返回DataFrame和portfolios字典）"""
    results = []
    portfolios = {}
    for code in codes:
        portfolio = backtest_signal_on_single_stock(code, start_date, end_date, signal_func, exit_func, hold_days)
        if portfolio is not None:
            stats = portfolio.stats()
            results.append({
                '股票代码': code,
                '开始日期': stats.get('Start', ''),
                '结束日期': stats.get('End', ''),
                '回测周期_天': stats.get('Period', pd.Timedelta(0)).days,
                '起始资金': stats.get('Start Value', 100000),
                '最终资金': stats.get('End Value', 0),
                '总收益率_%': stats.get('Total Return [%]', 0),
                '基准回报率_%': stats.get('Benchmark Return [%]', 0),
                '最大总敞口_%': stats.get('Max Gross Exposure [%]', 0),
                '总费用': stats.get('Total Fees Paid', 0),
                '最大回撤_%': stats.get('Max Drawdown [%]', 0),
                '最大回撤持续时间_天': stats.get('Max Drawdown Duration', pd.Timedelta(0)).days,
                '总交易次数': stats.get('Total Trades', 0),
                '已平仓交易总次数': stats.get('Total Closed Trades', 0),          # 新增
                '胜率_%': stats.get('Win Rate [%]', 0),
                '最佳交易_%': stats.get('Best Trade [%]', 0),
                '最差交易_%': stats.get('Worst Trade [%]', 0),
                '平均盈利收益率_%': stats.get('Avg Winning Trade [%]', 0),
                '平均亏损收益率_%': stats.get('Avg Losing Trade [%]', 0),
                '平均获胜交易持续时间_天': stats.get('Avg Winning Trade Duration', pd.Timedelta(0)).days,   # 新增
                '平均亏损交易持续时间_天': stats.get('Avg Losing Trade Duration', pd.Timedelta(0)).days,   # 新增
                '利润因子': stats.get('Profit Factor', 0),
                '期望值': stats.get('Expectancy', 0),
                '夏普比率': stats.get('Sharpe Ratio', 0),
                '卡尔玛比率': stats.get('Calmar Ratio', 0),
                '欧米茄比率': stats.get('Omega Ratio', 0),
                '索提诺比率': stats.get('Sortino Ratio', 0),
            })
            portfolios[code] = portfolio   # 保存 portfolio 对象供绘图
    return pd.DataFrame(results), portfolios
