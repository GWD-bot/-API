# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 21:03:40 2026

@author: GWD
"""

# signal_evaluator.py
import pandas as pd

def turtle_breakout_signal(df, entry_period=20, exit_period=10, atr_period=20):
    """
    返回 (entries, exits) 两个布尔 Series
    """
    df = df.copy()   
    # 计算 ATR
    df['H-L'] = df['High'] - df['Low']
    df['H-PC'] = (df['High'] - df['Close'].shift(1)).abs()
    df['L-PC'] = (df['Low'] - df['Close'].shift(1)).abs()
    df['TR'] = df[['H-L', 'H-PC', 'L-PC']].max(axis=1)
    df['ATR'] = df['TR'].rolling(window=atr_period).mean()
    
    # 昨日通道高点/低点
    entry_high = df['High'].rolling(window=entry_period, min_periods=entry_period).max().shift(1)
    exit_low   = df['Low'].rolling(window=exit_period, min_periods=exit_period).min().shift(1)
    
    entries = (df['Close'] > entry_high) & (entry_high.notna())
    exits   = (df['Close'] < exit_low) & (exit_low.notna())
    
    # 可选的额外止损：如果价格低于入场价 - 2*ATR（需要跟踪入场价，这里略）
    
    return entries, exits
    
    # 昨日通道高点/低点（避免未来函数）
    entry_high = df['High'].rolling(window=entry_period, min_periods=entry_period).max().shift(1)
    exit_low   = df['Low'].rolling(window=exit_period, min_periods=exit_period).min().shift(1)
    
    # 买入信号：收盘价突破昨日通道高点
    entries = (df['Close'] > entry_high) & (entry_high.notna())
    # 卖出信号：收盘价跌破昨日通道低点
    exits   = (df['Close'] < exit_low) & (exit_low.notna())
    
    return entries, exits

# 为了兼容您现有的框架，分别提供买入函数和卖出函数
def turtle_buy_signal(df):
    entries, _ = turtle_breakout_signal(df)
    return entries

def turtle_sell_signal(df, entries):
    _, exits = turtle_breakout_signal(df)
    return exits
def generate_macd_signal(df):
    close = df['Close']
    exp1 = close.ewm(span=12, adjust=False).mean()
    exp2 = close.ewm(span=26, adjust=False).mean()
    dif = exp1 - exp2
    dea = dif.ewm(span=9, adjust=False).mean()
    return (dif > dea) & (dif.shift(1) <= dea.shift(1))

def generate_ma_golden_cross_signal(df, short=26, long=55):
    close = df['Close']
    ma_short = close.rolling(window=short).mean()
    ma_long = close.rolling(window=long).mean()
    return (ma_short > ma_long) & (ma_short.shift(1) <= ma_long.shift(1))

def generate_sell_on_death_cross(df, entries, short=26, long=55):
    """死叉卖出：短期均线下穿长期均线时发出卖出信号（且当日有持仓）"""
    short_ma = df['Close'].rolling(window=short).mean()
    long_ma = df['Close'].rolling(window=long).mean()
    # 死叉信号：上一天短期均线 >= 长期均线，今天 < 长期均线
    death_cross = (short_ma.shift(1) >= long_ma.shift(1)) & (short_ma < long_ma)
    # 只在有持仓的日子里才允许卖出（即 entries 之后，且未平仓）
    # 为了简化，我们允许任何死叉日卖出，但 portfolio 会自动处理空仓时无效
    return death_cross

def generate_rsi_signal(df, period=14, oversold=30):
    close = df['Close']
   
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
# 返回最新值是否超卖
    return rsi.iloc[-1] < oversold if not pd.isna(rsi.iloc[-1]) else False

def combined_signal_macd_and_ma(df):
    macd_entry = generate_macd_signal(df)
    ma_bullish = (df['Close'] > df['Close'].rolling(20).mean()) & \
                 (df['Close'].rolling(5).mean() > df['Close'].rolling(10).mean())
    return macd_entry & ma_bullish
# ... 其他信号
