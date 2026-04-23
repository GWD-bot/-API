# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 21:09:49 2026

@author: GWD
"""

# metrics.py
import pandas as pd
import matplotlib.pyplot as plt
import plotly.graph_objects as go

def plot_equity_curve(portfolio, title="策略净值曲线", filename="equity_curve.html"):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=portfolio.wrapper.index, y=portfolio.value(), mode='lines', name='净值'))
    fig.update_layout(title=title, xaxis_title='日期', yaxis_title='资金')
    fig.write_html(filename)
    return fig

def summarize_backtest_results(results_df):
    summary = {
        '平均收益率_%': results_df['总收益率_%'].mean(),
        '中位数收益率_%': results_df['总收益率_%'].median(),
        '胜率平均值_%': results_df['胜率_%'].mean(),
        '平均夏普比率': results_df['夏普比率'].mean(),
        '平均最大回撤_%': results_df['最大回撤_%'].mean()
    }
    return pd.DataFrame([summary])

