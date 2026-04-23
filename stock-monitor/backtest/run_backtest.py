# -*- coding: utf-8 -*-
"""
Created on Tue Apr 21 05:38:04 2026

@author: GWD
"""

# -*- coding: utf-8 -*-
"""
批量回测入口：支持多股票、多信号，输出汇总报告
"""

import json
import pandas as pd
from backtest_engine import batch_backtest
from signal_evaluator import (
    generate_macd_signal,
    generate_ma_golden_cross_signal,
    combined_signal_macd_and_ma,
    generate_sell_on_death_cross,
    turtle_buy_signal,        # 新增
    turtle_sell_signal  
)
from metrics import plot_equity_curve, summarize_backtest_results
import re

# 信号配置：buy 为买入信号函数，exit 为卖出函数（None 表示使用默认固定持有期）
SIGNAL_FUNCS = {
    "MACD金叉": {
        "buy": generate_macd_signal,
        "exit": None
    },
    "26/55均线金叉": {
        "buy": lambda df: generate_ma_golden_cross_signal(df, 26, 55),
        "exit": generate_sell_on_death_cross   # 只对这个信号使用死叉卖出
    },
    "组合_MACD+均线多头": {
        "buy": combined_signal_macd_and_ma,
        "exit": None
    },
    "海龟交易法": {                      # 新增
        "buy": turtle_buy_signal,
        "exit": turtle_sell_signal
    }
}

def main():
    # 读取配置
    with open('config_backtest.json', 'r', encoding='utf-8') as f:
        config = json.load(f)

    stock_pool = config['stock_pool']
    start = config['start_date']
    end = config['end_date']
    hold_days = config.get('hold_days', 5)
    signals_to_test = config.get('signals_to_test', list(SIGNAL_FUNCS.keys()))

    all_results = []
    for signal_name in signals_to_test:
        # 1. 先获取配置字典
        signal_cfg = SIGNAL_FUNCS[signal_name]
        # 2. 取出买入函数和卖出函数
        signal_func = signal_cfg["buy"]
        exit_func = signal_cfg.get("exit")   # 没有则返回 None
        print(f"\n===== 批量回测信号：{signal_name} =====")
        df_res, portfolios = batch_backtest(stock_pool, start, end, signal_func, exit_func=exit_func, hold_days=hold_days)
        if not df_res.empty:
            df_res.insert(0, '信号名称', signal_name)   # 添加信号名称列
            all_results.append(df_res)
            '''
            # 为每个股票生成权益曲线（可选）
            for code, port in portfolios.items():
                # 清理信号名称中的非法字符
                safe_signal_name = signal_name.replace('/', '_').replace('\\', '_')
                filename = f"equity_{code}_{safe_signal_name}.html"
                plot_equity_curve(port, title=f"{code} {signal_name} 净值曲线", filename=filename)
            '''    
    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)       
        final_df.to_csv("backtest_summary.csv", index=False, encoding='utf-8-sig')
        print("\n回测完成，结果保存至 backtest_summary.csv")
        print(final_df)
        
        # ========== 新增：按股票代码分别保存，每个文件包含所有信号 ==========
        # 定义要输出的指标顺序（可根据需要调整）
        indicator_order = [            
            '回测周期_天',
            '起始资金',
            '最终资金',
            '总收益率_%',
            '基准回报率_%',
            '最大总敞口_%',
            '总费用',
            '最大回撤_%',
            '最大回撤持续时间_天',
            '总交易次数',
            '已平仓交易总次数',          # 新增
            '胜率_%',
            '最佳交易_%',
            '最差交易_%',
            '平均盈利收益率_%',
            '平均亏损收益率_%',
            '平均获胜交易持续时间_天',   # 新增
            '平均亏损交易持续时间_天',   # 新增
            '利润因子',
            '期望值',
            '夏普比率',
            '卡尔玛比率',
            '欧米茄比率',
            '索提诺比率'
        ]
    
        # 确保所有指标列都存在
        for col in indicator_order:
            if col not in final_df.columns:
                final_df[col] = None
    
        # 按股票代码分组
        for code, group in final_df.groupby('股票代码'):
            # group 包含该股票所有信号的行（每个信号一行）
            # 重置索引，以便后续操作
            group = group.reset_index(drop=True)
            
            # 方式一：横向布局（指标为行，信号为列）
            # 创建 DataFrame，索引为指标名称，列为信号名称
            signals = group['信号名称'].tolist()
            # 初始化一个字典，每个指标对应一个列表（按信号顺序）
            data = {}
            for indicator in indicator_order:
                # 获取该指标在所有信号下的值
                values = []
                for _, row in group.iterrows():
                    val = row[indicator]
                    # 如果是浮点数，保留4位小数
                    if isinstance(val, float):
                        val = round(val, 2)
                    values.append(val)
                data[indicator] = values
            df_stock = pd.DataFrame(data, index=signals).T   # 转置：指标为行，信号为列
            df_stock.index.name = '指标名称'
            df_stock.columns.name = '信号名称'
            
            # 保存到单独文件（清理股票代码中的特殊字符，如 '.SH' 保留）
            safe_code = code.replace('/', '_').replace('\\', '_')
            filename = f"{safe_code}_performance.csv"
            df_stock.to_csv(filename, encoding='utf-8-sig')
            print(f"已保存股票 {code} 的绩效汇总至 {filename}")
            
            # 可选：同时打印控制台
            print(f"\n股票代码：{code}")
            print(df_stock)
            print("-" * 60)
    
      
 
        # 生成汇总统计
        summary = summarize_backtest_results(final_df)
        summary.to_csv("backtest_summary_stats.csv", index=False)
        print("\n整体绩效汇总：")
        print(summary)
    else:
        print("没有成功回测的结果")

if __name__ == "__main__":
    main()