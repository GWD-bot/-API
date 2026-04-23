# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 06:40:15 2026

@author: GWD
"""

import sqlite3
import pandas as pd

db_path = r'C:\Users\GWD\monitor.db'
conn = sqlite3.connect(db_path)

# 查看表中有哪些股票代码示例
query = "SELECT DISTINCT 股票代码 FROM daily_full_snapshot LIMIT 10"
df_codes = pd.read_sql_query(query, conn)
print("数据库中的股票代码示例：")
print(df_codes)

# 尝试查询你使用的代码
code_to_check = '600036.SH'  # 改成你刚才用的
query2 = f"SELECT COUNT(*) as cnt FROM daily_full_snapshot WHERE 股票代码 = '{code_to_check}'"
cnt = pd.read_sql_query(query2, conn)
print(f"\n代码 {code_to_check} 的数据条数：{cnt.iloc[0,0]}")

conn.close()