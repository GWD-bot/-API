# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 21:50:13 2026

@author: GWD
"""

# -*- coding: utf-8 -*-
"""

每日股票监测系统：从本地数据库读取最新交易日全量数据，应用多规则筛选和技术指标，生成组合信号并发送邮件报告。

"""

import os
import json
import smtplib
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders



# ====================== 固定配置 ======================
LOG_FILE = "stock_fetch.log"                     # 日志文件名

# ====================== 从JSON加载配置 ======================
with open('config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

# 邮件配置
MAIL_CONFIG = config['mail']
SMTP_SERVER = MAIL_CONFIG['smtp_server']
SMTP_PORT = MAIL_CONFIG['smtp_port']
SENDER_EMAIL = MAIL_CONFIG['sender_email']
SENDER_PASSWORD = MAIL_CONFIG['sender_password']
RECEIVER_EMAIL = MAIL_CONFIG['receiver_email']


# 在加载 config 后添加
SAVE_DAILY_FILES = config.get('save_daily_files', True)   # 默认 True 保持兼容


# 筛选规则列表
with open('rules.json', 'r', encoding='utf-8') as f:
    RULES = json.load(f)



# ====================== 工具函数 ======================
def log_message(msg):
    """将消息写入日志文件并打印（带时间戳）"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {msg}\n")
    print(f"[{timestamp}] {msg}")



def apply_rule(df, rule):
    """
    根据单条规则筛选 DataFrame
    :param df: 原始数据（中文列名）
    :param rule: 规则字典，包含 name, col, operator, threshold, file_prefix 等
    :return: 筛选后的 DataFrame，若无数据则返回 None
    """
    col = rule["col"]
    if col not in df.columns:
        log_message(f"警告：列 {col} 不存在，跳过规则 {rule['name']}")
        return None
    op = rule["operator"]
    if op == "gt":
        return df[df[col] > rule["threshold"]]
    elif op == "lt":
        return df[df[col] < rule["threshold"]]
    elif op == "bottom":
        n = rule.get("limit", 10)
        return df.nsmallest(n, col)
    elif op == "top":
        n = rule.get("limit", 10)
        return df.nlargest(n, col)    
    else:
        log_message(f"未知操作符 {op}")
        return None

def save_to_db(rule_name, df, date_str, value_col):
    """
    将筛选结果存入 SQLite 数据库（表 daily_hits）
    """
    if df.empty:
        log_message(f"规则 {rule_name} 无数据，跳过入库")
        return
    try:
        conn = sqlite3.connect('monitor.db')
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_hits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                "规则名称" TEXT,
                "股票代码" TEXT,
                "股票名称" TEXT,
                "数值" REAL,
                "日期" TEXT
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_hits_rule_date ON daily_hits("规则名称", "日期")')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_hits_code ON daily_hits("股票代码")')

        # 删除同规则、同日期旧数据
        cursor.execute('DELETE FROM daily_hits WHERE "规则名称" = ? AND "日期" = ?', (rule_name, date_str))
        log_message(f"已删除 daily_hits 中规则 '{rule_name}' 日期为 {date_str} 的旧数据")

        data = [(rule_name, row['股票代码'], row['股票名称'], row[value_col], date_str) 
                for _, row in df.iterrows()]
        cursor.executemany('''
            INSERT INTO daily_hits ("规则名称", "股票代码", "股票名称", "数值", "日期")
            VALUES (?, ?, ?, ?, ?)
        ''', data)

        conn.commit()
        conn.close()
        log_message(f"已将 {len(df)} 条记录存入数据库（规则：{rule_name}）")
    except Exception as e:
        log_message(f"❌ 数据库写入失败（规则：{rule_name}）：{e}")

def generate_histogram(df, date_str):
    """生成涨跌幅分布直方图，返回图片文件名"""
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    plt.figure(figsize=(10,6))
    plt.hist(df['涨跌幅'].dropna(), bins=50, color='skyblue', edgecolor='black')
    plt.title(f'{date_str} A股涨跌幅分布')
    plt.xlabel('涨跌幅(%)')
    plt.ylabel('股票数量')
    filename = f'hist_{date_str}.png'
    plt.savefig(filename)
    plt.close()
    return filename

def send_email(subject, body, attachments=[]):
    """
    发送邮件（支持HTML正文和附件）
    """
    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECEIVER_EMAIL
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'html', 'utf-8'))

    for file_path in attachments:
        if os.path.isfile(file_path):
            with open(file_path, 'rb') as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(file_path)}"')
                msg.attach(part)

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.send_message(msg)
        server.quit()
        log_message(f"邮件发送成功至 {RECEIVER_EMAIL}")
    except Exception as e:
        log_message(f"邮件发送失败：{e}")

def process_single_date(df_day, date_str):
    """
    处理单个交易日的数据：全量入库、过滤、规则筛选、邮件发送
    """
    log_message(f"========== 开始处理日期 {date_str} ==========")


    # 3. 根据配置决定是否生成直方图
    hist_file = None
    if SAVE_DAILY_FILES:
        hist_file = generate_histogram(df_day, date_str)
    # 否则 hist_file 保持 None


    # 4. 股票过滤（排除ST、科创板、8/9开头）
    df_day['股票代码'] = df_day['股票代码'].astype(str)
    st_mask = df_day['股票名称'].str.contains('ST|\\*ST', na=False, regex=True)
    kcb_mask = df_day['股票代码'].str.startswith('688')
    eight_nine_mask = df_day['股票代码'].str.startswith(('8', '9'))
    exclude_mask = st_mask | kcb_mask | eight_nine_mask
    filtered_df = df_day[~exclude_mask].copy()
    # 新增：排除流通市值 <= 0 的股票（避免零值干扰排序）
    filtered_df = filtered_df[filtered_df['流通市值'] > 0]
    
    # 新增：排除停牌股票（可根据实际数据特征选择判断条件）
    # 方法一：成交量 <= 0 视为停牌（常见处理）
    filtered_df = filtered_df[filtered_df['成交量'] > 0]
    log_message(f"日期 {date_str} 过滤后 {len(filtered_df)} 条")
    # ========== 新增：记录每只股票触发的信号 ==========
    stock_signals = {}  # key: 股票代码, value: {'name': 股票名称, 'signals': set()}

    # 初始化所有股票的信号集合为空
    for _, row in filtered_df.iterrows():
        code = row['股票代码']
        name = row['股票名称']
        stock_signals[code] = {'name': name, 'signals': set()}


    # 5. 应用规则筛选（只记录信号，不在此入库）
    all_reports = []
    for rule in RULES:
        # 仅处理 type == 'filter' 的规则（指标规则稍后统一计算）
        if rule.get('type') == 'filter':
            result_df = apply_rule(filtered_df, rule)
            if result_df is not None and not result_df.empty:
                count = len(result_df)
                # 记录信号
                for _, row in result_df.iterrows():
                    code = row['股票代码']
                    stock_signals[code]['signals'].add(rule['name'])
                # 不再调用 save_to_db，因为其他模块已负责入库
                sample = result_df[['股票代码', '股票名称', rule['col']]].head(10)
                all_reports.append({
                    "name": rule['name'],
                    "count": count,
                    "sample": sample
                })
            else:
                log_message(f"规则 {rule['name']} 未筛选出数据")
                 
    # ========== 新增：调用技术指标计算函数，补充指标信号 ==========
    # 你需要自己实现 get_technical_signals_for_stock(code, date_str) 函数
    # 该函数返回该股票触发的指标信号名称列表（如 ['26/55均线金叉', 'MACD金叉', 'RSI超卖']）
    # 注意：为避免重复请求，可以在这里批量获取历史数据并缓存
    indicator_hits = {}  # {规则名称: [{'股票代码':..., '股票名称':..., '数值':...}]}
    
    for code, info in stock_signals.items():
        indicator_signals = get_technical_signals_for_stock(code, date_str)
        if indicator_signals:
            stock_row = filtered_df[filtered_df['股票代码'] == code]
            if not stock_row.empty:
                for signal_name in indicator_signals:
                    # 仅将需要入库的信号加入 indicator_hits
                    if signal_name in ['26/55均线金叉']:  # 需要入库的信号列表
                        if signal_name not in indicator_hits:
                            indicator_hits[signal_name] = []
                        value = 1.0
                        indicator_hits[signal_name].append({
                            '股票代码': code,
                            '股票名称': info['name'],
                            '数值': value
                        })
                    # 无论是否入库，都更新 stock_signals 中的信号集合
                    info['signals'].add(signal_name)
            else:
                # 如果没有数据行，仍然要更新信号集合（但这种情况极少）
                info['signals'].update(indicator_signals)
    
    # 批量入库指标信号
    for signal_name, hit_list in indicator_hits.items():
        if hit_list:
            temp_df = pd.DataFrame(hit_list)
            save_to_db(signal_name, temp_df, date_str, '数值')
            log_message(f"指标信号 {signal_name} 入库 {len(hit_list)} 只股票")
    # ========== 指标信号补充结束 ==========

    # ========== 新增：组合规则筛选 ==========
    # 加载组合规则配置
    combos = []
    try:
        with open('combinations.json', 'r', encoding='utf-8') as f:
            combos = json.load(f)
    except FileNotFoundError:
        log_message("未找到 combinations.json，跳过组合信号筛选")
    except Exception as e:
        log_message(f"加载组合规则失败：{e}")

    combination_reports = []
    for combo in combos:
        required_signals = set(combo['signals'])
        hit_codes = []
        for code, info in stock_signals.items():
            if required_signals.issubset(info['signals']):
                hit_codes.append({'股票代码': code, '股票名称': info['name']})
        if hit_codes:
            combo_df = pd.DataFrame(hit_codes)
            combo_filename = f"组合信号_{combo['name'].replace('%','pct').replace('>','gt').replace('<','lt').replace('/','_').replace('\\','_').replace(':','_').replace('*','_').replace('?','_').replace('"','_').replace('|','_')}_{date_str}.xlsx"
            combo_df.to_excel(combo_filename, index=False)
            combination_reports.append({
                "name": combo['name'],
                "description": combo.get('description', ''),
                "count": len(hit_codes),
                "filename": combo_filename,
                "df": combo_df
            })
            log_message(f"组合规则 {combo['name']} 命中 {len(hit_codes)} 只股票")
        else:
            log_message(f"组合规则 {combo['name']} 未命中任何股票")
    # ========== 组合筛选结束 ==========

    # 7. 发送日报邮件（如果有筛选结果）
    attachments = []
    if SAVE_DAILY_FILES and hist_file and os.path.isfile(hist_file):
        attachments.append(hist_file)
    
    body = f"<h2>{date_str} 股票监测日报</h2>\n"
    
    if combination_reports:
        body += "<p><b>组合信号（多指标共振）：</b></p>\n"
        for rep in combination_reports:
            body += f"<h3>{rep['name']}（{rep['description']}）共{rep['count']}只</h3>\n"
            body += rep['df'].head(10).to_html(index=False)
            body += "<hr>\n"
            attachments.append(rep['filename'])
        send_email(subject=f"股票组合信号日报 {date_str}", body=body, attachments=attachments)
    elif all_reports:
        body += "<p><b>单规则筛选结果：</b></p>\n"
        for rep in all_reports:
            body += f"<h3>{rep['name']}（共{rep['count']}只）</h3>\n"
            body += rep['sample'].to_html(index=False)
            body += "<hr>\n"
        send_email(subject=f"股票监测日报 {date_str}", body=body, attachments=attachments)
    else:
        log_message("今日无任何规则命中数据，不发送邮件。")
def get_technical_signals_for_stock(code, date_str):
    signals = []
    conn = sqlite3.connect('monitor.db')
    query = """
        SELECT "交易日期", "收盘价", "涨跌幅", "最高价", "最低价"
        FROM daily_full_snapshot
        WHERE "股票代码" = ? AND "交易日期" <= ?
        ORDER BY "交易日期" DESC LIMIT 60
    """
    df = pd.read_sql_query(query, conn, params=(code, date_str))
    conn.close()
    
    # 至少需要20条数据才能计算均线多头排列
    if len(df) < 20:
        return signals
    
    df = df.sort_values('交易日期')  # 升序
    close = df['收盘价'].astype(float)
    pct_chg = df['涨跌幅'].astype(float)
    high = df['最高价'].astype(float)
    low = df['最低价'].astype(float)
    dates = df['交易日期'].tolist()  # 日期列表，按升序
    
    # 1. 均线多头排列 (5,10,20) 且收盘价站上20日均线
    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma20 = close.rolling(20).mean()
    if not pd.isna(ma20.iloc[-1]) and ma5.iloc[-1] > ma10.iloc[-1] > ma20.iloc[-1] and close.iloc[-1] > ma20.iloc[-1]:
        signals.append('均线多头排列')
    
    # 2. 涨停检测函数
    def is_limit_up(pct, code_str):
        if code_str.startswith(('30', '68')):
            return pct >= 19.5   # 创业板/科创板
        else:
            return pct >= 9.8    # 主板
    
    # 参数配置
    max_callback_days = 3      # 允许涨停后最多回调3个交易日
    exclude_today = True       # 排除涨停当天（必须间隔至少1天）
    
    # 获取最近5个交易日的数据（含当天）
    recent = df.tail(5).copy()
    limit_candidates = []
    for idx, row in recent.iterrows():
        if is_limit_up(row['涨跌幅'], code):
            limit_candidates.append(idx)
    
    if limit_candidates:
        # 取最近的一个涨停日（日期最大）
        latest_limit_idx = limit_candidates[-1]
        limit_low = df.loc[latest_limit_idx, '最低价']
        limit_high = df.loc[latest_limit_idx, '最高价']
        current_close = close.iloc[-1]
        
        # 计算涨停日到今天的天数间隔（交易日间隔）
        limit_date = df.loc[latest_limit_idx, '交易日期']
        current_date = dates[-1]
        # 使用日期列表计算位置差
        limit_pos = dates.index(limit_date)
        today_pos = len(dates) - 1
        days_gap = today_pos - limit_pos
        
        condition = True
        if exclude_today and days_gap == 0:
            condition = False
        if max_callback_days is not None and days_gap > max_callback_days:
            condition = False
        
        if condition and (limit_low <= current_close <= limit_high):
            signals.append('涨停后回调不破')
    
    # 3. 26/55均线金叉（需要至少55条数据）
    if len(df) >= 55:
        ma26 = close.rolling(26).mean()
        ma55 = close.rolling(55).mean()
        if len(ma26) >= 2 and len(ma55) >= 2:
            if ma26.iloc[-2] < ma55.iloc[-2] and ma26.iloc[-1] > ma55.iloc[-1]:
                signals.append('26/55均线金叉')
    
    # 后续可添加 MACD、RSI 等
    # 4. MACD 金叉（12,26,9）
    if len(df) >= 35:  # 需要足够数据
        # 计算 MACD
        exp1 = close.ewm(span=12, adjust=False).mean()
        exp2 = close.ewm(span=26, adjust=False).mean()
        dif = exp1 - exp2
        dea = dif.ewm(span=9, adjust=False).mean()
        macd = 2 * (dif - dea)
        # 金叉：DIF 上穿 DEA
        if len(dif) >= 2 and len(dea) >= 2:
            if dif.iloc[-2] < dea.iloc[-2] and dif.iloc[-1] > dea.iloc[-1]:
                signals.append('MACD金叉')
    
    # 5. RSI 超卖（14天，<30）
    if len(df) >= 15:
        delta = close.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        if not pd.isna(rsi.iloc[-1]) and rsi.iloc[-1] < 30:
            signals.append('RSI超卖')
    return signals

def main():
    log_message("=== 脚本开始运行 ===")
    conn = sqlite3.connect('monitor.db')
    # 查询 daily_full_snapshot 中所有不重复的交易日期，排序
    trade_dates = pd.read_sql_query("SELECT DISTINCT \"交易日期\" FROM daily_full_snapshot ORDER BY \"交易日期\"", conn)['交易日期'].tolist()
    conn.close()
    if not trade_dates:
        log_message("数据库中没有全量数据，请先运行数据维护脚本")
        return
    # 可选：只处理最新一个交易日，或处理所有未处理的日期（可以记录已发送邮件的日期）
    # 简单起见，只处理最后一个交易日
    latest_date = trade_dates[-1]
    log_message(f"将处理最新交易日：{latest_date}")
    # 从数据库读取该日期的全量数据
    conn = sqlite3.connect('monitor.db')
    df_day = pd.read_sql_query(f"SELECT * FROM daily_full_snapshot WHERE \"交易日期\" = '{latest_date}'", conn)
    conn.close()
    if df_day.empty:
        log_message(f"日期 {latest_date} 无数据")
        return
    process_single_date(df_day, latest_date)
    log_message("=== 脚本运行结束 ===")

if __name__ == "__main__":
    main()