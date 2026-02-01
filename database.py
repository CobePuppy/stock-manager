import sqlite3
import pandas as pd
import config
from datetime import datetime, timedelta
import os

def get_history_connection():
    return sqlite3.connect(config.HISTORY_DB_PATH)

def get_connection():
    return sqlite3.connect(config.DB_PATH)

def init_db():
    conn = get_connection()
    c = conn.cursor()
    
    # Watchlist table
    c.execute('''CREATE TABLE IF NOT EXISTS watchlist (
                    stock_code TEXT PRIMARY KEY,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )''')
    
    # Fund flow cache table
    c.execute('''CREATE TABLE IF NOT EXISTS fund_flow_cache (
                    stock_code TEXT,
                    period_type TEXT,
                    cache_date TEXT,
                    "股票简称" TEXT,
                    "最新价" REAL,
                    "涨跌幅" REAL,
                    "换手率" REAL,
                    "净额" REAL,
                    "成交额" REAL,
                    "资金流入净额" REAL,
                    "增仓占比" REAL,
                    "流通市值" REAL,
                    -- 其他列会自动添加，但最好只存关键列或允许动态列
                    PRIMARY KEY (stock_code, period_type, cache_date)
                )''')

    # Daily Top Stocks History table (NEW)
    # 用于保存每天的前20名股票代码，用于后续回测
    c.execute('''CREATE TABLE IF NOT EXISTS daily_top_history (
                    record_date TEXT,
                    stock_code TEXT,
                    stock_name TEXT,
                    rank INTEGER,
                    period_type TEXT,
                    saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (record_date, stock_code, period_type)
                )''')
                
    conn.commit()
    conn.close()
    
    # Init History DB
    init_history_db()

def init_all_dbs():
    """初始化所有数据库"""
    init_db()
    # init_history_db is called within init_db, but we can be explicit if needed
    # But currently init_db() calls it at the end.

def init_history_db():
    conn = get_history_connection()
    c = conn.cursor()
    # 历史每日明细数据 (Backtest Data)
    # 记录每个曾入榜股票的每日数据
    c.execute('''CREATE TABLE IF NOT EXISTS daily_stock_data (
                    trade_date TEXT,
                    stock_code TEXT,
                    stock_name TEXT,
                    close_price REAL,
                    change_pct REAL,
                    net_inflow REAL,
                    ratio REAL,
                    turnover REAL,
                    market_cap REAL,
                    PRIMARY KEY (trade_date, stock_code)
                )''')
    conn.commit()
    conn.close()

def get_all_tracked_stocks():
    """获取所有在历史Top榜单中出现过的股票代码"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT stock_code FROM daily_top_history")
        codes = [row[0] for row in cursor.fetchall()]
        return codes
    except Exception as e:
        print(f"获取历史Top股票列表失败: {e}")
        return []
    finally:
        conn.close()

def save_daily_data_for_backtest(df_filtered):
    """保存筛选后的股票当日数据到历史回测数据库"""
    if df_filtered.empty:
        return

    conn = get_history_connection()
    trade_date = get_stock_trade_date()
    
    try:
        data_to_save = []
        for _, row in df_filtered.iterrows():
            code = str(row['股票代码'])
            # 兼容列名
            name = row.get('股票简称', row.get('股票名称', ''))
            price = row.get('最新价', 0)
            change = row.get('涨跌幅', 0)
            net = row.get('净额', 0)
            ratio = row.get('增仓占比', 0)
            turnover = row.get('成交额', 0)
            mcap = row.get('流通市值', 0)
            
            data_to_save.append((trade_date, code, name, price, change, net, ratio, turnover, mcap))
            
        c = conn.cursor()
        # 删除当天旧记录 (如果存在)
        # 简单起见，我们使用 REPLACE 或者忽略删除特定ID (因为每天我们会全量更新一次Backtest Universe)
        # 但考虑到我们可能分批更新，还是应该基于ID删除
        # 为了高效，直接用 REPLACE INTO
        
        c.executemany('''INSERT OR REPLACE INTO daily_stock_data 
                         (trade_date, stock_code, stock_name, close_price, change_pct, net_inflow, ratio, turnover, market_cap)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', data_to_save)
        
        conn.commit()
        print(f"[Backtest] 已保存 {len(data_to_save)} 只追踪股票的当日数据到 {config.HISTORY_DB_PATH}")
        
    except Exception as e:
        print(f"保存回测数据失败: {e}")
    finally:
        conn.close()


def save_daily_top_list(df: pd.DataFrame, period: str, top_n: int = 20):
    """
    保存每日排名前N的股票到历史表
    """
    if df.empty or '股票代码' not in df.columns:
        return

    # 只保存前 top_n
    df_top = df.head(top_n).copy()
    
    trade_date = get_stock_trade_date()
    
    conn = get_connection()
    try:
        data_to_insert = []
        rank_counter = 1
        
        for _, row in df_top.iterrows():
            code = str(row['股票代码'])
            # 兼容简称列名
            name = row.get('股票简称', row.get('股票名称', ''))
            
            data_to_insert.append((trade_date, code, name, rank_counter, period))
            rank_counter += 1
            
        # 使用 INSERT OR REPLACE 覆盖当天的记录
        c = conn.cursor()
        
        # 为了覆盖整个榜单，可以先删除当天该类型的旧记录，再插入新记录
        # 如果只用REPLACE，可能导致名次变动后旧名次残留（例如以前第20名变成了第21名，不在新列表里，REPLACE不会删除它）
        c.execute("DELETE FROM daily_top_history WHERE record_date = ? AND period_type = ?", (trade_date, period))
        
        c.executemany('''INSERT INTO daily_top_history (record_date, stock_code, stock_name, rank, period_type) 
                         VALUES (?, ?, ?, ?, ?)''', data_to_insert)
        
        conn.commit()
        print(f"[{period}] 前 {top_n} 名股票已保存到历史库 (日期: {trade_date})")
        
    except Exception as e:
        print(f"保存历史排名失败: {e}")
    finally:
        conn.close()

def get_stock_trade_date():
    """获取有效的交易日期（如果是周末则返回上周五）"""
    now = datetime.now()
    weekday = now.weekday() # 0=周一, 6=周日
    
    if weekday == 5: # 周六
        trade_date = now - timedelta(days=1)
    elif weekday == 6: # 周日
        trade_date = now - timedelta(days=2)
    else:
        trade_date = now
        
    return trade_date.strftime("%Y-%m-%d")

def save_fund_flow_cache(df, period):
    """保存资金流数据到缓存表"""
    if df.empty:
        return
        
    conn = get_connection()
    try:
        # 转换所有列为字符串以避免类型冲突，或者pandas会自动处理
        # 最好确保股票代码是字符串
        df_save = df.copy()
        if '股票代码' in df_save.columns:
            df_save['股票代码'] = df_save['股票代码'].astype(str)
            
        # 删除不需要保存的列 (如 '序号'，通常是API返回的索引列)
        if '序号' in df_save.columns:
            df_save = df_save.drop(columns=['序号'])
            
        # 映射列名以匹配数据库Schema
        if '股票代码' in df_save.columns:
            df_save = df_save.rename(columns={'股票代码': 'stock_code'})
            
        # 使用交易日日期而非自然日
        trade_date = get_stock_trade_date()
        
        df_save['period_type'] = period
        df_save['cache_date'] = trade_date
        
        # 0. 去重：确保同一天同一周期下，股票代码唯一
        if 'stock_code' in df_save.columns:
           df_save = df_save.drop_duplicates(subset=['stock_code', 'period_type', 'cache_date'], keep='first')
        
        # 动态获取数据库现有列，仅保存Schema中存在的列
        # 这能防止 'table fund_flow_cache has no column named XXX' 错误
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(fund_flow_cache)")
        db_cols = [info[1] for info in cursor.fetchall()]
        
        # 找出 DataFrame 中多余的列并过滤掉
        valid_cols = [c for c in df_save.columns if c in db_cols]
        df_save = df_save[valid_cols]
        
        # 删除同日期的旧数据，避免重复
        conn.execute("DELETE FROM fund_flow_cache WHERE period_type = ? AND cache_date = ?", (period, trade_date))
        conn.commit()
        
        # 保存到数据库
        # if_exists='append' 因为我们可能存不同period的数据在同一张表
        df_save.to_sql('fund_flow_cache', conn, if_exists='append', index=False)
    except Exception as e:
        print(f"数据库保存失败: {e}")
    finally:
        conn.close()

def get_fund_flow_cache(period):
    """读取当天的资金流缓存"""
    date_str = get_stock_trade_date()
    conn = get_connection()
    try:
        # 检查表是否存在
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='fund_flow_cache'")
        if not cursor.fetchone():
            return pd.DataFrame()

        query = "SELECT * FROM fund_flow_cache WHERE period_type = ? AND cache_date = ?"
        df = pd.read_sql(query, conn, params=(period, date_str))
        
        # 清理辅助列 并 恢复列名
        if not df.empty:
            if 'period_type' in df.columns:
                del df['period_type']
            if 'cache_date' in df.columns:
                del df['cache_date']
            if 'stock_code' in df.columns:
                df = df.rename(columns={'stock_code': '股票代码'})
        return df
    except Exception as e:
        print(f"数据库读取失败: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def clean_old_data(days=7):
    """清理过期数据"""
    conn = get_connection()
    cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    try:
        # 检查表是否存在
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='fund_flow_cache'")
        if cursor.fetchone():
            conn.execute("DELETE FROM fund_flow_cache WHERE cache_date < ?", (cutoff_date,))
            conn.commit()
            conn.execute("VACUUM")
    except Exception as e:
        print(f"数据库清理失败: {e}")
    finally:
        conn.close()

# --- Watchlist Operations ---
def get_watchlist():
    conn = get_connection()
    try:
        df = pd.read_sql("SELECT stock_code FROM watchlist", conn)
        return df['stock_code'].tolist()
    except:
        return []
    finally:
        conn.close()

def update_watchlist(codes):
    """全量更新自选股列表"""
    conn = get_connection()
    try:
        conn.execute("DELETE FROM watchlist")
        if codes:
            data = [(code,) for code in codes]
            conn.executemany("INSERT INTO watchlist (stock_code) VALUES (?)", data)
        conn.commit()
    except Exception as e:
        print(f"自选股更新失败: {e}")
    finally:
        conn.close()
