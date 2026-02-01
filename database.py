import sqlite3
import pandas as pd
import config
from datetime import datetime, timedelta
import os

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
                
    conn.commit()
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
