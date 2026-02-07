#这个文件用于获取股市数据 需要获取A股中本日 3日 7日 的资金流入数据（包含 大额 中额 小额资金流入数据）
#然后将这些数据进行排名，分为大额资金流入排名 中额资金流入排名 小额资金流入排名 并列出前50名

import pandas as pd
import numpy as np
import os

# 修复可能的代理配置问题 (Fix ProxyError)
# 某些环境下系统代理会自动注入Python，导致akshare连接失败或超时
os.environ['no_proxy'] = '*' 
os.environ.pop('HTTP_PROXY', None)
os.environ.pop('HTTPS_PROXY', None)
os.environ.pop('ALL_PROXY', None)

import akshare as ak
from datetime import datetime, timedelta
import time
import sys
import database # Import database module
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# 全局缓存，避免重复请求同一只股票的量比数据
_VOLUME_RATIO_CACHE = {}
# 全局数据缓存，避免单次运行中多次请求同一类型数据（如即时排行）
_FUND_FLOW_CACHE = {}

# 全局锁，用于线程安全的打印和计数
_PRINT_LOCK = Lock()
_REQUEST_DELAY = 0.05  # 50ms延迟（并发模式下可以更短）
_MAX_WORKERS = 15  # 并发线程数（可根据网络情况调整）

def convert_unit(x):
    if pd.isna(x):
        return np.nan
    s = str(x)
    if '亿' in s:
        return float(s.replace('亿', '')) * 100000000
    elif '万' in s:
        return float(s.replace('万', '')) * 10000
    elif s == '-' or s == '':
        return np.nan
    else:
        try:
            return float(s)
        except:
            return np.nan

def format_amount(x):
    """能够将数字格式化为 xx亿 或 xx万 的字符串"""
    if pd.isna(x):
        return '-'
    try:
        val = float(x)
        abs_val = abs(val)
        if abs_val >= 100000000:
            return f"{val / 100000000:.2f}亿"
        elif abs_val >= 10000:
            return f"{val / 10000:.2f}万"
        else:
            return f"{val:.2f}"
    except:
        return str(x)

def format_percentage(x):
    """将数字格式化为百分比字符串"""
    if pd.isna(x):
        return '-'
    try:
        val = float(x)
        return f"{val:.2f}%"
    except:
        return str(x)


def clean_old_files(directory: str, days: int = 7):
    """
    删除目录下超过指定天数的文件，并清理已过期的数据库数据
    :param directory: 目录路径
    :param days: 天数阈值
    """
    # 1. 清理数据库过期数据
    try:
        database.clean_old_data(days)
    except Exception as e:
        print(f"数据库清理失败: {e}")

    # 2. 清理结果文件 (analysis_results)
    if not os.path.exists(directory):
        return
        
    now = datetime.now()
    cutoff_time = now - timedelta(days=days)
    
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            try:
                # 获取文件修改时间
                file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                if file_mtime < cutoff_time:
                    os.remove(file_path)
                    print(f"已清理过期文件: {filename}")
            except Exception as e:
                print(f"清理文件失败 {filename}: {e}")

def get_fund_flow_data(period: str = '即时') -> pd.DataFrame:
    """
    获取资金流入数据 (一级:内存缓存 -> 二级:数据库缓存 -> 三级:API获取)
    :param period: '即时', '3日排行', '5日排行', '10日排行', '20日排行'
    :return: 包含资金流入数据的DataFrame
    """
    # 0. 检查内存缓存 (防止主程序多次调用或递归调用时重复获取)
    if period in _FUND_FLOW_CACHE:
        print(f"检测到内存缓存数据，直接读取 (Period: {period})")
        return _FUND_FLOW_CACHE[period]

    # 初始化数据库（确保表存在）
    database.init_db()

    # 清理过期
    # clean_old_files 已经在main中被调用，这里可以不调用，或者也调用防守
    
    # 1. 尝试从数据库读取原始数据（优先）
    try:
        trade_date = database.get_stock_trade_date()
        print(f"[缓存检查] 交易日期: {trade_date}, 查询周期: {period}")

        # 优先尝试读取原始数据
        df_raw = database.get_raw_fund_flow_cache(period)
        print(f"[缓存检查] 原始数据表返回 {len(df_raw)} 条数据")

        if not df_raw.empty:
            print(f"[缓存命中] 从原始数据表读取，准备计算指标...")
            df_cache = df_raw
        else:
            # 如果原始数据表没有，尝试读取计算结果表（兼容旧数据）
            df_cache = database.get_fund_flow_cache(period)
            print(f"[缓存检查] 计算结果表返回 {len(df_cache)} 条数据")

        if not df_cache.empty:
            print(f"[缓存命中] 检测到今日数据库缓存数据，直接读取 (Period: {period})")
            # 补全股票代码前导0 (数据库Text类型应该保留了，但防守一下)
            if '股票代码' in df_cache.columns:
                df_cache['股票代码'] = df_cache['股票代码'].astype(str).apply(lambda x: x.zfill(6))

            # 如果是即时数据，需要处理超大单数据
            if period == '即时':
                # 检查是否已有主力资金流数据
                if '主力净流入' not in df_cache.columns or df_cache['主力净流入'].isna().all():
                    print("[提示] 缓存数据缺少主力资金流数据，尝试从主力资金流缓存表读取...")

                    # 尝试从主力资金流缓存表读取
                    try:
                        trade_date = database.get_stock_trade_date()
                        conn = database.get_connection()
                        cursor = conn.cursor()
                        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='main_force_cache'")

                        if cursor.fetchone():
                            query = "SELECT * FROM main_force_cache WHERE cache_date = ?"
                            df_main_force = pd.read_sql(query, conn, params=(trade_date,))

                            if not df_main_force.empty:
                                # 恢复列名并合并
                                if 'stock_code' in df_main_force.columns:
                                    df_main_force = df_main_force.rename(columns={'stock_code': '股票代码'})
                                if 'cache_date' in df_main_force.columns:
                                    df_main_force = df_main_force.drop(columns=['cache_date'])

                                # 合并主力资金流数据
                                df_cache = df_cache.merge(df_main_force, on='股票代码', how='left')
                                df_cache['超大单净额'] = df_cache['超大单净额'].fillna(0)
                                df_cache['大单净额'] = df_cache['大单净额'].fillna(0)
                                df_cache['主力净流入'] = df_cache['主力净流入'].fillna(0)
                                print(f"[OK] 成功从主力资金流缓存读取数据")
                            else:
                                # 数据严谨性要求：缓存表为空时，抛出异常
                                conn.close()
                                raise ValueError("[数据完整性错误] 主力资金流缓存表为空，无法保证数据准确性")
                        else:
                            # 数据严谨性要求：缓存表不存在时，抛出异常
                            conn.close()
                            raise ValueError("[数据完整性错误] 主力资金流缓存表不存在，无法保证数据准确性")

                        conn.close()
                    except ValueError:
                        # 重新抛出数据完整性错误，不使用净额作为后备
                        raise
                    except Exception as ex:
                        # 数据严谨性要求：读取失败时，抛出异常而不是使用净额
                        raise RuntimeError(f"[数据完整性错误] 读取主力资金流缓存失败: {ex}，无法保证数据准确性") from ex

                # 使用主力净流入作为主力净额（超大单 + 大单）
                df_cache['主力净额'] = df_cache['主力净流入']

                # 重新计算增仓占比
                if '成交额' in df_cache.columns:
                    df_cache['增仓占比'] = (df_cache['主力净额'] / df_cache['成交额'].replace(0, np.nan)) * 100

                # 计算流通市值（如果缺失）
                if '流通市值' not in df_cache.columns and '成交额' in df_cache.columns and '换手率' in df_cache.columns:
                    def parse_rate_val(x):
                        if pd.isna(x): return np.nan
                        s = str(x).replace('%', '')
                        try: return float(s)
                        except: return np.nan

                    temp_rates = df_cache['换手率'].apply(parse_rate_val)
                    df_cache['流通市值'] = df_cache['成交额'] / (temp_rates.replace(0, np.nan) / 100)

            # 写入内存缓存
            _FUND_FLOW_CACHE[period] = df_cache
            return df_cache
    except Exception as e:
        print(f"数据库读取异常: {e}, 转为API获取")
    
    # 2. 从API获取
    try:
        print(f"正在从API获取 {period} 数据...")
        # 使用akshare获取资金流入数据
        fund_flow_df = ak.stock_fund_flow_individual(symbol=period)
        
        if not fund_flow_df.empty:
            # 处理股票代码：转为字符串并补全6位前导0
            fund_flow_df['股票代码'] = fund_flow_df['股票代码'].astype(str).str.zfill(6)
            
            # 筛选股票代码以 6, 3, 0 开头的 (前导0已补全，可以直接匹配)
            # 强制确保列名为字符串，去除可能存在的引号或空格，再匹配
            fund_flow_df['股票代码'] = fund_flow_df['股票代码'].astype(str).str.strip().str.replace("'", "").str.replace('"', "")
            fund_flow_df['股票代码'] = fund_flow_df['股票代码'].str.zfill(6)
            
            fund_flow_df = fund_flow_df[fund_flow_df['股票代码'].str.startswith(('6', '3', '0'))]
            print(f"筛选 A股(6/3/0)后剩余股票数量: {len(fund_flow_df)} 只")
            
            # 数据类型转换与指标计算
            numeric_cols = ['最新价', '流入资金', '流出资金', '净额', '成交额', '资金流入净额']
            for col in numeric_cols:
                if col in fund_flow_df.columns:
                    fund_flow_df[col] = fund_flow_df[col].apply(convert_unit)

            if period == '即时':
                print("[筛选预处理] 提前计算流通市值并过滤，以减少不必要的爬虫请求...")
                
                # 1. 提前计算 '流通市值'
                # 流通市值 = 成交额 / (换手率 / 100)
                # 确保列名没有空格
                fund_flow_df.columns = [c.strip() for c in fund_flow_df.columns]
                
                if '成交额' in fund_flow_df.columns and '换手率' in fund_flow_df.columns:
                     def parse_rate_val(x):
                         if pd.isna(x): return np.nan
                         s = str(x).replace('%', '')
                         try: return float(s)
                         except: return np.nan
                     
                     temp_rates = fund_flow_df['换手率'].apply(parse_rate_val)
                     # 避免除以0
                     fund_flow_df['流通市值'] = fund_flow_df['成交额'] / (temp_rates.replace(0, np.nan) / 100)
                else:
                    print("[Error] 缺少计算流通市值的关键列('成交额'或'换手率')")

                # 2. 提前执行市值过滤 (流通市值 < 1000亿)
                # 1000亿 = 1000 * 100000000 = 100,000,000,000
                if '流通市值' in fund_flow_df.columns:
                    original_count = len(fund_flow_df)
                    # 过滤掉 >= 1000亿 的 (保留 < 1000亿)
                    # 注意: 处理NaN值，如果算出NaN通常意味着数据不全，安全起见可以保留或过滤，这里选择过滤掉以免报错
                    fund_flow_df = fund_flow_df[fund_flow_df['流通市值'].notna() & (fund_flow_df['流通市值'] < 1000_0000_0000)]
                    print(f"[筛选结果] 过滤大盘股(>=1000亿)后: {original_count} -> {len(fund_flow_df)} 只")
                else:
                    print("[Warning] 未能计算流通市值，将跳过市值过滤，处理全量数据！")
                
                # 3. 开始获取主力资金流数据（超大单 + 大单）
                print(f"\n[步骤1/2] 准备获取 {len(fund_flow_df)} 只目标股票的主力资金流数据...")

                # 提取股票代码列表（已筛选6、3、0开头 且 市值<1000亿）
                stock_codes = fund_flow_df['股票代码'].tolist()

                # 串行获取所有股票的主力资金流数据
                print(f"[步骤2/2] 获取主力资金流数据（超大单 + 大单）...")
                df_main_force = fetch_all_main_force_flow(stock_codes)

                if not df_main_force.empty:
                    # 合并主力资金流数据
                    fund_flow_df = fund_flow_df.merge(
                        df_main_force,
                        on='股票代码',
                        how='left'
                    )

                    # 将缺失值填充为0
                    fund_flow_df['超大单净额'] = fund_flow_df['超大单净额'].fillna(0)
                    fund_flow_df['大单净额'] = fund_flow_df['大单净额'].fillna(0)
                    fund_flow_df['主力净流入'] = fund_flow_df['主力净流入'].fillna(0)

                    # 使用主力净流入作为主力净额（超大单 + 大单）
                    fund_flow_df['主力净额'] = fund_flow_df['主力净流入']

                    print(f"[OK] 成功合并主力资金流数据")
                else:
                    # 数据严谨性要求：无法获取主力资金流数据时，抛出异常
                    raise RuntimeError(
                        "[数据完整性错误] 未能获取主力资金流数据（超大单+大单），"
                        "无法计算准确的增仓占比。请检查网络连接或稍后重试。"
                    )

                # 计算增仓占比: 增仓占比 = 主力净流入 / 成交额 * 100
                # 主力净流入 = (超大单 + 大单) 买入额 - (超大单 + 大单) 卖出额
                if '成交额' in fund_flow_df.columns and '主力净额' in fund_flow_df.columns:
                    fund_flow_df['增仓占比'] = (fund_flow_df['主力净额'] / fund_flow_df['成交额'].replace(0, np.nan)) * 100
                
                # '流通市值' 已经提前计算了，这里不需要再算一次
                
                # 确保以前的逻辑兼容 (仅当主力净额存在时使用)
                if '主力净额' in fund_flow_df.columns:
                    fund_flow_df['主力净流入'] = fund_flow_df['主力净额']
                else:
                    fund_flow_df['主力净流入'] = np.nan

            elif '日排行' in period:
                # N日排行返回列: '资金流入净额'
                if '资金流入净额' in fund_flow_df.columns:
                     # 映射为 '净额' 以便统一处理
                     fund_flow_df['净额'] = fund_flow_df['资金流入净额']
                     
                     # 尝试估算增仓占比
                     try:
                         # 递归获取即时数据（利用缓存）
                         print(f"正在获取即时数据以辅助计算 {period} 增仓占比...")
                         df_instant = get_fund_flow_data(period='即时')
                         
                         if not df_instant.empty:
                             # 准备合并数据
                             # 即时数据我们需要: 股票代码, 成交额(Instant Turnover), 换手率(Instant TurnRate)
                             # N日数据我们需要: 连续换手率(N-day TurnRate), 净额(N-day NetInflow)
                             
                             # 预处理即时数据的换手率
                             def parse_rate(x):
                                 if pd.isna(x): return np.nan
                                 s = str(x).replace('%', '')
                                 try: return float(s)
                                 except: return np.nan

                             cols_ref = ['股票代码', '成交额', '换手率']
                             if '流通市值' in df_instant.columns:
                                 cols_ref.append('流通市值')

                             df_ref = df_instant[cols_ref].copy()
                             df_ref['即时成交额'] = df_ref['成交额']
                             df_ref['即时换手率'] = df_ref['换手率'].apply(parse_rate)
                             
                             # 合并
                             merge_cols = ['股票代码', '即时成交额', '即时换手率']
                             if '流通市值' in df_ref.columns:
                                 merge_cols.append('流通市值')

                             merged = pd.merge(fund_flow_df, df_ref[merge_cols], on='股票代码', how='left')
                             
                             # N日数据的 '连续换手率'
                             merged['N日换手率'] = merged['连续换手率'].apply(parse_rate)
                             
                             # 估算公式: Ratio = (N日净额 * 即时换手率) / (即时成交额 * N日换手率) * 100
                             merged['增仓占比'] = (merged['净额'] * merged['即时换手率']) / (merged['即时成交额'] * merged['N日换手率']) * 100
                             
                             # 将结果回填
                             fund_flow_df = merged
                             print(f"已成功估算 {period} 增仓占比和流通市值")
                         else:
                             fund_flow_df['增仓占比'] = 0.0
                     except Exception as ex:
                         print(f"估算增仓占比失败: {ex}")
                         fund_flow_df['增仓占比'] = 0.0 
            
            # 保存到数据库缓存（双表架构：原始数据 + 计算结果）
            try:
                print(f"[缓存保存] 准备保存 {len(fund_flow_df)} 条数据到数据库...")

                # 1. 保存原始数据（未经任何计算）
                # 需要先保存原始数据，因为fund_flow_df后续会被修改
                # 创建原始数据副本（只包含API返回的原始字段）
                original_cols = ['股票代码', '股票简称', '最新价', '涨跌幅', '换手率', '净额', '成交额',
                                 '流入资金', '流出资金', '资金流入净额', '连续换手率', '阶段涨跌幅']
                df_raw_to_save = fund_flow_df.copy()

                # 只保留原始字段（如果存在）
                cols_to_save = [col for col in original_cols if col in df_raw_to_save.columns]
                # 同时保留主力资金流数据（如果有）
                for col in ['超大单净额', '大单净额', '主力净流入']:
                    if col in df_raw_to_save.columns:
                        cols_to_save.append(col)

                df_raw_to_save = df_raw_to_save[cols_to_save]
                database.save_raw_fund_flow_cache(df_raw_to_save, period)

                # 2. 保存计算结果（包含增仓占比、流通市值等计算字段）
                database.save_fund_flow_cache(fund_flow_df, period)

                print(f"[缓存保存] 数据已成功保存至数据库 (原始数据 + 计算结果, 周期: {period})")
            except Exception as e:
                print(f"[缓存保存] 写入数据库缓存失败: {e}")
                import traceback
                traceback.print_exc()

        # 写入内存缓存
        if not fund_flow_df.empty:
            _FUND_FLOW_CACHE[period] = fund_flow_df
            
        return fund_flow_df
    except Exception as e:
        print(f"获取资金流入数据失败: {e}")
        return pd.DataFrame()


import concurrent.futures

def fetch_single_stock_main_force_flow(stock_code: str, debug: bool = False) -> dict:
    """
    获取单只股票的主力资金流数据（超大单 + 大单）

    :param stock_code: 股票代码（6位）
    :param debug: 是否打印调试信息
    :return: 包含超大单净额和大单净额的字典，失败返回None
    """
    try:
        # 判断市场：6开头是上海，3和0开头是深圳
        market = "sh" if stock_code.startswith('6') else "sz"

        # 调用akshare接口获取单只股票资金流数据
        try:
            df = ak.stock_individual_fund_flow(stock=stock_code, market=market)
        except Exception as e:
            if debug:
                print(f"[DEBUG] {stock_code}: 获取资金流数据失败 - {e}")
            return None

        if df.empty:
            if debug:
                print(f"[DEBUG] {stock_code}: 返回数据为空")
            return None

        # 获取最新一天的数据（最后一行）
        latest_data = df.iloc[-1]

        if debug:
            print(f"[DEBUG] {stock_code}: 可用列名: {latest_data.index.tolist()}")
            print(f"[DEBUG] {stock_code}: 最新日期: {latest_data.get('日期', '未知')}")

        # 提取主力资金流数据（超大单 + 大单）
        result = {
            '股票代码': stock_code
        }

        # 提取超大单净额
        super_large_net = convert_unit(latest_data['超大单净流入-净额'])
        result['超大单净额'] = super_large_net

        # 提取大单净额
        large_net = convert_unit(latest_data['大单净流入-净额'])
        result['大单净额'] = large_net

        # 计算主力净流入（超大单 + 大单）
        result['主力净流入'] = super_large_net + large_net

        if debug:
            print(f"[DEBUG] {stock_code}: 超大单净额 = {super_large_net}")
            print(f"[DEBUG] {stock_code}: 大单净额 = {large_net}")
            print(f"[DEBUG] {stock_code}: 主力净流入 = {result['主力净流入']}")

        return result

    except Exception as e:
        if debug:
            print(f"[DEBUG] {stock_code}: 异常 - {e}")
        return None

def save_main_force_cache(df: pd.DataFrame):
    """
    保存主力资金流数据到数据库缓存（超大单 + 大单）

    :param df: 包含主力资金流数据的DataFrame
    """
    if df.empty:
        return

    try:
        conn = database.get_connection()
        cursor = conn.cursor()

        # 创建主力资金流缓存表（如果不存在）
        cursor.execute('''CREATE TABLE IF NOT EXISTS main_force_cache (
                            stock_code TEXT PRIMARY KEY,
                            cache_date TEXT,
                            超大单净额 REAL,
                            大单净额 REAL,
                            主力净流入 REAL
                        )''')

        trade_date = database.get_stock_trade_date()

        # 准备数据
        df_save = df.copy()
        if '股票代码' in df_save.columns:
            df_save = df_save.rename(columns={'股票代码': 'stock_code'})

        df_save['cache_date'] = trade_date

        # 删除当天旧数据
        conn.execute("DELETE FROM main_force_cache WHERE cache_date = ?", (trade_date,))
        conn.commit()

        # 保存新数据
        df_save.to_sql('main_force_cache', conn, if_exists='append', index=False)

        print(f"[缓存保存] 已保存 {len(df_save)} 只股票的主力资金流数据到数据库")

    except Exception as e:
        print(f"[缓存保存] 保存失败: {e}")
    finally:
        conn.close()

def fetch_all_main_force_flow(stock_codes: list, use_cache: bool = True) -> pd.DataFrame:
    """
    批量获取多只股票的主力资金流数据（串行版本，避免API限流）

    :param stock_codes: 股票代码列表
    :param use_cache: 是否使用数据库缓存
    :return: 包含主力资金流数据的DataFrame（超大单净额、大单净额、主力净流入）
    """
    if not stock_codes:
        return pd.DataFrame()

    # 1. 尝试从数据库缓存读取
    if use_cache:
        try:
            trade_date = database.get_stock_trade_date()
            conn = database.get_connection()

            # 检查是否存在主力资金流缓存表
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='main_force_cache'")

            if cursor.fetchone():
                # 表存在，尝试读取今日数据
                query = "SELECT * FROM main_force_cache WHERE cache_date = ?"
                df_cache = pd.read_sql(query, conn, params=(trade_date,))
                conn.close()

                if not df_cache.empty:
                    # 恢复列名
                    if 'stock_code' in df_cache.columns:
                        df_cache = df_cache.rename(columns={'stock_code': '股票代码'})
                    if 'cache_date' in df_cache.columns:
                        df_cache = df_cache.drop(columns=['cache_date'])

                    print(f"[缓存命中] 从数据库读取到 {len(df_cache)} 只股票的主力资金流数据")
                    return df_cache
            else:
                conn.close()
        except Exception as e:
            print(f"[缓存读取] 数据库读取失败: {e}，转为API获取")

    # 2. 从API获取（并发线程池版本）
    total = len(stock_codes)
    print(f"开始获取{total}只股票的主力资金流数据（并发模式，{_MAX_WORKERS}线程）...")

    results = []
    success_count = [0]  # 使用列表以便在闭包中修改
    completed_count = [0]
    start_time = time.time()

    # 线程安全的结果收集
    results_lock = Lock()

    def fetch_with_progress(code):
        """带进度更新的获取函数"""
        result = fetch_single_stock_main_force_flow(code, debug=False)

        # 线程安全地更新结果和计数
        with results_lock:
            if result:
                results.append(result)
                success_count[0] += 1

            completed_count[0] += 1
            current = completed_count[0]

            # 每10个更新一次进度（避免刷新过快）
            if current % 10 == 0 or current == total:
                progress = current / total * 100
                elapsed = time.time() - start_time

                if current > 0:
                    avg_time = elapsed / current
                    remaining = avg_time * (total - current)
                    eta_min = int(remaining // 60)
                    eta_sec = int(remaining % 60)

                    print(f"\r进度: {current}/{total} ({progress:.1f}%) | 成功: {success_count[0]} | 速度: {current/elapsed:.1f}个/秒 | 预计剩余: {eta_min}分{eta_sec}秒",
                          end="", flush=True)

        # 短暂延迟，避免过度并发导致API限流
        time.sleep(_REQUEST_DELAY)
        return result

    # 使用线程池并发处理
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        # 提交所有任务
        futures = [executor.submit(fetch_with_progress, code) for code in stock_codes]

        # 等待所有任务完成
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"\n[WARNING] 任务执行异常: {e}")

    # 完成后换行
    elapsed_total = int(time.time() - start_time)
    print(f"\n完成！成功获取 {success_count[0]}/{total} 只股票的主力资金流数据")
    print(f"总耗时: {elapsed_total}秒 | 平均速度: {total/elapsed_total:.1f}个/秒")

    if results:
        df_result = pd.DataFrame(results)

        # 3. 保存到数据库缓存
        if use_cache and not df_result.empty:
            try:
                save_main_force_cache(df_result)
            except Exception as e:
                print(f"[缓存保存] 保存主力资金流数据到数据库失败: {e}")

        return df_result
    else:
        return pd.DataFrame()

def calculate_price_momentum_score(price_change):
    """
    计算涨跌幅评分 (0-100分) - 严格标准
    评估价格动量强度
    """
    if pd.isna(price_change):
        return 0

    # 处理字符串格式（如 "5.23%" 或 "-3.45%"）
    if isinstance(price_change, str):
        price_change = float(price_change.replace('%', ''))

    pct = float(price_change)

    if pct >= 9.9:  # 涨停或接近涨停
        return 100
    elif pct >= 8:  # 强势上涨
        return 90
    elif pct >= 6:  # 较强上涨
        return 80
    elif pct >= 4:  # 温和上涨
        return 70
    elif pct >= 2:  # 小幅上涨
        return 60
    elif pct >= 1:  # 微涨
        return 50
    elif pct >= 0:  # 平盘附近
        return 40
    elif pct >= -2:  # 小幅下跌
        return 30
    elif pct >= -4:  # 明显下跌
        return 20
    elif pct >= -6:  # 大幅下跌
        return 10
    else:  # 暴跌
        return 0

def calculate_turnover_rate_score(turnover_rate):
    """
    计算换手率评分 (0-100分) - 严格标准
    评估交易活跃度，理想范围5-10%（缩窄）
    """
    if pd.isna(turnover_rate):
        return 0

    # 处理字符串格式
    if isinstance(turnover_rate, str):
        turnover_rate = float(turnover_rate.replace('%', ''))

    rate = float(turnover_rate)

    if 5 <= rate <= 10:  # 最理想活跃范围（缩窄）
        return 100
    elif 3 <= rate < 5 or 10 < rate <= 15:  # 可接受
        return 80
    elif 2 <= rate < 3 or 15 < rate <= 20:  # 偏离理想
        return 60
    elif 1 <= rate < 2 or 20 < rate <= 30:  # 过低或过高
        return 40
    else:  # 极端情况（换手率过低或过高都不好）
        return 20

def calculate_turnover_amount_score(turnover_amount):
    """
    计算成交额评分 (0-100分) - 严格标准
    评估流动性，成交额越大越能保证真实性
    """
    if pd.isna(turnover_amount) or turnover_amount <= 0:
        return 0

    amount = float(turnover_amount)

    if amount >= 20_0000_0000:  # >= 20亿
        return 100
    elif amount >= 10_0000_0000:  # >= 10亿
        return 85
    elif amount >= 5_0000_0000:  # >= 5亿
        return 70
    elif amount >= 2_0000_0000:  # >= 2亿
        return 55
    elif amount >= 1_0000_0000:  # >= 1亿
        return 40
    else:  # < 1亿
        return 20

def calculate_position_increase_score(position_ratio):
    """
    计算增仓评分 (0-100分) - 严格标准
    根据增仓占比计算分数，提高门槛
    """
    if pd.isna(position_ratio):
        return 0

    # 处理字符串格式（如 "20.01%" 或 "-5.3%"）
    if isinstance(position_ratio, str):
        position_ratio = float(position_ratio.replace('%', ''))

    ratio = float(position_ratio)

    if ratio >= 25:  # 超强增仓
        return 100
    elif ratio >= 20:  # 强增仓
        return 90
    elif ratio >= 15:  # 明显增仓
        return 80
    elif ratio >= 12:  # 较强增仓
        return 70
    elif ratio >= 10:  # 中等增仓
        return 60
    elif ratio >= 8:  # 温和增仓
        return 50
    elif ratio >= 6:  # 小幅增仓
        return 40
    elif ratio >= 4:  # 微增仓
        return 30
    elif ratio >= 2:  # 弱增仓
        return 20
    elif ratio >= 0:  # 几乎无增仓
        return 10
    else:
        # 负增仓（资金流出），严格扣分
        return max(0, 10 + ratio * 3)  # 每-1%扣3分

def classify_turnover_level(row):
    """
    基于成交额和换手率判断放量等级（无需历史数据）

    逻辑：
    - 成交额大 + 换手率高 = 明显放量
    - 成交额适中 + 换手率高 = 温和放量
    - 成交额大 + 换手率低 = 大盘股正常
    - 成交额小 + 换手率低 = 缩量
    """
    turnover_amount = row.get('成交额', 0)
    turnover_rate = row.get('换手率', 0)

    # 处理换手率字符串格式
    if isinstance(turnover_rate, str):
        turnover_rate = float(turnover_rate.replace('%', ''))

    if pd.isna(turnover_amount) or pd.isna(turnover_rate):
        return "数据缺失"

    turnover_amount = float(turnover_amount)
    turnover_rate = float(turnover_rate)

    # 判断逻辑
    if turnover_amount >= 10_0000_0000:  # >= 10亿
        if turnover_rate >= 10:
            return "强放量"
        elif turnover_rate >= 5:
            return "明显放量"
        elif turnover_rate >= 3:
            return "温和放量"
        else:
            return "正常"
    elif turnover_amount >= 5_0000_0000:  # >= 5亿
        if turnover_rate >= 15:
            return "强放量"
        elif turnover_rate >= 8:
            return "明显放量"
        elif turnover_rate >= 5:
            return "温和放量"
        else:
            return "正常"
    elif turnover_amount >= 2_0000_0000:  # >= 2亿
        if turnover_rate >= 20:
            return "明显放量"
        elif turnover_rate >= 10:
            return "温和放量"
        else:
            return "正常"
    else:  # < 2亿
        if turnover_rate >= 15:
            return "温和放量"
        elif turnover_rate >= 5:
            return "正常"
        else:
            return "缩量"

def calculate_volume_ratio_score(volume_ratio):
    """
    计算量比评分 (0-100分) - 严格标准
    评估成交量变化，量比越大表示资金关注度越高
    """
    if pd.isna(volume_ratio) or volume_ratio <= 0:
        return 0

    ratio = float(volume_ratio)

    if ratio >= 5:  # 巨量
        return 100
    elif ratio >= 3:  # 强放量
        return 90
    elif ratio >= 2:  # 明显放量
        return 80
    elif ratio >= 1.5:  # 温和放量
        return 70
    elif ratio >= 1.2:  # 小幅放量
        return 60
    elif ratio >= 0.8:  # 正常
        return 40
    else:  # 缩量
        return 20

def calculate_comprehensive_score(row):
    """
    计算综合评分 (0-100分) - 多维度评分系统

    评分维度和权重:
    - 增仓占比 45%: 资金流入强度（核心指标）
    - 涨跌幅   18%: 价格动量（趋势确认）
    - 换手率   13.5%: 交易活跃度（市场关注）
    - 成交额   13.5%: 流动性保障（避免小票）
    - 量比     10%: 成交量放大（可选，需额外计算）

    设计理念:
    - 增仓强 + 价格涨 + 活跃度高 + 流动性好 + 放量 = 高分
    - 多维度验证，降低单一指标误判风险
    - 量比可选，未计算时自动调整权重
    """
    # 1. 增仓占比评分
    position_ratio = row.get('增仓占比', 0)
    position_score = calculate_position_increase_score(position_ratio)

    # 2. 涨跌幅评分
    price_change = row.get('涨跌幅', 0)
    momentum_score = calculate_price_momentum_score(price_change)

    # 3. 换手率评分
    turnover_rate = row.get('换手率', 0)
    activity_score = calculate_turnover_rate_score(turnover_rate)

    # 4. 成交额评分
    turnover_amount = row.get('成交额', 0)
    liquidity_score = calculate_turnover_amount_score(turnover_amount)

    # 5. 量比评分（可选）
    volume_ratio = row.get('当日量比', None)
    has_volume_ratio = pd.notna(volume_ratio) and volume_ratio > 0

    if has_volume_ratio:
        # 有量比数据：5维度评分
        volume_score = calculate_volume_ratio_score(volume_ratio)
        comprehensive = (
            position_score * 0.45 +
            momentum_score * 0.18 +
            activity_score * 0.135 +
            liquidity_score * 0.135 +
            volume_score * 0.10
        )
    else:
        # 无量比数据：4维度评分（原权重）
        comprehensive = (
            position_score * 0.50 +
            momentum_score * 0.20 +
            activity_score * 0.15 +
            liquidity_score * 0.15
        )

    return round(comprehensive, 1)

def classify_volume_level(volume_ratio):
    """
    根据量比值分类放量等级
    """
    if pd.isna(volume_ratio) or volume_ratio <= 0:
        return "数据缺失"
    elif volume_ratio < 0.8:
        return "萎缩"
    elif volume_ratio < 1.2:
        return "正常"
    elif volume_ratio < 1.8:
        return "温和放量"
    elif volume_ratio < 2.5:
        return "明显放量"
    elif volume_ratio < 5:
        return "强放量"
    else:
        return "巨量"

def calculate_volume_ratio_local(stock_code):
    """
    本地计算量比，避免API限制
    量比 = 今日成交量 / 近5日平均成交量
    """
    try:
        # 获取最近10天的历史数据（确保有足够数据计算5日均量）
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=15)).strftime("%Y%m%d")

        df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date, end_date=end_date, adjust="")

        if df.empty or len(df) < 2:
            return np.nan

        # 按日期升序排序
        df = df.sort_values('日期')

        # 今日成交量（最后一行）
        today_volume = df.iloc[-1]['成交量']

        # 近5日平均成交量（倒数第2到第6行，不包括今天）
        if len(df) >= 6:
            avg_volume_5d = df.iloc[-6:-1]['成交量'].mean()
        elif len(df) >= 2:
            # 数据不足5日，用全部历史数据（排除今天）
            avg_volume_5d = df.iloc[:-1]['成交量'].mean()
        else:
            return np.nan

        # 避免除以0
        if avg_volume_5d == 0 or pd.isna(avg_volume_5d):
            return np.nan

        volume_ratio = today_volume / avg_volume_5d
        return round(volume_ratio, 2)

    except Exception:
        # 静默失败，返回NaN
        return np.nan

def add_pe_ratio_for_stocks(df: pd.DataFrame) -> pd.DataFrame:
    """
    为股票列表补充市盈率(PE)数据

    使用全市场行情接口批量获取PE数据
    """
    if df.empty or '股票代码' not in df.columns:
        return df

    try:
        print(f"正在获取市盈率(PE)数据...")
        # 获取全市场行情数据（包含PE）
        spot_df = ak.stock_zh_a_spot_em()

        if spot_df.empty:
            print("警告: 未能获取市场行情数据")
            return df

        # 查找市盈率相关列
        pe_col = None
        for col in spot_df.columns:
            if '市盈率' in col or col == '市盈率-动态':
                pe_col = col
                break

        if pe_col is None:
            print("警告: 市场行情数据中未找到市盈率列")
            return df

        # 建立代码到PE的映射
        spot_df['代码'] = spot_df['代码'].astype(str).str.zfill(6)
        pe_map = spot_df.set_index('代码')[pe_col].to_dict()

        # 映射到目标DataFrame
        df['市盈率'] = df['股票代码'].map(pe_map)

        # 统计
        has_pe = df['市盈率'].notna().sum()
        print(f"成功获取 {has_pe}/{len(df)} 只股票的市盈率数据")

    except Exception as e:
        print(f"获取PE数据失败: {e}")
        # 添加空列避免后续报错
        if '市盈率' not in df.columns:
            df['市盈率'] = None

    return df

def add_volume_ratio_for_top_stocks(df: pd.DataFrame) -> pd.DataFrame:
    """
    为筛选后的股票列表计算量比并重新评分

    用于两步筛选的第二步：对Top 500计算量比
    - 逐个计算量比（今日成交量 / 近5日均量）
    - 添加量比评分
    - 重新计算5维度综合评分
    """
    if df.empty or '股票代码' not in df.columns:
        return df

    total = len(df)
    print(f"开始计算{total}只股票的量比...")

    volume_ratios = {}
    success_count = 0

    for idx, (_, row) in enumerate(df.iterrows()):
        code = row['股票代码']

        # 显示进度（每50个打印一次）
        if (idx + 1) % 50 == 0 or (idx + 1) == total:
            print(f"  进度: {idx + 1}/{total}")

        ratio = calculate_volume_ratio_local(code)
        if not pd.isna(ratio):
            success_count += 1
        volume_ratios[code] = ratio

        # 避免请求过快
        time.sleep(0.05)

    # 添加量比列
    df['当日量比'] = df['股票代码'].map(volume_ratios)
    print(f"成功计算 {success_count}/{total} 只股票的量比")

    # 添加量比评分
    df['量比评分'] = df['当日量比'].apply(calculate_volume_ratio_score)

    # 重新计算综合评分（现在包含量比）
    df['综合评分'] = df.apply(calculate_comprehensive_score, axis=1)

    return df

def add_comprehensive_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    为股票列表添加多维度综合评分（极速版）

    新版评分系统：
    - 增仓占比 50%: 资金流入强度
    - 涨跌幅   20%: 价格动量
    - 换手率   15%: 交易活跃度
    - 成交额   15%: 流动性保障

    优势：
    - 所有数据已包含在DataFrame中，无需额外API调用
    - 计算速度极快（毫秒级完成5000只股票）
    - 多维度验证，更准确预测上涨概率
    """
    if df.empty or '股票代码' not in df.columns:
        return df

    print(f"正在计算多维度综合评分（极速模式）...")

    # 计算综合评分（已包含所有子维度评分）
    df['综合评分'] = df.apply(calculate_comprehensive_score, axis=1)

    # 单独计算各维度评分用于展示（可选）
    df['增仓评分'] = df['增仓占比'].apply(calculate_position_increase_score)
    df['动量评分'] = df['涨跌幅'].apply(calculate_price_momentum_score)
    df['活跃度评分'] = df['换手率'].apply(calculate_turnover_rate_score)
    df['流动性评分'] = df['成交额'].apply(calculate_turnover_amount_score)

    # 添加放量等级（基于成交额和换手率）
    df['放量等级'] = df.apply(classify_turnover_level, axis=1)

    # 统计综合评分分布
    score_ranges = [
        ('优秀(≥80分)', len(df[df['综合评分'] >= 80])),
        ('良好(70-79分)', len(df[(df['综合评分'] >= 70) & (df['综合评分'] < 80)])),
        ('中等(60-69分)', len(df[(df['综合评分'] >= 60) & (df['综合评分'] < 70)])),
        ('一般(<60分)', len(df[df['综合评分'] < 60]))
    ]

    print(f"[OK] 评分完成！综合评分分布:")
    for label, count in score_ranges:
        if count > 0:
            print(f"  {label}: {count}只")

    return df

def rank_fund_flow(fund_flow_df: pd.DataFrame, sort_by: str = 'comprehensive', top_n: int = 50, period: str = None, enable_volume_ratio: bool = True) -> pd.DataFrame:
    """
    对资金流入数据进行排名（两步筛选优化）

    :param fund_flow_df: 包含资金流入数据的DataFrame
    :param sort_by: 排序指标
                    'comprehensive'(默认): 综合评分 (增仓+涨跌+换手+成交额+量比)
                    'ratio': 增仓占比
                    'net': 净流入(主力)
    :param top_n: 返回前N名
    :param period: 周期名称(如'即时'), 如果提供且为'即时'，则会触发保存Top20到历史数据库
    :param enable_volume_ratio: 是否启用量比计算（两步筛选）
    :return: 排名后的DataFrame

    两步筛选流程（enable_volume_ratio=True时）:
    1. 快速评分：计算4维度评分（无量比），筛选Top 500
    2. 精细评分：对Top 500计算量比，加入5维度评分，输出Top N
    """
    # 第一步：快速4维度评分（不计算量比）
    print("第1步：快速4维度评分...")
    fund_flow_df_with_scores = add_comprehensive_scores(fund_flow_df.copy())

    # 如果启用量比且是综合评分排序，执行第二步筛选
    if enable_volume_ratio and sort_by == 'comprehensive':
        # 先用4维度评分筛选出Top 500
        temp_top_500 = fund_flow_df_with_scores.sort_values(by='综合评分', ascending=False).head(500)

        print(f"第2步：对Top 500补充量比和PE数据...")
        # 对Top 500计算量比
        temp_top_500_enhanced = add_volume_ratio_for_top_stocks(temp_top_500.copy())
        # 对Top 500补充PE数据
        temp_top_500_enhanced = add_pe_ratio_for_stocks(temp_top_500_enhanced)

        # 合并回原数据（保留量比和PE数据）
        fund_flow_df_with_scores = fund_flow_df_with_scores.drop(temp_top_500.index)
        fund_flow_df_with_scores = pd.concat([fund_flow_df_with_scores, temp_top_500_enhanced], ignore_index=False)

        print("[OK] 两步筛选完成！")

    if sort_by == 'comprehensive':
        column_name = '综合评分'
    elif sort_by == 'ratio':
        column_name = '增仓占比'
    elif sort_by == 'net':
        column_name = '净额'
    else:
        # 尝试兼容以前的参数
        column_names = {'large': '大单净流入', 'medium': '中单净流入', 'small': '小单净流入'}
        column_name = column_names.get(sort_by)

    if column_name and column_name in fund_flow_df_with_scores.columns:
        # 兼容列名 '股票简称' (新API) 和 '股票名称' (旧代码可能期望)
        name_col = '股票简称' if '股票简称' in fund_flow_df_with_scores.columns else '股票名称'

        # 保留未过滤的全量数据用于回测记录
        original_df = fund_flow_df

        # 使用带有量比和综合评分的数据框
        working_df = fund_flow_df_with_scores

        # 0. 增加过滤逻辑: 在所有票中找出流通盘小于1000亿元的
        # 流通市值单位通常是元
        if '流通市值' in working_df.columns:
            # 1000亿 = 1000 * 100000000 = 100,000,000,000
            # 过滤掉 >= 1000亿 的
            filtered_df = working_df[working_df['流通市值'] < 1000_0000_0000]
            if filtered_df.empty:
                print("警告: 过滤后数据为空，可能所有股票流通市值都超过阈值或者流通市值数据异常")
            else:
                working_df = filtered_df

        # 1. 先排序并取Top N
        ranked_df = working_df.sort_values(by=column_name, ascending=False).head(top_n).copy()

        # 2. 构建显示列
        result_cols = ['股票代码', name_col]
        if '流通市值' in working_df.columns:
            result_cols.append('流通市值')
        if '市盈率' in ranked_df.columns:
            result_cols.append('市盈率')

        # 根据排序方式添加关键指标列
        if sort_by == 'comprehensive':
            # 综合评分排序：显示综合评分及各维度子评分
            result_cols.extend(['综合评分', '增仓占比', '涨跌幅', '换手率', '成交额'])
            # 添加放量等级（基于成交额和换手率）
            if '放量等级' in ranked_df.columns:
                result_cols.append('放量等级')
            # 如果有量比数据，也添加
            if '当日量比' in ranked_df.columns:
                result_cols.append('当日量比')
        else:
            # 其他排序方式：显示主排序列
            result_cols.append(column_name)
            # 如果不是按净额排序，且净额存在，也展示净额以便参考
            if column_name != '净额' and '净额' in working_df.columns:
                result_cols.append('净额')

        # 3. 添加综合评分及各维度评分列（如果存在且未添加）
        score_cols = ['综合评分', '增仓评分', '动量评分', '活跃度评分', '流动性评分', '量比评分']
        for col in score_cols:
            if col in ranked_df.columns and col not in result_cols:
                result_cols.append(col)

        # 4. 触发保存历史 Top (仅当 period='即时' 且按综合评分或增仓占比排序时)
        if period == '即时' and sort_by in ['comprehensive', 'ratio']:
            try:
                # A. 保存当日榜单前20到 daily_top_history
                database.save_daily_top_list(ranked_df, period, top_n=20)
                
                # B. 触发回测数据更新:
                # 获取所有曾入榜的股票代码
                tracked_codes = database.get_all_tracked_stocks()
                
                if tracked_codes:
                    print(f"正在更新 {len(tracked_codes)} 只历史入榜股票的当日数据...")
                    
                    # 筛选出 tracked_codes 对应的行
                    # 需要先把 股票代码 转为 string 比较
                    fund_flow_df_str = original_df.copy()
                    if '股票代码' in fund_flow_df_str.columns:
                        fund_flow_df_str['股票代码'] = fund_flow_df_str['股票代码'].astype(str)
                    
                    df_tracked = fund_flow_df_str[fund_flow_df_str['股票代码'].isin(tracked_codes)]
                    
                    if not df_tracked.empty:
                        database.save_daily_data_for_backtest(df_tracked)
                    else:
                        print("Warning: 未在当前数据中找到任何已追踪股票的数据")
                
            except Exception as e:
                print(f"Warning: 自动保存历史排名/回测数据失败: {e}")

        # 过滤result_cols，只保留ranked_df中实际存在的列
        final_cols = [col for col in result_cols if col in ranked_df.columns]
        return ranked_df[final_cols]
    else:
        print(f"列名 {column_name} 不存在于数据中，可用列: {fund_flow_df_with_scores.columns.tolist()}")
        return pd.DataFrame()

def save_to_csv(df: pd.DataFrame, filename: str, folder: str = 'analysis_results'):
    """
    将DataFrame保存为CSV文件 (自动进行中文单位格式化)
    :param df: 要保存的DataFrame
    :param filename: 文件名前缀
    :param folder: 保存目录，默认为 analysis_results

    """
    try:
        if not os.path.exists(folder):
            os.makedirs(folder)
            
        # 使用简洁日期格式 YYMMDD，如 260201
        today_str = datetime.now().strftime("%y%m%d")
        full_filename = f"{filename}_{today_str}.csv"
        file_path = os.path.join(folder, full_filename)
        
        # 创建用于显示的副本，避免修改原数据影响后续计算（如果有的话）
        display_df = df.copy()
        
        # 自动识别并格式化数值列
        amount_cols = ['净额', '成交额', '资金流入净额', '大单净流入', '中单净流入', '小单净流入', '流入资金', '流出资金', '主力净流入', '流通市值']
        percent_cols = ['增仓占比', '涨跌幅', '换手率', '阶段涨跌幅', '连续换手率']

        for col in amount_cols:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(format_amount)

        for col in percent_cols:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(format_percentage)

        # 格式化评分类列为小数或整数显示
        if '放量评分' in display_df.columns:
            display_df['放量评分'] = display_df['放量评分'].apply(lambda x: f"{int(x)}分" if pd.notna(x) else '-')
        if '综合评分' in display_df.columns:
            display_df['综合评分'] = display_df['综合评分'].apply(lambda x: f"{x:.1f}分" if pd.notna(x) else '-')
        
        display_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"数据已保存到 {file_path}")
        # 返回格式化后的df供控制台打印
        return display_df
    except Exception as e:
        print(f"保存数据失败: {e}")
        return df