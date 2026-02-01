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
    获取资金流入数据 (优先读取数据库缓存)
    :param period: '即时', '3日排行', '5日排行', '10日排行', '20日排行'
    :return: 包含资金流入数据的DataFrame
    """
    # 初始化数据库（确保表存在）
    database.init_db()

    # 清理过期
    # clean_old_files 已经在main中被调用，这里可以不调用，或者也调用防守
    
    # 1. 尝试从数据库读取
    try:
        df_cache = database.get_fund_flow_cache(period)
        if not df_cache.empty:
            print(f"检测到今日数据库缓存数据，直接读取 (Period: {period})")
            # 补全股票代码前导0 (数据库Text类型应该保留了，但防守一下)
            if '股票代码' in df_cache.columns:
                df_cache['股票代码'] = df_cache['股票代码'].astype(str).apply(lambda x: x.zfill(6))
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
            fund_flow_df = fund_flow_df[fund_flow_df['股票代码'].str.startswith(('6', '3', '0'))]
            
            # 数据类型转换与指标计算
            numeric_cols = ['最新价', '流入资金', '流出资金', '净额', '成交额', '资金流入净额']
            for col in numeric_cols:
                if col in fund_flow_df.columns:
                    fund_flow_df[col] = fund_flow_df[col].apply(convert_unit)

            if period == '即时':
                # 计算增仓占比: 增仓占比 = 净额 / 成交额 * 100
                if '成交额' in fund_flow_df.columns and '净额' in fund_flow_df.columns:
                    fund_flow_df['增仓占比'] = (fund_flow_df['净额'] / fund_flow_df['成交额']) * 100
                
                # 计算并添加 '流通市值'
                # 流通市值 = 成交额 / (换手率 / 100)
                if '成交额' in fund_flow_df.columns and '换手率' in fund_flow_df.columns:
                     def parse_rate_val(x):
                         if pd.isna(x): return np.nan
                         s = str(x).replace('%', '')
                         try: return float(s)
                         except: return np.nan
                     
                     temp_rates = fund_flow_df['换手率'].apply(parse_rate_val)
                     # 避免除以0
                     fund_flow_df['流通市值'] = fund_flow_df['成交额'] / (temp_rates.replace(0, np.nan) / 100)

                # 确保以前的逻辑兼容
                fund_flow_df['主力净流入'] = fund_flow_df.get('净额', 0)

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
            
            # 保存到数据库缓存
            try:
                database.save_fund_flow_cache(fund_flow_df, period)
                print(f"数据已更新并缓存至数据库")
            except Exception as e:
                print(f"写入数据库缓存失败: {e}")

        return fund_flow_df
    except Exception as e:
        print(f"获取资金流入数据失败: {e}")
        return pd.DataFrame()


import concurrent.futures

def fetch_calculated_daily_volume_ratio(code):
    try:
        # 2. 获取历史数据 (作为主要数据源和fallback)
        # 调整时间窗口，确保获取足够数据
        end_date = datetime.now()
        start_date = end_date - timedelta(days=60) # 扩大范围防止假期
        start_date_str = start_date.strftime("%Y%m%d")
        
        hist_df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date_str, adjust="qfq")
        if hist_df.empty or '成交量' not in hist_df.columns:
            return code, np.nan
            
        # 按日期降序
        hist_df = hist_df.sort_values(by="日期", ascending=False)
        
        # 3. 尝试获取实时数据 (当日成交量)
        today_vol = None
        is_spot_data = False
        
        try:
            bid_df = ak.stock_bid_ask_em(symbol=code)
            vol_row = bid_df[bid_df['item'] == '成交量']
            if not vol_row.empty:
                today_vol = float(vol_row['value'].values[0])
                is_spot_data = True
        except Exception:
            # 如果获取实时数据失败（如网络限制），后续使用历史最新作为当日
            pass
            
        # 4. 确定 当日量 和 历史均量 的取值范围
        today_date_str = end_date.strftime("%Y-%m-%d")
        
        target_vol = 0.0
        past_df = pd.DataFrame()
        
        if is_spot_data and today_vol is not None:
            target_vol = today_vol
            # 如果历史数据的最新一条也是今天，则计算均值时需要跳过它
            if not hist_df.empty and hist_df.iloc[0]['日期'] == today_date_str:
                past_df = hist_df.iloc[1:8]
            else:
                past_df = hist_df.head(7)
        else:
            # Fallback: 使用历史数据的最新一天作为 "当日"
            # (适用于盘后分析或实时接口受限时)
            if not hist_df.empty:
                target_vol = hist_df.iloc[0]['成交量']
                past_df = hist_df.iloc[1:8] # 过去7日不包含最新这一天
            else:
                return code, np.nan
        
        if past_df.empty or len(past_df) < 1:
             return code, np.nan
             
        avg_vol = past_df['成交量'].mean()
        
        if avg_vol == 0:
            return code, 0.0
            
        ratio = target_vol / avg_vol
        return code, round(ratio, 2)
        
    except Exception:
        return code, np.nan

def fetch_volume_ratio_for_list(df: pd.DataFrame) -> pd.DataFrame:
    """
    为给定的股票列表计算 '当日量比' (当日成交量/过去7日平均每日成交量)
    """
    if df.empty or '股票代码' not in df.columns:
        return df

    print(f"正在计算前 {len(df)} 名的当日量比数据 (并发请求)...")
    
    codes = df['股票代码'].tolist()
    ratios_map = {}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_code = {executor.submit(fetch_calculated_daily_volume_ratio, code): code for code in codes}
        
        for future in concurrent.futures.as_completed(future_to_code):
            code = future_to_code[future]
            try:
                c, val = future.result()
                ratios_map[c] = val
            except Exception:
                ratios_map[code] = np.nan
            
    # 映射回 DataFrame
    df['当日量比'] = df['股票代码'].map(ratios_map)
    return df

def rank_fund_flow(fund_flow_df: pd.DataFrame, sort_by: str = 'ratio', top_n: int = 50) -> pd.DataFrame:
    """
    对资金流入数据进行排名
    :param fund_flow_df: 包含资金流入数据的DataFrame
    :param sort_by: 排序指标，'ratio'表示增仓占比，'net'表示净流入(主力)
    :param top_n: 返回前N名
    :return: 排名后的DataFrame
    """
    if sort_by == 'ratio':
        column_name = '增仓占比'
    elif sort_by == 'net':
        column_name = '净额'
    else:
        # 尝试兼容以前的参数
        column_names = {'large': '大单净流入', 'medium': '中单净流入', 'small': '小单净流入'}
        column_name = column_names.get(sort_by)
    
    if column_name and column_name in fund_flow_df.columns:
        # 兼容列名 '股票简称' (新API) 和 '股票名称' (旧代码可能期望)
        name_col = '股票简称' if '股票简称' in fund_flow_df.columns else '股票名称'
        
        # 1. 先排序并取Top N
        ranked_df = fund_flow_df.sort_values(by=column_name, ascending=False).head(top_n).copy()
        
        if '流通市值' in fund_flow_df.columns:
            result_cols = ['股票代码', name_col, '流通市值', column_name]
        else:
            result_cols = ['股票代码', name_col, column_name]

        # 如果不是按净额排序，且净额存在，也展示净额以便参考
        if column_name != '净额' and '净额' in fund_flow_df.columns:
            result_cols.append('净额')
            
        # 2. 为这 Top N 获取 '当日量比' 数据 (暂时废弃)
        # ranked_df = fetch_volume_ratio_for_list(ranked_df)
        
        # 3. 将 '当日量比' 加入展示列
        # if '当日量比' in ranked_df.columns:
        #    result_cols.append('当日量比')
            
        return ranked_df[result_cols]
    else:
        print(f"列名 {column_name} 不存在于数据中，可用列: {fund_flow_df.columns.tolist()}")
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
        
        display_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"数据已保存到 {file_path}")
        # 返回格式化后的df供控制台打印
        return display_df
    except Exception as e:
        print(f"保存数据失败: {e}")
        return df