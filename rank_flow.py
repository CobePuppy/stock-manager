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

# 全局缓存，避免重复请求同一只股票的量比数据
_VOLUME_RATIO_CACHE = {}

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

def calculate_price_momentum_score(price_change):
    """
    计算涨跌幅评分 (0-100分)
    评估价格动量强度
    """
    if pd.isna(price_change):
        return 0

    pct = float(price_change)

    if pct >= 9.9:  # 涨停或接近涨停
        return 100
    elif pct >= 7:  # 强势
        return 90
    elif pct >= 5:  # 较强上涨
        return 80
    elif pct >= 3:  # 温和上涨
        return 70
    elif pct >= 1:  # 小幅上涨
        return 50
    elif pct >= -1:  # 平盘附近
        return 30
    elif pct >= -3:  # 小幅下跌
        return 20
    elif pct >= -5:  # 明显下跌
        return 10
    else:  # 大幅下跌
        return 0

def calculate_turnover_rate_score(turnover_rate):
    """
    计算换手率评分 (0-100分)
    评估交易活跃度，理想范围3-15%
    """
    if pd.isna(turnover_rate):
        return 0

    # 处理字符串格式
    if isinstance(turnover_rate, str):
        turnover_rate = float(turnover_rate.replace('%', ''))

    rate = float(turnover_rate)

    if 3 <= rate <= 15:  # 理想活跃范围
        return 100
    elif 2 <= rate < 3 or 15 < rate <= 20:  # 可接受
        return 75
    elif 1 <= rate < 2 or 20 < rate <= 30:  # 偏离理想
        return 50
    elif 0.5 <= rate < 1 or 30 < rate <= 40:  # 过低或过高
        return 25
    else:  # 极端情况
        return 10

def calculate_turnover_amount_score(turnover_amount):
    """
    计算成交额评分 (0-100分)
    评估流动性，成交额越大越能保证真实性
    """
    if pd.isna(turnover_amount) or turnover_amount <= 0:
        return 0

    amount = float(turnover_amount)

    if amount >= 10_0000_0000:  # >= 10亿
        return 100
    elif amount >= 5_0000_0000:  # >= 5亿
        return 85
    elif amount >= 2_0000_0000:  # >= 2亿
        return 70
    elif amount >= 1_0000_0000:  # >= 1亿
        return 55
    elif amount >= 5000_0000:    # >= 5000万
        return 40
    else:  # < 5000万
        return 20

def calculate_position_increase_score(position_ratio):
    """
    计算增仓评分 (0-100分)
    根据增仓占比计算分数
    """
    if pd.isna(position_ratio):
        return 0

    ratio = float(position_ratio)

    if ratio >= 20:
        return 100
    elif ratio >= 15:
        return 95
    elif ratio >= 12:
        return 90
    elif ratio >= 10:
        return 85
    elif ratio >= 8:
        return 75
    elif ratio >= 6:
        return 65
    elif ratio >= 5:
        return 55
    elif ratio >= 4:
        return 48
    elif ratio >= 3:
        return 40
    elif ratio >= 2:
        return 32
    elif ratio >= 1:
        return 25
    elif ratio >= 0:
        return 15
    else:
        # 负增仓（资金流出），按比例递减
        return max(0, 15 + ratio * 2)  # 每-1%扣2分

def calculate_comprehensive_score(row):
    """
    计算综合评分 (0-100分) - 新版多维度评分系统

    评分维度和权重:
    - 增仓占比 50%: 资金流入强度（核心指标）
    - 涨跌幅   20%: 价格动量（趋势确认）
    - 换手率   15%: 交易活跃度（市场关注）
    - 成交额   15%: 流动性保障（避免小票）

    设计理念:
    - 增仓强 + 价格涨 + 活跃度高 + 流动性好 = 高分
    - 多维度验证，降低单一指标误判风险
    - 所有数据无需额外计算，速度极快
    """
    # 1. 增仓占比评分（权重50%）
    position_ratio = row.get('增仓占比', 0)
    position_score = calculate_position_increase_score(position_ratio)

    # 2. 涨跌幅评分（权重20%）
    price_change = row.get('涨跌幅', 0)
    momentum_score = calculate_price_momentum_score(price_change)

    # 3. 换手率评分（权重15%）
    turnover_rate = row.get('换手率', 0)
    activity_score = calculate_turnover_rate_score(turnover_rate)

    # 4. 成交额评分（权重15%）
    turnover_amount = row.get('成交额', 0)
    liquidity_score = calculate_turnover_amount_score(turnover_amount)

    # 综合评分 = 加权平均
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

def rank_fund_flow(fund_flow_df: pd.DataFrame, sort_by: str = 'comprehensive', top_n: int = 50, period: str = None) -> pd.DataFrame:
    """
    对资金流入数据进行排名
    :param fund_flow_df: 包含资金流入数据的DataFrame
    :param sort_by: 排序指标
                    'comprehensive'(默认): 综合评分 (增仓+放量)
                    'ratio': 增仓占比
                    'net': 净流入(主力)
    :param top_n: 返回前N名
    :param period: 周期名称(如'即时'), 如果提供且为'即时'，则会触发保存Top20到历史数据库
    :return: 排名后的DataFrame
    """
    # 先为所有数据计算多维度综合评分（在过滤和排序之前）
    # 这样综合评分排序才有意义
    fund_flow_df_with_scores = add_comprehensive_scores(fund_flow_df.copy())

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
        if '流通市值' in working_df.columns:
            result_cols = ['股票代码', name_col, '流通市值']
        else:
            result_cols = ['股票代码', name_col]

        # 根据排序方式添加关键指标列
        if sort_by == 'comprehensive':
            # 综合评分排序：显示综合评分及各维度子评分
            result_cols.extend(['综合评分', '增仓占比', '涨跌幅', '换手率', '成交额'])
        else:
            # 其他排序方式：显示主排序列
            result_cols.append(column_name)
            # 如果不是按净额排序，且净额存在，也展示净额以便参考
            if column_name != '净额' and '净额' in working_df.columns:
                result_cols.append('净额')

        # 3. 添加综合评分及各维度评分列（如果存在且未添加）
        score_cols = ['综合评分', '增仓评分', '动量评分', '活跃度评分', '流动性评分']
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