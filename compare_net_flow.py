import akshare as ak
import pandas as pd

print('对比两个接口的净额数据')
print('=' * 80)

# 1. 获取即时数据中的某只股票
df_instant = ak.stock_fund_flow_individual(symbol='即时')
df_instant['股票代码'] = df_instant['股票代码'].astype(str).str.zfill(6)

# 找一只流通性好的股票，比如600016
stock_code = '600016'
instant_data = df_instant[df_instant['股票代码'] == stock_code]

if not instant_data.empty:
    print(f'\n[即时数据] {stock_code}:')
    print(f'  净额: {instant_data["净额"].values[0]}')
    print(f'  成交额: {instant_data["成交额"].values[0]}')
    
# 2. 获取该股票的详细资金流数据（今天）
try:
    df_detail = ak.stock_individual_fund_flow(stock='600016', market='sh')
    latest = df_detail.iloc[-1]  # 最新一天
    
    print(f'\n[详细数据] {stock_code} (日期: {latest["日期"]}):')
    print(f'  主力净额: {latest["主力净额-金额"]:.2f}')
    print(f'  超大单净额: {latest["超大单净额-金额"]:.2f}')
    print(f'  大单净额: {latest["大单净额-金额"]:.2f}')
    print(f'  中单净额: {latest["中单净额-金额"]:.2f}')
    print(f'  小单净额: {latest["小单净额-金额"]:.2f}')
    
    # 验证：主力净额 = 超大单 + 大单
    calculated_main = latest["超大单净额-金额"] + latest["大单净额-金额"]
    print(f'\n  验证: 超大单+大单 = {calculated_main:.2f}')
    
    # 总净额 = 所有分类净额之和
    total_net = (latest["超大单净额-金额"] + latest["大单净额-金额"] + 
                 latest["中单净额-金额"] + latest["小单净额-金额"])
    print(f'  验证: 所有分类净额之和 = {total_net:.2f}')
    
except Exception as e:
    print(f'\n获取详细数据失败: {e}')

print('=' * 80)
