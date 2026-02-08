import akshare as ak
import pandas as pd

print('对比即时数据和详细数据')
print('=' * 80)

# 1. 获取即时数据
df_instant = ak.stock_fund_flow_individual(symbol='即时')
df_instant['股票代码'] = df_instant['股票代码'].astype(str).str.zfill(6)

stock_code = '600016'
instant = df_instant[df_instant['股票代码'] == stock_code].iloc[0]

print(f'\n[即时数据] {stock_code}:')
print(f'  流入资金: {instant["流入资金"]}')
print(f'  流出资金: {instant["流出资金"]}')
print(f'  净额: {instant["净额"]}')
print(f'  成交额: {instant["成交额"]}')
print(f'  涨跌幅: {instant["涨跌幅"]}')

# 2. 获取详细数据
df_detail = ak.stock_individual_fund_flow(stock='600016', market='sh')
latest = df_detail.iloc[-1]

print(f'\n[详细数据] {stock_code} (日期: {latest["日期"]}):')
print(f'  主力净额-金额: {latest["主力净额-金额"]/1e8:.2f}亿')
print(f'  超大单净额-金额: {latest["超大单净额-金额"]/1e8:.2f}亿')
print(f'  大单净额-金额: {latest["大单净额-金额"]/1e8:.2f}亿')
print(f'  中单净额-金额: {latest["中单净额-金额"]/1e8:.2f}亿')
print(f'  小单净额-金额: {latest["小单净额-金额"]/1e8:.2f}亿')
print(f'  收盘价: {latest["收盘价"]}')
print(f'  涨跌幅: {latest["涨跌幅"]}%')

print(f'\n[分析]')
print(f'  即时数据的"净额" ≠ 详细数据的任何一个净额')
print(f'  即时数据可能是实时交易数据（当前时刻）')
print(f'  详细数据是全天统计数据（收盘后）')

print('=' * 80)
