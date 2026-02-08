"""
检查即时数据的"净额"字段含义
"""
import akshare as ak

print("=" * 80)
print("检查stock_fund_flow_individual接口")
print("=" * 80)

# 获取即时数据
print("\n获取即时数据...")
df_instant = ak.stock_fund_flow_individual(symbol='即时')

print(f"\n列名: {df_instant.columns.tolist()}")
print(f"\n前5行数据:")
print(df_instant.head())

# 检查是否有其他相关接口
print("\n\n检查akshare中是否有超大单相关接口:")
import inspect
for name in dir(ak):
    if 'fund' in name.lower() and 'flow' in name.lower():
        print(f"  - {name}")

print("=" * 80)
