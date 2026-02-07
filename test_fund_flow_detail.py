"""
测试获取详细的资金流数据（分级：超大单、大单、中单、小单）
"""
import akshare as ak
import pandas as pd

print("=" * 80)
print("测试不同资金流API的数据结构")
print("=" * 80)

# 测试股票代码
test_code = '300953'

print(f"\n1. stock_fund_flow_individual (个股资金流排行) - 当前使用")
print("-" * 80)
try:
    df1 = ak.stock_fund_flow_individual(symbol='即时')
    stock1 = df1[df1['股票代码'].astype(str).str.zfill(6) == test_code]

    if not stock1.empty:
        print(f"列名: {df1.columns.tolist()}")
        print(f"\n{test_code} 的数据:")
        for col in stock1.columns:
            print(f"  {col}: {stock1.iloc[0][col]}")
    else:
        print(f"未找到 {test_code}")
except Exception as e:
    print(f"ERROR: {e}")

print(f"\n2. stock_individual_fund_flow (单股详细资金流)")
print("-" * 80)
try:
    # 参数: stock='股票代码', market='sz'(深圳) 或 'sh'(上海)
    df2 = ak.stock_individual_fund_flow(stock=test_code, market='sz')

    print(f"列名: {df2.columns.tolist()}")
    print(f"\n最新数据（第1行）:")
    print(df2.head(1).T)

    # 检查是否有主力净流入字段
    main_flow_cols = [c for c in df2.columns if any(x in c for x in ['主力', '超大单', '大单', '中单', '小单'])]
    if main_flow_cols:
        print(f"\n资金流分级字段: {main_flow_cols}")

except Exception as e:
    print(f"ERROR: {e}")

print(f"\n3. stock_main_fund_flow (主力资金流排行)")
print("-" * 80)
try:
    df3 = ak.stock_main_fund_flow(symbol='全部股票')
    stock3 = df3[df3['代码'].astype(str).str.zfill(6) == test_code]

    if not stock3.empty:
        print(f"列名: {df3.columns.tolist()}")
        print(f"\n{test_code} 的数据:")
        for col in stock3.columns:
            print(f"  {col}: {stock3.iloc[0][col]}")
    else:
        print(f"未找到 {test_code}")
except Exception as e:
    print(f"ERROR: {e}")

print("\n" + "=" * 80)
print("分析结论")
print("=" * 80)
print("主力资金 = 超大单 + 大单")
print("散户资金 = 中单 + 小单")
print("总净额 = 主力资金 + 散户资金")
print()
print("正确的增仓占比应该是：主力资金净流入 / 成交额 × 100")
