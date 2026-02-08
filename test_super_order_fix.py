"""
测试修复后的超大单数据获取
"""
import akshare as ak
import pandas as pd

print("=" * 80)
print("测试修复后的超大单数据获取")
print("=" * 80)

try:
    # 获取超大单数据
    print("\n[1] 获取超大单数据...")
    df_super = ak.stock_fund_flow_individual(symbol='超大单')
    print(f"  成功！获取 {len(df_super)} 行, {len(df_super.columns)} 列")
    print(f"  列名: {df_super.columns.tolist()}")

    # 查看前5条数据
    print("\n[2] 前5条数据:")
    print(df_super.head())

    # 检查是否有净额相关字段
    print("\n[3] 检查净额字段:")
    net_columns = [col for col in df_super.columns if '净' in col or '资金' in col]
    print(f"  包含'净'或'资金'的列: {net_columns}")

    # 测试特定股票（东方财富截图中的003018）
    print("\n[4] 查找特定股票 003018:")
    df_super['股票代码'] = df_super['股票代码'].astype(str).str.zfill(6)
    stock_003018 = df_super[df_super['股票代码'] == '003018']
    if not stock_003018.empty:
        print(f"  找到了！")
        print(stock_003018)
    else:
        print(f"  未找到 003018")

    print("\n[OK] 测试完成！")

except Exception as e:
    print(f"\n[ERROR] 发生错误: {e}")
    import traceback
    traceback.print_exc()

print("=" * 80)
