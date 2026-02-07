"""
测试增仓占比计算逻辑
"""
import rank_flow as rf
import pandas as pd

print("=" * 80)
print("增仓占比计算验证")
print("=" * 80)

# 获取即时数据
df = rf.get_fund_flow_data('即时')

if not df.empty:
    print("\n1. 数据列名检查：")
    print("-" * 80)
    print("所有列:", df.columns.tolist())
    print()

    # 检查关键字段
    key_fields = ['净额', '资金流入净额', '主力净流入', '成交额', '增仓占比']
    print("关键字段存在性：")
    for field in key_fields:
        exists = field in df.columns
        has_data = df[field].notna().any() if exists else False
        print(f"  {field:<15}: 存在={exists}, 有数据={has_data}")

    print("\n2. 字段含义说明：")
    print("-" * 80)
    print("根据 akshare API 和东方财富网数据：")
    print("  '净额' = 主力资金净流入（大单+超大单净额）")
    print("  '成交额' = 当日总成交金额")
    print("  '增仓占比' = 净额 / 成交额 × 100")
    print()
    print("注：'资金流入净额' 字段在即时数据中为空，仅在N日排行中使用")

    print("\n3. 增仓占比计算验证（前10只）：")
    print("-" * 80)
    print(f"{'股票代码':<10} {'股票简称':<12} {'净额(万)':<15} {'成交额(万)':<15} "
          f"{'计算值%':<12} {'实际值%':<12} {'匹配':<6}")
    print("-" * 80)

    for i, row in df.head(10).iterrows():
        code = row['股票代码']
        name = row['股票简称']
        net = row['净额'] if pd.notna(row['净额']) else 0
        turnover = row['成交额'] if pd.notna(row['成交额']) else 1

        # 手动计算增仓占比
        calculated = (net / turnover * 100) if turnover != 0 else 0
        actual = row['增仓占比'] if pd.notna(row['增仓占比']) else 0

        # 判断是否匹配（允许0.01的误差）
        match = "OK" if abs(calculated - actual) < 0.01 else "ERROR"

        print(f"{code:<10} {name:<12} {net/10000:>14.2f} {turnover/10000:>14.2f} "
              f"{calculated:>11.2f} {actual:>11.2f} {match:<6}")

    print("-" * 80)

    # 统计验证
    print("\n4. 全量数据验证：")
    print("-" * 80)
    df_test = df.copy()
    df_test['计算增仓占比'] = (df_test['净额'] / df_test['成交额']) * 100
    df_test['差异'] = abs(df_test['计算增仓占比'] - df_test['增仓占比'])

    match_count = (df_test['差异'] < 0.01).sum()
    total_count = len(df_test)

    print(f"总股票数: {total_count}")
    print(f"计算匹配数: {match_count}")
    print(f"匹配率: {match_count/total_count*100:.2f}%")

    if match_count < total_count:
        print(f"\n不匹配的数据示例：")
        print(df_test[df_test['差异'] >= 0.01][['股票代码', '股票简称', '净额', '成交额', '增仓占比', '计算增仓占比']].head(5))

    print("\n5. 增仓占比分布：")
    print("-" * 80)
    ranges = [
        ('超强(≥25%)', len(df[df['增仓占比'] >= 25])),
        ('强(20-25%)', len(df[(df['增仓占比'] >= 20) & (df['增仓占比'] < 25)])),
        ('明显(15-20%)', len(df[(df['增仓占比'] >= 15) & (df['增仓占比'] < 20)])),
        ('一般(10-15%)', len(df[(df['增仓占比'] >= 10) & (df['增仓占比'] < 15)])),
        ('较弱(<10%)', len(df[df['增仓占比'] < 10])),
        ('流出(<0%)', len(df[df['增仓占比'] < 0])),
    ]
    for label, count in ranges:
        pct = count / len(df) * 100
        print(f"  {label:<15}: {count:>5}只 ({pct:>5.2f}%)")

print("\n" + "=" * 80)
print("[OK] 增仓占比计算验证完成")
print("=" * 80)
