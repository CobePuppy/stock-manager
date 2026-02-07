"""
测试使用超大单净额计算增仓占比
"""
import rank_flow as rf
import pandas as pd

print("=" * 80)
print("测试超大单净额计算增仓占比")
print("=" * 80)

# 清除缓存，强制从API获取新数据
import database
try:
    database.clear_all_cache()
    print("已清除缓存")
except:
    pass

# 获取数据（会触发超大单数据获取）
print("\n获取即时资金流数据（含超大单）...")
df = rf.get_fund_flow_data('即时')

if not df.empty:
    print(f"\n成功获取 {len(df)} 只股票数据")

    # 检查列名
    print("\n数据列:")
    print(df.columns.tolist())

    # 查找 300953
    stock = df[df['股票代码'] == '300953']

    if not stock.empty:
        print("\n" + "=" * 80)
        print("300953 震裕科技 - 数据对比")
        print("=" * 80)

        net_all = stock.iloc[0]['净额'] if '净额' in stock.columns else None
        net_main = stock.iloc[0]['主力净额'] if '主力净额' in stock.columns else None
        turnover = stock.iloc[0]['成交额']
        ratio = stock.iloc[0]['增仓占比']

        print(f"所有资金净额: {net_all:,.0f} ({net_all/100000000:.2f}亿)" if pd.notna(net_all) else "所有资金净额: 无数据")
        print(f"主力净额（超大单）: {net_main:,.0f} ({net_main/100000000:.2f}亿)" if pd.notna(net_main) else "主力净额: 无数据")
        print(f"成交额: {turnover:,.0f} ({turnover/100000000:.2f}亿)")
        print()

        if pd.notna(net_all) and pd.notna(net_main):
            print(f"所有资金占比: {(net_all/turnover)*100:.2f}%")
            print(f"主力资金占比: {(net_main/turnover)*100:.2f}% ← 这个才是真正的增仓占比")
            print(f"实际增仓占比（系统）: {ratio:.2f}%")
            print()

            if abs((net_main/turnover)*100 - ratio) < 0.01:
                print("✓ 验证通过：使用的是主力净额（超大单）")
            else:
                print("✗ 验证失败：未使用主力净额")
    else:
        print("\n未找到 300953，显示前5条数据:")
        print(df[['股票代码', '股票简称', '净额', '主力净额', '成交额', '增仓占比']].head(5) if '主力净额' in df.columns else df.head(5))

    # 统计
    if '主力净额' in df.columns and '净额' in df.columns:
        print("\n" + "=" * 80)
        print("全局数据统计")
        print("=" * 80)

        df_valid = df[df['成交额'] > 0].copy()
        df_valid['所有资金占比'] = (df_valid['净额'] / df_valid['成交额']) * 100
        df_valid['主力资金占比'] = (df_valid['主力净额'] / df_valid['成交额']) * 100

        print(f"平均所有资金占比: {df_valid['所有资金占比'].mean():.2f}%")
        print(f"平均主力资金占比: {df_valid['主力资金占比'].mean():.2f}%")
        print()
        print(f"所有资金占比 > 20%: {len(df_valid[df_valid['所有资金占比'] > 20])} 只")
        print(f"主力资金占比 > 20%: {len(df_valid[df_valid['主力资金占比'] > 20])} 只")

else:
    print("未获取到数据")

print("\n" + "=" * 80)
print("测试完成")
print("=" * 80)
