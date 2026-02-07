"""
测试PE（市盈率）补充功能
"""
import pandas as pd
import rank_flow as rf

print("=" * 60)
print("测试PE（市盈率）数据补充")
print("=" * 60)

# 创建模拟数据
print("\n1. 生成模拟数据（20只股票）...")
test_codes = [
    '600519',  # 贵州茅台
    '000858',  # 五粮液
    '600036',  # 招商银行
    '601318',  # 中国平安
    '000001',  # 平安银行
    '600276',  # 恒瑞医药
    '300750',  # 宁德时代
    '002594',  # 比亚迪
    '600887',  # 伊利股份
    '000333',  # 美的集团
]

mock_data = {
    '股票代码': test_codes,
    '股票简称': [f'测试{i}' for i in range(len(test_codes))],
}

df = pd.DataFrame(mock_data)
print(f"[OK] 生成 {len(df)} 只股票的测试数据")

# 测试PE数据获取
print("\n2. 测试PE数据获取...")
df_with_pe = rf.add_pe_ratio_for_stocks(df.copy())

# 检查结果
if '市盈率' in df_with_pe.columns:
    has_pe = df_with_pe['市盈率'].notna().sum()
    print(f"\n[OK] 成功获取 {has_pe}/{len(df)} 只股票的PE数据")

    print("\n" + "=" * 60)
    print("PE数据示例:")
    print("=" * 60)
    print(df_with_pe[['股票代码', '股票简称', '市盈率']].to_string(index=False))

    # 统计PE分布
    valid_pe = df_with_pe[df_with_pe['市盈率'].notna()]['市盈率']
    if len(valid_pe) > 0:
        print("\n" + "=" * 60)
        print("PE统计:")
        print("=" * 60)
        print(f"平均PE: {valid_pe.mean():.2f}")
        print(f"最低PE: {valid_pe.min():.2f}")
        print(f"最高PE: {valid_pe.max():.2f}")
else:
    print("\n[ERROR] 未能获取PE数据列")

print("\n" + "=" * 60)
print("[OK] PE数据测试完成")
print("=" * 60)
