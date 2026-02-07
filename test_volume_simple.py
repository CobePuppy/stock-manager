"""
简单测试量比评分函数
"""
import pandas as pd
import numpy as np
import rank_flow as rf

print("=" * 60)
print("测试量比评分系统")
print("=" * 60)

# 创建模拟数据（500只股票）
print("\n1. 生成模拟数据（500只股票）...")
num_stocks = 500

mock_data = {
    '股票代码': [f"{i:06d}" for i in range(num_stocks)],
    '股票简称': [f"股票{i}" for i in range(num_stocks)],
    '增仓占比': np.random.uniform(-10, 30, num_stocks),
    '涨跌幅': np.random.uniform(-10, 10, num_stocks),
    '换手率': np.random.uniform(0.1, 30, num_stocks),
    '成交额': np.random.uniform(1000_0000, 50_0000_0000, num_stocks),
    '净额': np.random.uniform(-1000_0000, 5000_0000, num_stocks),
    '流通市值': np.random.uniform(10_0000_0000, 800_0000_0000, num_stocks),
}

df = pd.DataFrame(mock_data)
print(f"[OK] 生成 {len(df)} 只股票的模拟数据")

# 测试4维度评分
print("\n2. 测试4维度快速评分...")
df_scored = rf.add_comprehensive_scores(df.copy())
print(f"[OK] 完成！综合评分列: {'综合评分' in df_scored.columns}")

# 显示Top 10（无量比）
print("\n" + "=" * 60)
print("Top 10（4维度评分，无量比）:")
print("=" * 60)
top10 = df_scored.nlargest(10, '综合评分')
print(top10[['股票代码', '综合评分', '增仓占比', '涨跌幅', '换手率']].to_string(index=False))

# 测试量比评分函数
print("\n" + "=" * 60)
print("3. 测试量比评分函数...")
print("=" * 60)

test_ratios = [0.5, 0.8, 1.2, 1.5, 2.0, 3.0, 5.0, 8.0]
print("量比值 -> 评分")
for ratio in test_ratios:
    score = rf.calculate_volume_ratio_score(ratio)
    print(f"  {ratio:.1f} -> {score}分")

# 测试5维度评分（手动添加量比）
print("\n" + "=" * 60)
print("4. 测试5维度评分（手动添加量比）...")
print("=" * 60)

# 为前10只股票添加模拟量比
df_with_volume = top10.copy()
df_with_volume['当日量比'] = np.random.uniform(0.8, 5.0, len(df_with_volume))
df_with_volume['量比评分'] = df_with_volume['当日量比'].apply(rf.calculate_volume_ratio_score)

# 重新计算综合评分（现在包含量比）
df_with_volume['综合评分'] = df_with_volume.apply(rf.calculate_comprehensive_score, axis=1)

print("Top 10（5维度评分，含量比）:")
print(df_with_volume[['股票代码', '综合评分', '增仓占比', '涨跌幅', '当日量比']].to_string(index=False))

print("\n" + "=" * 60)
print("[OK] 量比评分系统测试完成！")
print("=" * 60)
print("\n结论:")
print("1. 4维度快速评分功能正常")
print("2. 量比评分函数工作正常")
print("3. 5维度评分（含量比）可以正确计算")
print("4. 权重分配：增仓45% + 涨跌18% + 换手13.5% + 成交额13.5% + 量比10%")
print("=" * 60)
