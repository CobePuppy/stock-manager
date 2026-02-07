"""
测试成交额+换手率放量等级分类
"""
import pandas as pd
import numpy as np
import rank_flow as rf

print("=" * 60)
print("测试放量等级分类系统（基于成交额+换手率）")
print("=" * 60)

# 创建测试数据
print("\n1. 生成测试数据...")
test_cases = [
    # (成交额亿元, 换手率%, 预期等级)
    (15, 12, "强放量"),      # 大额+高换手
    (15, 6, "明显放量"),     # 大额+中换手
    (15, 4, "温和放量"),     # 大额+较高换手
    (15, 2, "正常"),         # 大额+低换手
    (7, 16, "强放量"),       # 中额+超高换手
    (7, 9, "明显放量"),      # 中额+高换手
    (7, 6, "温和放量"),      # 中额+中换手
    (7, 3, "正常"),          # 中额+低换手
    (3, 21, "明显放量"),     # 小额+超高换手
    (3, 12, "温和放量"),     # 小额+高换手
    (3, 5, "正常"),          # 小额+中换手
    (1, 16, "温和放量"),     # 微额+高换手
    (1, 6, "正常"),          # 微额+中换手
    (1, 2, "缩量"),          # 微额+低换手
]

mock_data = {
    '股票代码': [f"{i:06d}" for i in range(len(test_cases))],
    '股票简称': [f"测试{i}" for i in range(len(test_cases))],
    '成交额': [amount * 1_0000_0000 for amount, _, _ in test_cases],
    '换手率': [rate for _, rate, _ in test_cases],
    '增仓占比': np.random.uniform(10, 25, len(test_cases)),
    '涨跌幅': np.random.uniform(2, 8, len(test_cases)),
    '净额': np.random.uniform(1000_0000, 5000_0000, len(test_cases)),
    '流通市值': np.random.uniform(50_0000_0000, 500_0000_0000, len(test_cases)),
}

df = pd.DataFrame(mock_data)
print(f"[OK] 生成 {len(df)} 个测试用例")

# 测试放量等级分类
print("\n2. 测试放量等级分类...")
df_result = rf.add_comprehensive_scores(df.copy())

# 检查结果
if '放量等级' in df_result.columns:
    print("[OK] 放量等级列添加成功\n")

    # 显示结果对比
    print("=" * 80)
    print("测试结果对比:")
    print("=" * 80)
    print(f"{'成交额(亿)':<12} {'换手率(%)':<12} {'预期等级':<12} {'实际等级':<12} {'匹配':<6}")
    print("-" * 80)

    match_count = 0
    for i, (amount, rate, expected) in enumerate(test_cases):
        actual = df_result.iloc[i]['放量等级']
        is_match = "OK" if actual == expected else "X"
        if actual == expected:
            match_count += 1
        print(f"{amount:<12.1f} {rate:<12.1f} {expected:<12} {actual:<12} {is_match:<6}")

    print("-" * 80)
    print(f"匹配率: {match_count}/{len(test_cases)} ({match_count/len(test_cases)*100:.1f}%)")

    # 显示完整结果
    print("\n" + "=" * 80)
    print("完整数据预览（含综合评分）:")
    print("=" * 80)
    display_cols = ['股票代码', '成交额', '换手率', '放量等级', '综合评分']
    # 成交额转换为亿元显示
    df_display = df_result.copy()
    df_display['成交额(亿)'] = df_display['成交额'] / 1_0000_0000
    display_cols_new = ['股票代码', '成交额(亿)', '换手率', '放量等级', '综合评分']
    print(df_display[display_cols_new].to_string(index=False))

else:
    print("[ERROR] 未找到放量等级列")

print("\n" + "=" * 60)
print("[OK] 放量等级分类测试完成！")
print("=" * 60)
