"""
测试真实数据的放量等级分类
"""
import rank_flow as rf

print("=" * 60)
print("获取真实股票数据并测试放量等级")
print("=" * 60)

# 获取资金流数据
print("\n1. 获取资金流数据（超大单）...")
df = rf.get_fund_flow_data('超大单')

if df.empty:
    print("[ERROR] 未获取到数据")
else:
    print(f"[OK] 获取到 {len(df)} 只股票数据")

    # 执行排名和评分（禁用量比）
    print("\n2. 执行多维度评分（禁用量比API）...")
    ranked_df = rf.rank_fund_flow(df, sort_by='comprehensive', top_n=20, enable_volume_ratio=False)

    print(f"[OK] 评分完成，Top 20 股票")

    # 检查列
    print("\n3. 检查列名...")
    print(f"总列数: {len(ranked_df.columns)}")
    print(f"包含放量等级: {'放量等级' in ranked_df.columns}")

    if '放量等级' in ranked_df.columns:
        print("\n[OK] 放量等级列存在")

        # 显示放量等级分布
        print("\n4. 放量等级分布:")
        level_counts = ranked_df['放量等级'].value_counts()
        for level, count in level_counts.items():
            print(f"  {level}: {count}只")

        # 显示前10条数据
        print("\n5. Top 10 股票数据预览:")
        display_cols = ['股票代码', '股票简称', '成交额', '换手率', '放量等级', '综合评分']
        # 转换成交额为亿元
        df_display = ranked_df.head(10).copy()
        df_display['成交额(亿)'] = (df_display['成交额'] / 1_0000_0000).round(2)
        display_cols_new = ['股票代码', '股票简称', '成交额(亿)', '换手率', '放量等级', '综合评分']
        print(df_display[display_cols_new].to_string(index=False))
    else:
        print("\n[ERROR] 放量等级列不存在")
        print("可用列:", ranked_df.columns.tolist())

print("\n" + "=" * 60)
print("[OK] 测试完成")
print("=" * 60)
