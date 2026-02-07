"""
测试百分比数据显示修复
"""
import pandas as pd
import rank_flow as rf

print("=" * 60)
print("测试百分比数据格式转换")
print("=" * 60)

# 获取数据
df = rf.get_fund_flow_data('即时')
if not df.empty:
    ranked = rf.rank_fund_flow(df, sort_by='comprehensive', top_n=5, enable_volume_ratio=False)

    print("\n原始数据格式:")
    print("-" * 60)
    print(f"增仓占比类型: {ranked['增仓占比'].dtype}")
    print(f"换手率类型: {ranked['换手率'].dtype}")
    print(f"涨跌幅类型: {ranked['涨跌幅'].dtype}")

    print("\n原始数据示例:")
    print("-" * 60)
    for i, row in ranked.head(3).iterrows():
        print(f"{row['股票代码']} {row['股票简称']}")
        print(f"  增仓占比: {row['增仓占比']} (类型: {type(row['增仓占比']).__name__})")
        print(f"  换手率: {row['换手率']} (类型: {type(row['换手率']).__name__})")
        print(f"  涨跌幅: {row['涨跌幅']} (类型: {type(row['涨跌幅']).__name__})")
        print()

    # 模拟UI中的转换逻辑
    display_df = ranked.copy()
    percent_cols = ['换手率', '涨跌幅']
    for c in percent_cols:
        if c in display_df.columns:
            if display_df[c].dtype == 'object':
                display_df[c] = display_df[c].apply(
                    lambda x: float(str(x).replace('%', '')) if pd.notna(x) and '%' in str(x) else x
                )

    print("\n转换后数据格式:")
    print("-" * 60)
    print(f"换手率类型: {display_df['换手率'].dtype}")
    print(f"涨跌幅类型: {display_df['涨跌幅'].dtype}")

    print("\n转换后数据示例:")
    print("-" * 60)
    for i, row in display_df.head(3).iterrows():
        print(f"{row['股票代码']} {row['股票简称']}")
        print(f"  增仓占比: {row['增仓占比']:.2f}% (格式化后)")
        print(f"  换手率: {row['换手率']:.2f}% (格式化后)")
        print(f"  涨跌幅: {row['涨跌幅']:.2f}% (格式化后)")
        print()

    print("=" * 60)
    print("[OK] 转换测试完成！")
    print("现在在UI中使用 format='%.2f%%' 将正确显示为 13.39% 而不是 0.13%")
    print("=" * 60)
else:
    print("[ERROR] 未获取到数据")
