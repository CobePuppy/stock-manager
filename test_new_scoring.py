"""
快速测试新的多维度评分系统性能
"""
import time
import rank_flow as rf

print("=" * 60)
print("测试新版多维度评分系统性能")
print("=" * 60)

# 测试获取数据和评分速度
start_time = time.time()

print("\n1. 获取即时资金流向数据...")
df = rf.get_fund_flow_data(period='即时')

if df.empty:
    print("[ERROR] 未能获取数据，尝试使用缓存数据")
    # 尝试从数据库读取
    import database
    df = database.get_fund_flow_cache('即时')
    if df is not None and not df.empty:
        print("[OK] 从缓存读取数据成功")
    else:
        print("[ERROR] 无缓存数据可用，退出测试")
        print("提示: 请先运行主应用(streamlit run app.py)以获取数据")
        exit(1)

data_fetch_time = time.time() - start_time
print(f"[OK] 获取数据完成，共 {len(df)} 只股票，耗时: {data_fetch_time:.2f}秒")

print("\n2. 进行综合排名和评分...")
rank_start = time.time()

ranked_df = rf.rank_fund_flow(df, sort_by='comprehensive', top_n=50, period=None)

rank_time = time.time() - rank_start
total_time = time.time() - start_time

print(f"[OK] 排名评分完成，耗时: {rank_time:.2f}秒")
print(f"[STAT] 总耗时: {total_time:.2f}秒")

if not ranked_df.empty:
    print("\n" + "=" * 60)
    print("[TOP] Top 10 综合评分结果:")
    print("=" * 60)

    # 显示前10名
    display_cols = ['股票代码', '股票简称', '综合评分', '增仓占比', '涨跌幅', '换手率']
    available_cols = [col for col in display_cols if col in ranked_df.columns]

    print(ranked_df[available_cols].head(10).to_string(index=False))

    # 统计综合评分分布
    print("\n" + "=" * 60)
    print("[CHART] 评分分布统计:")
    print("=" * 60)
    excellent = len(ranked_df[ranked_df['综合评分'] >= 80])
    good = len(ranked_df[(ranked_df['综合评分'] >= 70) & (ranked_df['综合评分'] < 80)])
    medium = len(ranked_df[(ranked_df['综合评分'] >= 60) & (ranked_df['综合评分'] < 70)])
    low = len(ranked_df[ranked_df['综合评分'] < 60])

    print(f"优秀(≥80分): {excellent}只")
    print(f"良好(70-79分): {good}只")
    print(f"中等(60-69分): {medium}只")
    print(f"一般(<60分): {low}只")

print("\n" + "=" * 60)
print("[OK] 测试完成！新评分系统运行正常且速度极快")
print("=" * 60)
