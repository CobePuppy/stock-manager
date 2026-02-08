"""
使用缓存数据测试
"""
from rank_flow import get_fund_flow_data

print("=" * 80)
print("测试缓存数据")
print("=" * 80)

try:
    # 直接使用缓存数据（不清除）
    print("\n[1] 读取缓存数据...")
    df = get_fund_flow_data(period='即时')

    if not df.empty:
        print(f"[OK] 成功获取 {len(df)} 行数据")
        print(f"\n列名: {df.columns.tolist()}")

        # 检查关键字段
        print("\n[2] 检查关键字段:")
        key_fields = ['股票代码', '股票简称', '最新价', '涨跌幅', '换手率', '成交额', '主力净额', '增仓占比']
        for field in key_fields:
            if field in df.columns:
                print(f"  [OK] {field}")

        # 查看有增仓占比的股票数量
        if '增仓占比' in df.columns and '主力净额' in df.columns:
            valid_count = df['增仓占比'].notna().sum()
            positive_count = (df['增仓占比'] > 0).sum()
            positive_net = (df['主力净额'] > 0).sum()
            print(f"\n[3] 数据统计:")
            print(f"  有效增仓占比数据: {valid_count} / {len(df)}")
            print(f"  正增仓(增仓占比>0): {positive_count}")
            print(f"  正主力净额(主力净额>0): {positive_net}")

            # 显示增仓占比最高的前10只股票
            if positive_count > 0:
                print("\n[4] 增仓占比Top 10:")
                top_10 = df.nlargest(10, '增仓占比')[['股票代码', '股票简称', '增仓占比', '主力净额', '成交额', '涨跌幅']]
                for idx, row in top_10.iterrows():
                    print(f"  {row['股票代码']} {row['股票简称']:8s} 增仓:{row['增仓占比']:6.2f}% 主力净额:{row['主力净额']/1e8:6.2f}亿 成交额:{row['成交额']/1e8:6.2f}亿 涨跌:{row['涨跌幅']}")

        print("\n[OK] 测试完成！")
    else:
        print("[ERROR] 获取的数据为空")

except Exception as e:
    print(f"\n[ERROR] 发生错误: {e}")
    import traceback
    traceback.print_exc()

print("=" * 80)
