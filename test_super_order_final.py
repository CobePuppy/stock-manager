"""
测试最终的超大单数据获取方案
"""
import sqlite3
import os
from rank_flow import get_fund_flow_data

print("=" * 80)
print("测试超大单数据获取（并发+200ms延迟）")
print("=" * 80)

# 清除缓存
db_file = 'stock_data.db'
if os.path.exists(db_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM fund_flow_cache WHERE period_type = '即时'")
    conn.commit()
    conn.close()
    print("[OK] 已清除缓存\n")

# 获取数据
print("[1] 开始获取即时数据...")
df = get_fund_flow_data(period='即时')

if not df.empty:
    print(f"\n[OK] 最终获取 {len(df)} 行数据")

    # 检查关键字段
    print("\n[2] 检查关键字段:")
    for field in ['股票代码', '股票简称', '成交额', '超大单净额', '主力净额', '增仓占比']:
        exists = field in df.columns
        print(f"  {field}: {'[OK]' if exists else '[MISS]'}")

    # 检查数据质量
    if '超大单净额' in df.columns:
        valid_super = df['超大单净额'].notna().sum()
        nonzero_super = (df['超大单净额'] != 0).sum()
        print(f"\n[3] 超大单数据质量:")
        print(f"  有效数据: {valid_super}/{len(df)}")
        print(f"  非零数据: {nonzero_super}/{len(df)}")

    # 显示Top 10
    if '增仓占比' in df.columns:
        print("\n[4] 增仓占比Top 10（基于超大单净额）:")
        top_10 = df.nlargest(10, '增仓占比')[['股票代码', '股票简称', '增仓占比', '超大单净额', '成交额', '涨跌幅']]
        for _, row in top_10.iterrows():
            print(f"  {row['股票代码']} {row['股票简称']:8s} "
                  f"增仓:{row['增仓占比']:6.2f}% "
                  f"超大单:{row['超大单净额']/1e8:6.2f}亿 "
                  f"成交额:{row['成交额']/1e8:6.2f}亿 "
                  f"涨跌:{row['涨跌幅']}")

    print("\n[OK] 测试完成！")
else:
    print("[ERROR] 获取数据失败")

print("=" * 80)
