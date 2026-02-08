"""
测试正确的修复：直接使用即时数据的净额字段
"""
import sqlite3
import os
from rank_flow import get_fund_flow_data

print("=" * 80)
print("测试正确的修复")
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
print("[1] 获取即时数据...")
df = get_fund_flow_data(period='即时')

if not df.empty:
    print(f"[OK] 成功获取 {len(df)} 行数据\n")
    
    # 检查关键字段
    print("[2] 检查关键字段:")
    for field in ['净额', '主力净额', '成交额', '增仓占比']:
        exists = field in df.columns
        print(f"  {field}: {'[OK]' if exists else '[MISS]'}")
    
    # 验证计算
    if all(f in df.columns for f in ['净额', '主力净额', '成交额', '增仓占比']):
        print("\n[3] 验证计算公式:")
        sample = df.head(3)
        for idx, row in sample.iterrows():
            net = row['净额']
            turnover = row['成交额']
            ratio = row['增仓占比']
            expected = (net / turnover * 100) if turnover != 0 else 0
            print(f"  {row['股票简称']:8s}: 净额={net/1e8:7.2f}亿 成交额={turnover/1e8:7.2f}亿 增仓占比={ratio:6.2f}% (预期={expected:6.2f}%)")
    
    # 显示Top 10
    if '增仓占比' in df.columns:
        print("\n[4] 增仓占比Top 10:")
        top_10 = df.nlargest(10, '增仓占比')[['股票代码', '股票简称', '增仓占比', '净额', '成交额', '涨跌幅']]
        for _, row in top_10.iterrows():
            print(f"  {row['股票代码']} {row['股票简称']:8s} 增仓:{row['增仓占比']:6.2f}% 净额:{row['净额']/1e8:6.2f}亿 成交额:{row['成交额']/1e8:6.2f}亿")
    
    print("\n[OK] 测试完成！")
else:
    print("[ERROR] 获取数据失败")

print("=" * 80)
