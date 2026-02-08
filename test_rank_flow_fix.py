"""
测试修复后的rank_flow.py是否能正确获取超大单数据并计算增仓占比
"""
import sys
import pandas as pd
from rank_flow import get_fund_flow_data

print("=" * 80)
print("测试修复后的rank_flow.py")
print("=" * 80)

try:
    # 清除缓存（如果存在）
    import sqlite3
    import os
    db_file = 'stock_data.db'
    if os.path.exists(db_file):
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM fund_flow_cache WHERE period_type = '即时'")
        conn.commit()
        conn.close()
        print("[OK] 已清除即时数据缓存")

    # 获取即时数据
    print("\n[1] 获取即时数据...")
    df = get_fund_flow_data(period='即时')

    if not df.empty:
        print(f"[OK] 成功获取 {len(df)} 行数据")
        print(f"列名: {df.columns.tolist()}")

        # 检查关键字段
        print("\n[2] 检查关键字段:")
        key_fields = ['股票代码', '股票简称', '最新价', '涨跌幅', '换手率', '成交额', '主力净额', '增仓占比']
        for field in key_fields:
            if field in df.columns:
                print(f"  [OK] {field}")
            else:
                print(f"  [MISS] {field} (缺失)")

        # 查看有增仓占比的股票数量
        if '增仓占比' in df.columns:
            valid_count = df['增仓占比'].notna().sum()
            positive_count = (df['增仓占比'] > 0).sum()
            print(f"\n[3] 增仓占比统计:")
            print(f"  有效数据: {valid_count} / {len(df)}")
            print(f"  正增仓(>0): {positive_count}")

            # 显示增仓占比最高的前10只股票
            if positive_count > 0:
                top_10 = df.nlargest(10, '增仓占比')[['股票代码', '股票简称', '增仓占比', '主力净额', '成交额', '涨跌幅']]
                print("\n[4] 增仓占比Top 10:")
                print(top_10.to_string(index=False))

        # 查找003018（如果存在）
        print("\n[5] 查找003018:")
        df_003018 = df[df['股票代码'] == '003018']
        if not df_003018.empty:
            print("  找到了！数据如下:")
            for col in ['股票代码', '股票简称', '主力净额', '成交额', '增仓占比', '涨跌幅', '换手率']:
                if col in df_003018.columns:
                    value = df_003018[col].values[0]
                    print(f"    {col}: {value}")
        else:
            print("  未找到003018")

        print("\n[OK] 测试完成！")
    else:
        print("[ERROR] 获取的数据为空")

except Exception as e:
    print(f"\n[ERROR] 发生错误: {e}")
    import traceback
    traceback.print_exc()

print("=" * 80)
