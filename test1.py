"""
步骤1: 测试获取即时数据
"""
import akshare as ak

print("=" * 80)
print("步骤1: 测试获取即时数据")
print("=" * 80)

try:
    print("\n正在获取即时数据...")
    df = ak.stock_fund_flow_individual(symbol='即时')
    
    print(f"[OK] 成功获取 {len(df)} 只股票")
    print(f"\n列名: {df.columns.tolist()}")
    
    print(f"\n前3行数据:")
    print(df.head(3))
    
    # 筛选6、3、0开头的股票
    df['股票代码'] = df['股票代码'].astype(str).str.zfill(6)
    df_filtered = df[df['股票代码'].str.startswith(('6', '3', '0'))]
    
    print(f"\n[OK] 筛选后剩余 {len(df_filtered)} 只股票（6、3、0开头）")
    print(f"前3只股票代码: {df_filtered['股票代码'].head(3).tolist()}")
    
except Exception as e:
    print(f"[ERROR] {e}")
    import traceback
    traceback.print_exc()

print("=" * 80)
