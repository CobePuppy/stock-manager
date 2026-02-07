"""
精确定位 Length mismatch 错误
"""
import akshare as ak
import pandas as pd
import numpy as np

def convert_unit(x):
    """单位转换"""
    if pd.isna(x):
        return 0
    try:
        if isinstance(x, str):
            x = x.replace(',', '')
            if '亿' in x:
                return float(x.replace('亿', '')) * 100000000
            elif '万' in x:
                return float(x.replace('万', '')) * 10000
        return float(x)
    except:
        return 0

print("=" * 80)
print("开始逐步调试超大单数据处理")
print("=" * 80)

try:
    # 步骤1: 获取即时数据
    print("\n[步骤1] 获取即时数据...")
    df_instant = ak.stock_fund_flow_individual(symbol='即时')
    print(f"  即时数据: {len(df_instant)} 行, {len(df_instant.columns)} 列")
    print(f"  列名: {df_instant.columns.tolist()}")

    # 步骤2: 获取超大单数据
    print("\n[步骤2] 获取超大单数据...")
    df_super = ak.stock_fund_flow_individual(symbol='超大单')
    print(f"  超大单数据: {len(df_super)} 行, {len(df_super.columns)} 列")
    print(f"  列名: {df_super.columns.tolist()}")
    print(f"  索引类型: {type(df_super.index)}")
    print(f"  索引范围: {df_super.index.min()} ~ {df_super.index.max()}")

    # 步骤3: 重置索引
    print("\n[步骤3] 重置索引...")
    df_super = df_super.reset_index(drop=True)
    print(f"  重置后: {len(df_super)} 行, {len(df_super.columns)} 列")
    print(f"  索引范围: {df_super.index.min()} ~ {df_super.index.max()}")

    # 步骤4: 处理股票代码
    print("\n[步骤4] 处理股票代码...")
    df_super['股票代码'] = df_super['股票代码'].astype(str).str.zfill(6)
    print(f"  处理后: {len(df_super)} 行, {len(df_super.columns)} 列")

    # 步骤5: 过滤股票代码
    print("\n[步骤5] 过滤股票代码（关键步骤）...")
    print(f"  过滤前: {len(df_super)} 行, {len(df_super.columns)} 列")
    print(f"  过滤前索引: {df_super.index[:5].tolist()}")

    # 尝试不同的过滤方式
    print("\n  方式A: 使用 .loc")
    try:
        mask = df_super['股票代码'].str.startswith(('6', '3', '0'))
        df_test_a = df_super.loc[mask].copy()
        print(f"    成功: {len(df_test_a)} 行, {len(df_test_a.columns)} 列")
    except Exception as e:
        print(f"    失败: {e}")

    print("\n  方式B: 使用布尔索引 + .copy()")
    try:
        df_test_b = df_super[df_super['股票代码'].str.startswith(('6', '3', '0'))].copy()
        print(f"    成功: {len(df_test_b)} 行, {len(df_test_b.columns)} 列")
    except Exception as e:
        print(f"    失败: {e}")

    print("\n  方式C: 分步操作")
    try:
        mask = df_super['股票代码'].str.startswith(('6', '3', '0'))
        print(f"    mask 类型: {type(mask)}, 长度: {len(mask)}")
        df_filtered = df_super[mask]
        print(f"    过滤结果: {len(df_filtered)} 行, {len(df_filtered.columns)} 列")
        df_test_c = df_filtered.copy()
        print(f"    复制后: {len(df_test_c)} 行, {len(df_test_c.columns)} 列")
    except Exception as e:
        print(f"    失败: {e}")

    # 步骤6: 再次重置索引
    print("\n[步骤6] 再次重置索引...")
    df_super = df_test_b  # 使用方式B的结果
    print(f"  重置前: {len(df_super)} 行, {len(df_super.columns)} 列")
    df_super = df_super.reset_index(drop=True)
    print(f"  重置后: {len(df_super)} 行, {len(df_super.columns)} 列")

    # 步骤7: 转换净额单位
    print("\n[步骤7] 转换净额单位...")
    if '净额' in df_super.columns:
        print(f"  转换前: {len(df_super)} 行, {len(df_super.columns)} 列")
        print(f"  净额列类型: {df_super['净额'].dtype}")
        df_super['净额'] = df_super['净额'].apply(convert_unit)
        print(f"  转换后: {len(df_super)} 行, {len(df_super.columns)} 列")

    # 步骤8: 创建映射
    print("\n[步骤8] 创建映射字典...")
    super_net_map = df_super.set_index('股票代码')['净额'].to_dict()
    print(f"  映射字典: {len(super_net_map)} 个股票")

    # 步骤9: 测试映射到即时数据
    print("\n[步骤9] 测试映射...")
    df_instant['股票代码'] = df_instant['股票代码'].astype(str).str.zfill(6)
    print(f"  即时数据: {len(df_instant)} 行, {len(df_instant.columns)} 列")
    df_instant['主力净额'] = df_instant['股票代码'].map(super_net_map)
    print(f"  映射后: {len(df_instant)} 行, {len(df_instant.columns)} 列")
    print(f"  [OK] 成功！")

    # 显示结果
    print("\n[结果] 前5条数据:")
    print(df_instant[['股票代码', '股票简称', '主力净额']].head(5))

except Exception as e:
    print(f"\n[ERROR] 发生错误: {e}")
    import traceback
    print("\n完整错误堆栈:")
    traceback.print_exc()

print("\n" + "=" * 80)
