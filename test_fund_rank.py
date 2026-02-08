import akshare as ak

print('测试 stock_individual_fund_flow_rank 接口')
print('=' * 80)

# 测试"今日"参数
try:
    df = ak.stock_individual_fund_flow_rank(indicator='今日')
    print(f'\n成功获取数据: {len(df)}行, {len(df.columns)}列')
    print(f'\n列名: {df.columns.tolist()}')
    print(f'\n前5行:')
    print(df.head())
except Exception as e:
    print(f'失败: {e}')
    import traceback
    traceback.print_exc()
