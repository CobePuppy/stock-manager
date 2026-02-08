import akshare as ak

print('测试 stock_individual_fund_flow 接口（单个股票）')
print('=' * 80)

# 测试获取单个股票的资金流数据
try:
    # 以600016为例
    df = ak.stock_individual_fund_flow(stock='600016', market='sh')
    print(f'\n成功获取数据: {len(df)}行, {len(df.columns)}列')
    print(f'\n列名: {df.columns.tolist()}')
    print(f'\n最近5天数据:')
    print(df.tail(5))
except Exception as e:
    print(f'失败: {e}')
    import traceback
    traceback.print_exc()

print('=' * 80)
