import akshare as ak
import pandas as pd
import time

def test_main_list_api():
    sf = ak.stock_fund_flow_individual(symbol="即时")
    print(f"获取到 {len(sf)} 只股票的资金流向数据")
    #筛选出股票代码以6 3 0开头的行
    sf['股票代码'] = sf['股票代码'].astype('str').str.zfill(6)  # 确保股票代码是6位数，前面补0
    sf = sf[sf['股票代码'].str.startswith(('6', '3', '0'))]
    print(f"筛选后剩余 {len(sf)} 只股票的资金流向数据") 
    #保存股票代码列表到CSV
    sf.to_csv('filtered_stock_fund_flow.csv', index=False)
    print("已保存股票代码列表到filtered_stock_fund_flow.csv")


def test_single_stock_api(stock_code)-> bool:
    #从CSV读取股票代码列表
    try:
        sf = pd.read_csv('filtered_stock_fund_flow.csv')
    except Exception as e:
        print(f"读取CSV文件失败: {e}")
        return False
    print("\n" + "="*50)
    print("【测试 2】获取单只股票资金流接口: ak.stock_individual_fund_flow")
    # stock_code = "600000"
    #6 开头是上海，3和0开头是深圳，所以这里默认测试上海的股票，如果需要测试深圳的，可以根据股票代码前缀判断市场并调整market参
    if(stock_code.startswith('6')):
        market = "sh"
    else:
        market = "sz"
    print(f"正在测试股票: {market}{stock_code} 的资金流数据获取...")
    
    try:
        df = ak.stock_individual_fund_flow(stock=stock_code, market=market)
        if not df.empty:
            print("获取成功！")
            print("列名列表:", df.columns.tolist())
            print("\n最近5行数据:")
            print(df.tail(5).to_string())
            
            # 检查最后一行是否是当天的
            latest_date = df.iloc[-1].get('日期', '未知')
            print(f"\n最新数据日期: {latest_date}")
        else:
            print("返回数据为空。")
    except Exception as e:
        print(f"接口调用失败: {e}")
        return False

if __name__ == "__main__":
    test_main_list_api()
    count = 0
    #在CSV中轮询每只股票代码，调用test_single_stock_api测试接口是否正常\
    #只请求前100行

    with open('filtered_stock_fund_flow.csv', 'r', encoding='utf-8') as f:
        lines = f.readlines()[:100]
    for line in lines:
        stock_code = line.split(',')[1]  #假设股票代码在第二列
        time.sleep(0.4)  # 避免请求过快导致被封禁
        if test_single_stock_api(stock_code):
            count += 1
    print(f"成功获取 {count} 条数据")
        
    
   # test_single_stock_api()
