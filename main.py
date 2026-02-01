import rank_flow as rf
import pandas as pd
import time
import config
import predictor

def process_and_save(period, sort_by, top_n=20, filename_prefix="排名"):
    print(f"\n[{period}] 正在处理...")
    
    # 1. 检查更新 & 读取数据
    df = rf.get_fund_flow_data(period=period)
    
    if not df.empty:
        # 2. 排名/处理
        print(f"[{period}] 获取成功，正在排名...")
        
        # 对于N日排行，虽然无法计算增仓占比，但也显示以供参考
        if '日排行' in period and '增仓占比' not in df.columns:
             df['增仓占比'] = float('nan') # 标记缺失
             
        ranked_df = rf.rank_fund_flow(df, sort_by=sort_by, top_n=top_n)
        
        # 3. 输出展示 & 4. 保存本地 (结果)
        save_name = f"{filename_prefix}_{period}"
        formatted_df = rf.save_to_csv(ranked_df, save_name)
        
        print(f"\n--- {period} Top {top_n} (按{sort_by}排序, 单位已格式化) ---")
        if formatted_df is not None:
            print(formatted_df)
        else:
            print(ranked_df)
            
        if '日排行' in period:
            if sort_by == 'ratio':
                print(f"*注: {period}增仓占比是基于换手率反推成交额估算得出，数值仅供参考。")
            else:
                print(f"*注: 当前按资金净流入额排序。")

        # 5. AI 智能预测 (仅对即时数据且配置开启时执行)
        if period == '即时' and config.ENABLE_PREDICTION:
            try:
                pred_df = predictor.run_predictions(ranked_df, top_n=config.PREDICT_TOP_N)
                if pred_df is not None:
                    print("\n--- AI 智能预测结果 ---")
                    print(pred_df)
                    rf.save_to_csv(pred_df, f"AI预测_{period}")
            except Exception as e:
                print(f"智能预测执行出错: {e}")

    else:
        print(f"[{period}] 未获取到数据")

if __name__ == "__main__":
    current_date = pd.Timestamp.now().strftime("%Y-%m-%d")
    print(f"开始执行资金流向分析 - {current_date}")
    
    # 清理过期结果文件
    print("正在检查并清理过期结果文件...")
    rf.clean_old_files('analysis_results', days=7)
    
    # 即时数据 - 按增仓占比
    process_and_save(period='即时', sort_by='ratio', filename_prefix="增仓占比")

    # 3日数据 - 按增仓占比
    process_and_save(period='3日排行', sort_by='ratio', filename_prefix="增仓占比")

    # 5日数据 - 按增仓占比
    process_and_save(period='5日排行', sort_by='ratio', filename_prefix="增仓占比")

    print("\n所有任务完成。")
