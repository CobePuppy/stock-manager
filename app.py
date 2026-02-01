import streamlit as st
import pandas as pd
import rank_flow as rf
import predictor
import config
import json
import os
import time
import database

# --- Configuration & Utility Functions ---
st.set_page_config(page_title="Stock Manager AI", page_icon="📈", layout="wide")

# 初始化数据库
if not os.path.exists(config.DB_PATH) or not os.path.exists(config.HISTORY_DB_PATH):
    database.init_all_dbs()

def load_watchlist():
    return database.get_watchlist()

def save_watchlist(codes):
    database.update_watchlist(codes)

def get_data_cache_key(period):
    return f"data_{period}_{pd.Timestamp.now().strftime('%Y%m%d_%H')}"

def format_money_for_show(val):
    if isinstance(val, (int, float)):
        if abs(val) > 100000000:
            return f"{val/100000000:.2f}亿"
        elif abs(val) > 10000:
            return f"{val/10000:.2f}万"
        return f"{val:.2f}"
    return val

# --- CSS Styling for "Hover Sidebar" feel (Optional) ---
# Streamlit sidebar is click-to-open on mobile, but fixed on desktop.
# We can't easily make it hover-expand without custom components, 
# but we can style the buttons nicely.
st.markdown("""
<style>
    .css-1d391kg {padding-top: 1rem;} 
</style>
""", unsafe_allow_html=True)

# --- Sidebar Navigation ---
with st.sidebar:
    st.title("📈 股市 AI 助手")
    st.markdown("---")
    selected_page = st.radio(
        "功能菜单",
        ["🔍 智能选股", "🤖 AI 预测分析", "⭐ 自选关注", "🔙 策略回测"],
        index=0
    )
    st.markdown("---")
    st.markdown("**系统状态**")
    if config.ENABLE_PREDICTION:
        st.success(f"AI 预测: 已启用 ({config.LLM_MODEL})")
    else:
        st.warning("AI 预测: 未开启")

# --- Page 1: 智能选股 ---
if selected_page == "🔍 智能选股":
    st.header("🔍 资金流向智能选股")
    
    # 获取当前时间用于展示数据更新状态
    current_time_str = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    st.markdown(f"> 🕒 **最后更新时间:** {current_time_str}")

    with st.expander("📊 查看计算公式说明", expanded=False):
        st.markdown("""
        - **增仓占比**: `(净流入额 / 总成交额) * 100%`
        """)
    
    col1, col2, col3 = st.columns(3)
    period = None
    
    # 周期选择
    with col1:
        if st.button("查看 即时 增仓排名", use_container_width=True):
            period = "即时"
    with col2:
        if st.button("查看 3日 增仓排名", use_container_width=True):
            period = "3日排行"
    with col3:
        if st.button("查看 5日 增仓排名", use_container_width=True):
            period = "5日排行"
            
    # 如果没有点击按钮，保持上一次的状态（利用Session State）
    if period:
        st.session_state['last_period'] = period
    elif 'last_period' in st.session_state:
        period = st.session_state['last_period']
    else:
        period = "即时" # 默认

    # 计算显示日期
    trade_date = database.get_stock_trade_date()
    
    st.subheader(f"📅 当前展示: {period} 数据 (数据日期: {trade_date})")
    
    refresh = st.button("🔄 刷新数据")
    
    if period:
        # 获取数据的Loading状态
        with st.spinner('正在分析全市场资金流向...'):
            try:
                # 尝试获取数据
                if refresh or f'df_{period}' not in st.session_state:
                    df = rf.get_fund_flow_data(period=period)
                    if '日排行' in period and '增仓占比' not in df.columns:
                        df['增仓占比'] = float('nan')
                    st.session_state[f'df_{period}'] = df
                
                df = st.session_state.get(f'df_{period}')

                if df is not None and not df.empty:
                    # 排名计算
                    sort_by = 'ratio' if '增仓占比' in df.columns else 'net'
                    # 传入 period 参数以触发自动保存(如果是即时数据)
                    ranked_df = rf.rank_fund_flow(df, sort_by=sort_by, top_n=config.TOP_N, period=period)
                    
                    # 格式化展示
                    display_df = ranked_df.copy()
                    
                    # 提前对金额列进行单位转换，避免 Streamlit 默认按数字显示导致位数过长
                    # 复用 rank_flow.py 中的保存逻辑（它返回格式化后的 df），但这里我们只做展示转换，保留原 df 用于计算
                    
                    # 定义转换函数 (复用 rank_flow.py 的逻辑)
                    def format_money_for_show(val):
                        if isinstance(val, (int, float)):
                            if abs(val) > 100000000:
                                return f"{val/100000000:.2f}亿"
                            elif abs(val) > 10000:
                                return f"{val/10000:.2f}万"
                            return f"{val:.2f}"
                        return val

                    # 需要格式化的金额列
                    money_cols = ['净额', '成交额', '资金流入净额', '流通市值', '最新价'] # 最新价一般不转，但如果很大也可以
                    for c in money_cols:
                        if c in display_df.columns:
                            # 仅针对数值类型进行转换，如果已经是字符串则不处理
                            if pd.api.types.is_numeric_dtype(display_df[c]):
                                display_df[c] = display_df[c].apply(format_money_for_show)

                    st.dataframe(
                        display_df,
                        width="stretch",
                        column_config={
                            "股票代码": st.column_config.TextColumn("代码"),
                            "增仓占比": st.column_config.NumberColumn("增仓占比", format="%.2f%%"),
                        }
                    )
                    
                    # 快捷操作区
                    st.markdown("### 🛠️ 批量操作")
                    
                    # 批量加入自选
                    # 构造选项列表: "600355 ST精伦"
                    display_df['label'] = display_df['股票代码'] + " " + display_df['股票简称']
                    to_add_labels = st.multiselect("选择加入自选的股票", display_df['label'].tolist())
                    
                    if st.button("加入自选"):
                        current_wl = load_watchlist()
                        # 从label还原出代码
                        to_add_codes = [label.split(" ")[0] for label in to_add_labels]
                        updated_wl = list(set(current_wl + to_add_codes))
                        save_watchlist(updated_wl)
                        st.success(f"已添加 {len(to_add_codes)} 只股票到自选")
                else:
                    st.error("未能获取数据，请检查网络或稍后重试")
            except Exception as e:
                st.error(f"发生错误: {e}")

# --- Page 2: AI 预测 ---
elif selected_page == "🤖 AI 预测分析":
    st.header("🤖 AI 智能交易预测")
    
    if not config.ENABLE_PREDICTION:
        st.error("请在 config.py 中将 ENABLE_PREDICTION 设置为 True 并配置 API Key")
    else: 
        tab1, tab2 = st.tabs(["📊 批量预测 (Top N)", "🎯 单股诊断"])
        
        with tab1:
            st.write(f"当前配置: 预测排名前 {config.PREDICT_TOP_N} 的股票")
            
            # 检查是否有来自选股页面的数据
            target_df = st.session_state.get('prediction_target')
            
            if target_df is None:
                st.info("尚未选择数据。请先去 '智能选股' 页面获取数据，或点击下方按钮直接获取即时 Top 数据。")
                if st.button("获取此页面的即时 Top 数据"):
                     df = rf.get_fund_flow_data(period='即时')
                     if not df.empty:
                        target_df = rf.rank_fund_flow(df, sort_by='ratio', top_n=config.PREDICT_TOP_N, period='即时')
                        st.session_state['prediction_target'] = target_df
                        st.rerun()
            
            if target_df is not None:
                # 格式化展示
                display_df = target_df.copy()
                money_cols = ['净额', '成交额', '资金流入净额', '流通市值', '最新价']
                for c in money_cols:
                    if c in display_df.columns:
                        if pd.api.types.is_numeric_dtype(display_df[c]):
                            display_df[c] = display_df[c].apply(format_money_for_show)
                
                st.markdown("### 📋 待分析股票列表")
                st.dataframe(
                    display_df,
                    width="stretch",
                    column_config={
                        "股票代码": st.column_config.TextColumn("代码"),
                        "增仓占比": st.column_config.NumberColumn("增仓占比", format="%.2f%%"),
                        "换手率": st.column_config.NumberColumn("换手率", format="%.2f%%"),
                        "涨跌幅": st.column_config.NumberColumn("涨跌幅", format="%.2f%%"),
                    }
                )

                if st.button("🚀 开始 AI 分析"):
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    results = []
                    predictor_instance = predictor.StockPredictor()
                    
                    total = len(target_df)
                    for i, (index, row) in enumerate(target_df.iterrows()):
                        code = row['股票代码']
                        name = row['股票简称']
                        status_text.text(f"正在分析 ({i+1}/{total}): {name} {code} ...")
                        
                        # 补充基本面和新闻 (需确保 predictor 有这些方法，若上一步已更新则可直接用)
                        # 这里我们直接调用 predict，它内部会去 fetch 那些数据
                        pred = predictor_instance.predict(code, name, row)
                        
                        res_row = {
                            "股票代码": code,
                            "股票简称": name,
                            "推荐买入": pred.get("buy", "分析失败"),
                            "推荐卖出": pred.get("sell", "分析失败"),
                            "时间节点": pred.get("time", "分析失败")
                        }
                        results.append(res_row)
                        progress_bar.progress((i + 1) / total)
                    
                    status_text.text("分析完成！")
                    res_df = pd.DataFrame(results)
                    st.table(res_df)
                    
                    # 下载
                    csv = res_df.to_csv(index=False).encode('utf-8-sig')
                    st.download_button("📥 下载预测报告", csv, "AI_Prediction.csv", "text/csv")
        
        with tab2:
            st.write("输入代码进行单独诊断")
            code_input = st.text_input("股票代码 (如 000001)")
            name_input = st.text_input("股票简称 (可选)")
            
            if st.button("开始诊断"):
                if code_input:
                    with st.spinner("正在搜集数据并思考..."):
                        # 构造一个伪造的 row 数据，因为 predict 需要 row
                        
                        # 尝试从全市场数据中捞
                        found_row = None
                        # 先检查缓存
                        if 'df_即时' in st.session_state:
                            full_df = st.session_state['df_即时']
                            match = full_df[full_df['股票代码'] == code_input]
                            if not match.empty:
                                found_row = match.iloc[0]
                        
                        # 如果没缓存或缓存未命中，尝试现场获取一次该个股实时数据 (借助 akshare 接口或 rank_flow 逻辑)
                        # 为了兼容性，这里我们直接调用 get_fund_flow_data('即时') 重新拉取一次全量(如果有缓存机制会快)
                        # 或者为了节省时间，构造一个仅包含基本信息的Series，资金流数据暂时留空或填入"未知"
                        if found_row is None:
                             st.info("⚠️ 未在当前缓存中找到该股，正在尝试获取实时数据...")
                             # 简单策略：不强制拉全量，而是告诉AI资金流数据缺失，请依据技术/消息面
                             # 或者更优：如果 predictor 支持单独获取个股资金流 better. 
                             # 现阶段我们构建一个基础对象，避免程序报错。
                             
                             # 尝试获取个股实时行情作为资金流的替代参考（如果需要）
                             try:
                                 # 只是为了拿到正确的名字如果用户没填
                                 if not name_input:
                                     # 简单查一下名字，这里可以用 ak.stock_individual_info_em 获取名称，但比较慢
                                     # 暂且用 code 代替
                                     name_input = code_input
                                 
                                 found_row = pd.Series({
                                     '股票代码': code_input, 
                                     '股票简称': name_input,
                                     '最新价': '未知',
                                     '净额': '未知',
                                     '增仓占比': '未知'
                                 })
                             except:
                                 pass

                        predictor_instance = predictor.StockPredictor()
                        pred = predictor_instance.predict(code_input, name_input or code_input, found_row)
                        
                        st.markdown("### 📊 AI 诊断报告")
                        
                        if "error" in pred:
                            st.error(f"❌ 分析出错: {pred['error']}")
                        elif "text" in pred and "buy" not in pred:
                            st.info("💡 AI 给出的原始建议:")
                            st.markdown(pred["text"])
                        else:
                            # 提取数据
                            buy_price = pred.get("buy", "N/A")
                            sell_price = pred.get("sell", "N/A")
                            time_point = pred.get("time", "N/A")
                            
                            # 渲染美观的卡片
                            st.markdown("""
                            <style>
                            .trade-card {
                                background-color: #f8f9fa;
                                border: 1px solid #e9ecef;
                                border-radius: 8px;
                                padding: 20px;
                                margin-top: 10px;
                                border-left: 5px solid #4CAF50;
                            }
                            .price-label { font-size: 14px; color: #666; font-weight: 500;}
                            .buy-price { font-size: 24px; color: #d32f2f; font-weight: bold; } /* 红色买入 (中国习惯) */
                            .sell-price { font-size: 24px; color: #388e3c; font-weight: bold; } /* 绿色卖出 */
                            .time-block { 
                                margin-top: 20px; 
                                background-color: #e3f2fd; 
                                padding: 10px; 
                                border-radius: 5px; 
                                border-left: 3px solid #2196f3;
                            }
                            </style>
                            """, unsafe_allow_html=True)
                            
                            # 使用 HTML 展示主要指标
                            c1, c2 = st.columns(2)
                            with c1:
                                st.markdown(f"<div><span class='price-label'>🔴 建议低价入场</span><br><span class='buy-price'>{buy_price}</span></div>", unsafe_allow_html=True)
                            with c2:
                                st.markdown(f"<div><span class='price-label'>🟢 建议高抛区间</span><br><span class='sell-price'>{sell_price}</span></div>", unsafe_allow_html=True)
                            
                            st.markdown("---")
                            st.markdown(f"**⏰ 关键变盘/操作节点:**")
                            st.info(time_point, icon="🕒")
                else:
                    st.warning("请输入代码")

# --- Page 3: 自选关注 ---
elif selected_page == "⭐ 自选关注":
    st.header("⭐ 自选股观察")
    
    watchlist = load_watchlist()
    
    # 添加栏
    new_code = st.text_input("添加股票代码", placeholder="输入代码后回车")
    if new_code:
        if new_code not in watchlist:
            watchlist.append(new_code)
            save_watchlist(watchlist)
            st.success(f"已添加 {new_code}")
            st.rerun() # 立即刷新
        else:
            st.warning("已在列表中")

    if not watchlist:
        st.info("暂无自选股，快去'智能选股'页面添加吧！")
    else:
        # 尝试从即时数据中匹配自选股的资金流
        if st.button("刷新自选股数据"):
            with st.spinner("正在获取最新数据..."):
                df_all = rf.get_fund_flow_data(period='即时')
                if not df_all.empty:
                    # Filter
                    df_watch = df_all[df_all['股票代码'].isin(watchlist)]
                    if not df_watch.empty:
                         # 简单的排序
                        st.dataframe(df_watch, width="stretch")
                        
                        # AI 分析自选
                        if st.button("🤖 AI 分析所有自选股"):
                            st.session_state['prediction_target'] = df_watch
                            st.info("请切换到 'AI 预测分析' 页面查看结果")
                    else:
                        st.warning("自选股均未在当前市场排名前列或数据获取失败")
        
        st.markdown("### 自选列表管理")
        for code in watchlist:
            c1, c2 = st.columns([4, 1])
            c1.text(f"股票代码: {code}")
            if c2.button("删除", key=f"del_{code}"):
                watchlist.remove(code)
                save_watchlist(watchlist)
                st.rerun()

# --- Page 4: 策略回测 ---
elif selected_page == "🔙 策略回测":
    st.header("🔙 历史回测 (开发中)")
    st.info("此功能正在紧锣密鼓开发中... 👴")
    st.markdown("""
    **计划功能:**
    1. 设定 AI 推荐的买入卖出规则
    2. 基于过去 1 年数据进行模拟交易
    3. 生成收益率曲线和回撤分析
    """)
