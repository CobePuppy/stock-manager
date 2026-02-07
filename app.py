import streamlit as st
import pandas as pd
import rank_flow as rf
import predictor
import config
import json
import os
import time
import database
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import akshare as ak

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

def fetch_and_plot_kline(stock_code, stock_name=None):
    """获取并绘制K线图 (带均线和成交量)"""
    try:
        from datetime import datetime
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - pd.Timedelta(days=120)).strftime("%Y%m%d")
        
        # 获取日线一般历史数据
        df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
        
        if df.empty:
            st.warning(f"无法获取 {stock_code} 的K线数据")
            return

        # 确保按日期升序
        df = df.sort_values('日期')
        
        # 计算均线
        df['MA5'] = df['收盘'].rolling(window=5).mean()
        df['MA10'] = df['收盘'].rolling(window=10).mean()
        df['MA20'] = df['收盘'].rolling(window=20).mean()

        # 创建子图: 行1 K线, 行2 成交量
        fig = make_subplots(
            rows=2, cols=1, 
            shared_xaxes=True, 
            vertical_spacing=0.03, 
            row_heights=[0.7, 0.3],
            subplot_titles=(f'{stock_name or stock_code} 日K线图', '成交量')
        )

        # 1. K线
        fig.add_trace(go.Candlestick(
            x=df['日期'],
            open=df['开盘'], high=df['最高'], low=df['最低'], close=df['收盘'],
            name='K线',
            increasing_line_color='#ef5350', decreasing_line_color='#26a69a'
        ), row=1, col=1)

        # 2. 均线
        fig.add_trace(go.Scatter(x=df['日期'], y=df['MA5'], name='MA5', line=dict(color='black', width=1)), row=1, col=1)
        fig.add_trace(go.Scatter(x=df['日期'], y=df['MA10'], name='MA10', line=dict(color='orange', width=1)), row=1, col=1)
        fig.add_trace(go.Scatter(x=df['日期'], y=df['MA20'], name='MA20', line=dict(color='purple', width=1)), row=1, col=1)

        # 3. 成交量
        # 颜色根据涨跌: 收盘 >= 开盘 为红, 否则为绿
        colors = ['#ef5350' if c >= o else '#26a69a' for c, o in zip(df['收盘'], df['开盘'])]
        fig.add_trace(go.Bar(
            x=df['日期'], y=df['成交量'], 
            name='成交量',
            marker_color=colors
        ), row=2, col=1)

        # 布局调整
        fig.update_layout(
            xaxis_rangeslider_visible=False,
            height=600,
            margin=dict(l=50, r=50, t=30, b=30),
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"绘制K线图失败: {e}")

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

    # K线快速查看 - 优化版（默认展开）
    with st.expander("📈 个股K线快速查看", expanded=True):
        col_k1, col_k2, col_k3 = st.columns([2, 2, 1])

        with col_k1:
            kline_code = st.text_input("输入股票代码 (如 600519):", max_chars=6, key="kline_home")

        with col_k2:
            kline_name = st.text_input("或输入股票名称 (如 贵州茅台):", key="kline_name_home")

        # 处理名称搜索
        if kline_name and not kline_code:
            # 尝试从当前数据中查找
            df_current = st.session_state.get('df_即时') or st.session_state.get(f'df_{st.session_state.get("last_period", "即时")}')
            if df_current is not None and not df_current.empty:
                name_col = '股票简称' if '股票简称' in df_current.columns else '股票名称'
                matched = df_current[df_current[name_col].str.contains(kline_name, na=False, case=False)]
                if not matched.empty:
                    kline_code = matched.iloc[0]['股票代码']
                    st.info(f"找到股票: {matched.iloc[0][name_col]} ({kline_code})")
                else:
                    st.warning(f"未找到包含 '{kline_name}' 的股票")

        # 最近查看历史
        if 'kline_history' not in st.session_state:
            st.session_state['kline_history'] = []

        if kline_code and kline_code not in st.session_state['kline_history']:
            st.session_state['kline_history'].insert(0, kline_code)
            st.session_state['kline_history'] = st.session_state['kline_history'][:5]  # 只保留最近5个

        # 显示历史记录
        if st.session_state['kline_history']:
            st.markdown("**最近查看:** " + " | ".join([f"`{code}`" for code in st.session_state['kline_history']]))

        if kline_code:
            # 尝试获取股票名称
            df_current = st.session_state.get('df_即时') or st.session_state.get(f'df_{st.session_state.get("last_period", "即时")}')
            stock_name = None
            if df_current is not None and not df_current.empty:
                matched = df_current[df_current['股票代码'] == kline_code]
                if not matched.empty:
                    name_col = '股票简称' if '股票简称' in df_current.columns else '股票名称'
                    stock_name = matched.iloc[0][name_col]

            fetch_and_plot_kline(kline_code, stock_name)

    # 获取当前时间用于展示数据更新状态
    current_time_str = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    st.markdown(f"> 🕒 **最后更新时间:** {current_time_str}")
    
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
                    # 排名计算 - 优先使用综合评分（增仓+放量）
                    sort_by = 'comprehensive' if '增仓占比' in df.columns else 'net'
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
                    st.markdown("### 🛠️ 快捷操作")

                    # 构造选项列表: "600355 ST精伦"
                    name_col = '股票简称' if '股票简称' in ranked_df.columns else '股票名称'
                    ranked_df['label'] = ranked_df['股票代码'].astype(str) + " " + ranked_df[name_col].astype(str)

                    col_op1, col_op2 = st.columns(2)

                    with col_op1:
                        st.markdown("**📈 查看K线图**")
                        kline_select = st.selectbox(
                            "选择股票查看K线",
                            options=["请选择..."] + ranked_df['label'].tolist(),
                            key="kline_select_main"
                        )
                        if kline_select != "请选择...":
                            selected_code = kline_select.split(" ")[0]
                            selected_name = " ".join(kline_select.split(" ")[1:])
                            with st.expander(f"📊 {selected_name} ({selected_code}) K线图", expanded=True):
                                fetch_and_plot_kline(selected_code, selected_name)

                    with col_op2:
                        st.markdown("**⭐ 批量加入自选**")
                        to_add_labels = st.multiselect("选择股票", ranked_df['label'].tolist(), key="add_watchlist")
                        if st.button("加入自选", use_container_width=True):
                            if to_add_labels:
                                current_wl = load_watchlist()
                                # 从label还原出代码
                                to_add_codes = [label.split(" ")[0] for label in to_add_labels]
                                updated_wl = list(set(current_wl + to_add_codes))
                                save_watchlist(updated_wl)
                                st.success(f"✅ 已添加 {len(to_add_codes)} 只股票到自选")
                            else:
                                st.warning("请先选择股票")
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
                        target_df = rf.rank_fund_flow(df, sort_by='comprehensive', top_n=config.PREDICT_TOP_N, period='即时')
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
                        "当日量比": st.column_config.NumberColumn("当日量比", format="%.2f", help="今日成交量 / 过去5日均量"),
                        "换手率": st.column_config.NumberColumn("换手率", format="%.2f%%"),
                        "涨跌幅": st.column_config.NumberColumn("涨跌幅", format="%.2f%%"),
                    }
                )

                # 添加K线快速查看
                st.markdown("**📈 快速查看K线**")
                name_col = '股票简称' if '股票简称' in target_df.columns else '股票名称'
                stock_options = ["请选择..."] + (target_df['股票代码'].astype(str) + " " + target_df[name_col].astype(str)).tolist()
                kline_choice = st.selectbox("选择股票", stock_options, key="kline_predict")
                if kline_choice != "请选择...":
                    sel_code = kline_choice.split(" ")[0]
                    sel_name = " ".join(kline_choice.split(" ")[1:])
                    with st.expander(f"📊 {sel_name} ({sel_code}) K线图", expanded=True):
                        fetch_and_plot_kline(sel_code, sel_name)

                st.markdown("---")
                if st.button("🚀 开始 AI 分析"):
                    progress_bar = st.progress(0)
                    status_text = st.empty()

                    results = []
                    predictor_instance = predictor.StockPredictor()

                    total = len(target_df)
                    for i, (index, row) in enumerate(target_df.iterrows()):
                        code = row['股票代码']
                        name = row['股票简称']
                        status_text.text(f"正在深度分析 ({i+1}/{total}): {name} {code} ...")

                        # 调用AI进行深度分析
                        pred = predictor_instance.predict(code, name, row)

                        res_row = {
                            "股票代码": code,
                            "股票简称": name,
                            "综合评分": pred.get("comprehensive_score", "分析失败"),
                            "推荐买入": pred.get("buy", "分析失败"),
                            "推荐卖出": pred.get("sell", "分析失败"),
                            "建议仓位": pred.get("position", "分析失败"),
                            "时间节点": pred.get("time", "分析失败"),
                            "技术面分析": pred.get("technical_analysis", ""),
                            "资金面分析": pred.get("fund_analysis", ""),
                            "买入理由": pred.get("buy_reason", ""),
                            "卖出策略": pred.get("sell_reason", ""),
                            "风险提示": pred.get("risk", "")
                        }
                        results.append(res_row)
                        progress_bar.progress((i + 1) / total)

                    status_text.text("✅ 分析完成！")
                    res_df = pd.DataFrame(results)

                    # 保存到session_state供下载和查看
                    st.session_state['prediction_results'] = res_df

                    # 分标签页展示简要和详细信息
                    result_tab1, result_tab2 = st.tabs(["📊 简要概览", "📝 详细分析"])

                    with result_tab1:
                        st.markdown("### 📊 AI 分析概览")
                        summary_df = res_df[["股票代码", "股票简称", "综合评分", "推荐买入", "推荐卖出", "建议仓位"]].copy()
                        st.dataframe(summary_df, use_container_width=True)

                    with result_tab2:
                        st.markdown("### 📝 详细分析报告")
                        for idx, row in res_df.iterrows():
                            with st.expander(f"**{row['股票简称']} ({row['股票代码']})** - 综合评分: {row['综合评分']}", expanded=(idx == 0)):
                                col_a, col_b = st.columns(2)

                                with col_a:
                                    st.markdown(f"**🎯 操作建议**")
                                    st.info(f"**买入**: {row['推荐买入']}\n\n**卖出**: {row['推荐卖出']}\n\n**仓位**: {row['建议仓位']}")

                                with col_b:
                                    st.markdown(f"**⏰ 时间节点**")
                                    st.success(row['时间节点'])

                                st.markdown("**📈 技术面分析**")
                                st.write(row['技术面分析'])

                                st.markdown("**💰 资金面分析**")
                                st.write(row['资金面分析'])

                                col_c, col_d = st.columns(2)
                                with col_c:
                                    st.markdown("**✅ 买入理由**")
                                    st.write(row['买入理由'])

                                with col_d:
                                    st.markdown("**📤 卖出策略**")
                                    st.write(row['卖出策略'])

                                st.markdown("**⚠️ 风险提示**")
                                st.warning(row['风险提示'])
                    
                    # 下载
                    csv = res_df.to_csv(index=False).encode('utf-8-sig')
                    st.download_button("📥 下载预测报告", csv, "AI_Prediction.csv", "text/csv")
        
        with tab2:
            st.markdown("### 🎯 单股深度诊断")

            # 提供多种输入方式
            input_mode = st.radio("选择输入方式", ["手动输入", "从排名列表选择"], horizontal=True, key="input_mode")

            code_input = None
            name_input = None

            if input_mode == "手动输入":
                c1, c2, c3 = st.columns([2, 2, 1])
                with c1:
                    code_input = st.text_input("输入股票代码 (如 000001):", max_chars=6, key="manual_code")
                with c2:
                    name_input_search = st.text_input("或输入股票名称:", key="manual_name")
                    if name_input_search and not code_input:
                        # 尝试从缓存数据中搜索
                        df_search = st.session_state.get('df_即时') or st.session_state.get('prediction_target')
                        if df_search is not None and not df_search.empty:
                            name_col = '股票简称' if '股票简称' in df_search.columns else '股票名称'
                            matched = df_search[df_search[name_col].str.contains(name_input_search, na=False, case=False)]
                            if not matched.empty:
                                code_input = matched.iloc[0]['股票代码']
                                name_input = matched.iloc[0][name_col]
                                st.success(f"✓ 找到: {name_input} ({code_input})")
                            else:
                                st.warning(f"未找到包含 '{name_input_search}' 的股票")
            else:
                # 从排名列表选择
                df_select = st.session_state.get('df_即时') or st.session_state.get('prediction_target')
                if df_select is not None and not df_select.empty:
                    name_col = '股票简称' if '股票简称' in df_select.columns else '股票名称'
                    # 取前30个
                    top_df = df_select.head(30)
                    options = ["请选择..."] + (top_df['股票代码'].astype(str) + " " + top_df[name_col].astype(str)).tolist()
                    selected = st.selectbox("从Top30中选择", options, key="select_from_list")
                    if selected != "请选择...":
                        code_input = selected.split(" ")[0]
                        name_input = " ".join(selected.split(" ")[1:])
                else:
                    st.info("💡 请先在 '智能选股' 页面获取数据")

            if code_input:
                # 绘制K线图
                st.markdown("---")
                fetch_and_plot_kline(code_input, name_input)
                st.markdown("---")
            
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
                        
                        st.markdown("### 📊 AI 深度诊断报告")

                        if "error" in pred:
                            st.error(f"❌ 分析出错: {pred['error']}")
                        elif "text" in pred and "buy" not in pred:
                            st.info("💡 AI 给出的原始建议:")
                            st.markdown(pred["text"])
                        else:
                            # 渲染美观的卡片样式
                            st.markdown("""
                            <style>
                            .score-badge {
                                font-size: 32px;
                                font-weight: bold;
                                color: #1976d2;
                                text-align: center;
                                padding: 10px;
                                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                                -webkit-background-clip: text;
                                -webkit-text-fill-color: transparent;
                            }
                            .price-label { font-size: 14px; color: #666; font-weight: 500;}
                            .buy-price { font-size: 24px; color: #d32f2f; font-weight: bold; }
                            .sell-price { font-size: 24px; color: #388e3c; font-weight: bold; }
                            </style>
                            """, unsafe_allow_html=True)

                            # 顶部核心信息卡片
                            st.markdown("#### 🎯 核心建议")
                            col_top1, col_top2, col_top3 = st.columns(3)

                            with col_top1:
                                score = pred.get("comprehensive_score", "N/A")
                                st.markdown(f"**综合评分**")
                                st.markdown(f"<div class='score-badge'>{score}</div>", unsafe_allow_html=True)

                            with col_top2:
                                st.markdown(f"**建议仓位**")
                                position = pred.get("position", "N/A")
                                if "重仓" in position:
                                    st.success(f"### {position}")
                                elif "半仓" in position:
                                    st.info(f"### {position}")
                                elif "轻仓" in position:
                                    st.warning(f"### {position}")
                                else:
                                    st.error(f"### {position}")

                            with col_top3:
                                st.markdown(f"**操作建议**")
                                buy_price = pred.get("buy", "N/A")
                                if "观望" in buy_price:
                                    st.error("### 🚫 观望")
                                else:
                                    st.success("### ✅ 可买入")

                            st.markdown("---")

                            # 买卖价格区间
                            col_price1, col_price2 = st.columns(2)
                            with col_price1:
                                st.markdown(f"<div><span class='price-label'>🔴 建议买入价</span><br><span class='buy-price'>{buy_price}</span></div>", unsafe_allow_html=True)
                                st.caption(pred.get("buy_reason", ""))

                            with col_price2:
                                sell_price = pred.get("sell", "N/A")
                                st.markdown(f"<div><span class='price-label'>🟢 建议卖出价</span><br><span class='sell-price'>{sell_price}</span></div>", unsafe_allow_html=True)
                                st.caption(pred.get("sell_reason", ""))

                            st.markdown("---")

                            # 详细分析
                            st.markdown("#### 📈 技术面分析")
                            st.write(pred.get("technical_analysis", "暂无"))

                            st.markdown("#### 💰 资金面分析")
                            st.write(pred.get("fund_analysis", "暂无"))

                            st.markdown("#### ⏰ 时间节点与变盘分析")
                            st.info(pred.get("time", "暂无"), icon="🕒")

                            st.markdown("#### ⚠️ 风险提示")
                            st.warning(pred.get("risk", "暂无"))
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
