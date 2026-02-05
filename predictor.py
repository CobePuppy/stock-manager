import akshare as ak
import pandas as pd
import requests
import json
import time
import numpy as np
from datetime import datetime
import config

class StockPredictor:
    def __init__(self):
        self.api_url = config.LLM_API_URL
        self.api_key = config.LLM_API_KEY
        self.model = config.LLM_MODEL
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }

    def fetch_history_data(self, stock_code):
        """
        获取股票历史K线数据并计算技术指标
        """
        try:
            # 获取最近45天的日线数据 (用于计算指标)
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - pd.Timedelta(days=60)).strftime("%Y%m%d")
            
            # akshare 获取个股历史数据
            df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
            
            if df.empty:
                return "无历史数据"
            
            # 必须保证数据按日期升序
            if '日期' in df.columns:
                df = df.sort_values(by='日期')
                
            # --- 计算技术指标 ---
            # 1. 均线 MA
            df['MA5'] = df['收盘'].rolling(window=5).mean()
            df['MA10'] = df['收盘'].rolling(window=10).mean()
            df['MA20'] = df['收盘'].rolling(window=20).mean()
            
            # 2. RSI (6日, 12日)
            def calculate_rsi(data, window=6):
                delta = data.diff()
                gain = (delta.where(delta > 0, 0)).fillna(0)
                loss = (-delta.where(delta < 0, 0)).fillna(0)
                avg_gain = gain.rolling(window=window).mean()
                avg_loss = loss.rolling(window=window).mean()
                rs = avg_gain / avg_loss.replace(0, 1e-9)
                return 100 - (100 / (1 + rs))
            
            df['RSI6'] = calculate_rsi(df['收盘'], 6)
            
            # 3. MACD (12, 26, 9)
            exp1 = df['收盘'].ewm(span=12, adjust=False).mean()
            exp2 = df['收盘'].ewm(span=26, adjust=False).mean()
            df['MACD'] = exp1 - exp2
            df['Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
            df['Hist'] = df['MACD'] - df['Signal']

            # 取最后5行用于显示趋势
            recent_df = df.tail(5).copy()
            
            # 格式化指标数据
            result_str = "【近期 5 日关键数据与指标】\n"
            result_str += "日期      | 收盘 | 涨跌幅 | MA5 | MA20 | RSI6 | MACD\n"
            result_str += "-" * 60 + "\n"
            
            for _, row in recent_df.iterrows():
                try:
                    date_short = row['日期'][5:]
                    close = f"{row['收盘']:.2f}"
                    pct = f"{row['涨跌幅']:.2f}%"
                    ma5 = f"{row['MA5']:.2f}"
                    ma20 = f"{row['MA20']:.2f}"
                    rsi = f"{row['RSI6']:.1f}" if not pd.isna(row['RSI6']) else "-"
                    macd = f"{row['Hist']:.3f}" # 显示红绿柱
                    
                    result_str += f"{date_short} | {close} | {pct} | {ma5} | {ma20} | {rsi} | {macd}\n"
                except:
                    continue
                    
            return result_str
            
        except Exception as e:
            return f"获取历史行情失败: {e}"

    def fetch_basic_info(self, stock_code):
        """获取基本面数据 (行业、PE、总市值等)"""
        try:
            df = ak.stock_individual_info_em(symbol=stock_code)
            # df columns: item, value
            info_dict = dict(zip(df['item'], df['value']))
            
            # 挑选关键指标
            keys = ['行业', '总市值', '流通市值', '市盈率(动)', '市净率']
            summary = []
            for k in keys:
                if k in info_dict:
                     summary.append(f"{k}: {info_dict[k]}")
            return " | ".join(summary)
        except Exception as e:
            return f"基本面数据获取失败: {e}"

    def fetch_news(self, stock_code):
        """获取个股最新新闻 (Top 3)"""
        try:
            # 限制获取条数以节省流量
            df = ak.stock_news_em(symbol=stock_code)
            if df.empty:
                return "近期无重大新闻"
            
            # 只要前3条标题和时间
            news_list = []
            for _, row in df.head(3).iterrows():
                title = row['发布时间'][:10] + " " + row['新闻标题']
                news_list.append(title)
            return "\n".join(news_list)
        except Exception as e:
            return "新闻获取失败 (可能是接口限制)"

    def predict(self, stock_code, stock_name, fund_data_row):
        """
        调用大模型进行预测
        """
        if not self.api_key or "sk-xxxx" in self.api_key:
            return {"error": "未配置API Key"}

        # 1. 获取各类数据 (并行获取或顺序获取)
        history_data = self.fetch_history_data(stock_code)
        basic_info = self.fetch_basic_info(stock_code)
        news_data = self.fetch_news(stock_code)
        
        # 2. 格式化资金流数据
        fund_str = fund_data_row.to_string()
        
        # 3. 构造Prompt
        prompt = config.PROMPT_TEMPLATE.format(
            stock_name=stock_name,
            stock_code=stock_code,
            basic_info=basic_info,
            fund_data=fund_str,
            history_data=history_data,
            news_data=news_data
        )

        # 4. 调用API
        payload = {
            "model": self.model,
            "messages": [
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.3
        }

        try:
            response = requests.post(self.api_url, headers=self.headers, json=payload, timeout=30)
            response.raise_for_status()
            result = response.json()
            
            content = result['choices'][0]['message']['content']
            
            # 尝试解析JSON
            try:
                # 有些模型可能返回markdown代码块，去除 ```json ... ```
                cleaned_content = content.replace("```json", "").replace("```", "").strip()
                prediction = json.loads(cleaned_content)
                return prediction
            except json.JSONDecodeError:
                # 解析失败则返回原始文本
                return {"text": content}
                
        except Exception as e:
            return {"error": str(e)}

def run_predictions(df_ranked, top_n=3):
    """
    对排名靠前的股票批量进行预测
    """
    if not config.ENABLE_PREDICTION:
        return None

    predictor = StockPredictor()
    results = []

    print(f"\n>>> 开始智能预测 (Top {top_n}) ...")
    if not predictor.api_key or "sk-xxxx" in predictor.api_key:
        print("提示: 请在 config.py 中配置 LLM_API_KEY 以启用预测功能。")
        return None

    # 遍历前N名
    for index, row in df_ranked.head(top_n).iterrows():
        code = row['股票代码']
        name = row['股票简称']
        print(f"正在分析: {name} ({code})...")

        pred = predictor.predict(code, name, row)

        # 结果处理
        if "error" in pred:
            print(f"  预测失败: {pred['error']}")
        else:
            # 扁平化结果以便保存（新增更多字段）
            res_row = {
                "股票代码": code,
                "股票简称": name,
                "综合评分": pred.get("comprehensive_score", "-"),
                "推荐买入": pred.get("buy", pred.get("text", "")),
                "推荐卖出": pred.get("sell", ""),
                "建议仓位": pred.get("position", "-"),
                "时间节点": pred.get("time", ""),
                "技术面分析": pred.get("technical_analysis", ""),
                "资金面分析": pred.get("fund_analysis", ""),
                "买入理由": pred.get("buy_reason", ""),
                "卖出策略": pred.get("sell_reason", ""),
                "风险提示": pred.get("risk", "")
            }
            results.append(res_row)
            print(f"  综合评分: {res_row['综合评分']} | 买入: {res_row['推荐买入']} | 仓位: {res_row['建议仓位']}")

        time.sleep(1) # 避免触发API速率限制

    if results:
        return pd.DataFrame(results)
    else:
        return None
