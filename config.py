# 项目配置文件

# -----------------
# 基础配置
# -----------------

# 缓存数据保存目录
CACHE_DIR = 'stock_data_cache'

# 数据库路径
DB_PATH = 'stock_data.db'

# 回测历史数据库路径
HISTORY_DB_PATH = 'stock_history.db'

# 分析结果保存目录
RESULT_DIR = 'analysis_results'

# 历史文件保留天数
KEEP_DAYS = 7

# 每次分析取前多少名
TOP_N = 20

# -----------------
# 大模型配置 (用于预测功能)
# -----------------

# 是否启用预测功能
ENABLE_PREDICTION = True

# 对前多少名进行详细预测 (建议不要太多，避免消耗过多Token或请求超时)
PREDICT_TOP_N = 3

# 大模型 API 地址 (例如 DeepSeek, OpenAI, Moonshot 等兼容 OpenAI 格式的接口)
# 硅基流动 (SiliconFlow) API 地址
LLM_API_URL = "https://api.siliconflow.cn/v1/chat/completions"

# API Key (请在此处填入您的 Key)
LLM_API_KEY = "sk-fxaqhrxhuluwxteaudthpjmcljwidzyrowizeqgbxpyyklvz"

# 模型名称
LLM_MODEL = "Qwen/Qwen3-30B-A3B"

# 提示词模板
PROMPT_TEMPLATE = """
你是一个专业的股市交易员。请根据以下提供的股票数据进行分析，并做出预测。

【基本信息】
股票名称: {stock_name} ({stock_code})
{basic_info}

【资金流向】
{fund_data}

【历史行情 (近5日)】
{history_data}

【最新动态/新闻】
{news_data}

请严格仅输出以下3项内容（JSON格式），不要包含其他废话：
1. recommend_buy_price (推荐买入价格区间)
2. recommend_sell_price (推荐卖出价格区间)
3. estimated_time (预计高位/低位出现的时间或变盘节点)

返回格式示例:
{{
    "buy": "10.50 - 10.80",
    "sell": "11.50 - 12.00",
    "time": "预计3日内出现高点，下午2点后需警惕跳水"
}}
"""
