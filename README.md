# Stock Manager AI 📈

一个基于 Python 的 A股资金流向分析与智能预测系统。自动追踪资金强流向个股，记录历史数据以供回测，并提供 AI 辅助诊断建议。

## 主要功能

*   **资金流向监控**：实时/3日/5日/10日/20日 资金净流入与增仓占比排名。
*   **智能筛选**：自动过滤流通市值 >= 1000亿 的巨无霸，专注中小盘潜力股。
*   **历史回测数据**：自动记录所有曾经进入 Top 20 榜单的股票每日交易数据，构建专属 `stock_history.db` 数据库用于回测。
*   **AI 诊断**：集成 AI 预测模型，提供个股买卖点参考（需配置 key）。
*   **可视化看板**：基于 Streamlit 的 Web 界面，直观展示排行与个股深度数据。

## 部署指南

### 1. 环境准备

*   Python 3.8+
*   Git

### 2. 获取代码

```bash
git clone https://github.com/CobePuppy/stock-manager.git
cd stock-manager
```

### 3. 安装依赖

建议使用虚拟环境（Virtual Environment）以避免依赖冲突。

**Windows:**

```powershell
# 创建虚拟环境
python -m venv .venv

# 激活虚拟环境
.venv\Scripts\activate

# 安装依赖
pip install -r requirements.txt
```

**Linux/Mac:**

```bash
# 创建虚拟环境
python3 -m venv .venv

# 激活虚拟环境
source .venv/bin/activate

# 安装依赖
pip install -r requirements.txt
```

### 4. 运行系统

系统分为 **数据采集/分析** 和 **Web 前端** 两部分。

#### 方式一：一键运行 (Windows 推荐)

直接双击运行项目根目录下的 `run.bat` 脚本。它会自动执行一次数据分析，然后启动 Web 界面。

#### 方式二：手动运行

**步骤 1：执行数据抓取与分析**

此命令会从 API 获取最新资金流数据，更新 Top 20 榜单，并记录历史回测数据。

```bash
python main.py
```

*初次运行会自动初始化 `stock_data.db` (缓存库) 和 `stock_history.db` (回测库)。*

**步骤 2：启动 Web 可视化界面**

```bash
streamlit run app.py
```

启动后浏览器访问: `http://localhost:8501`

## 数据存储说明

本项目在本地生成两个 SQLite 数据库文件（已在 `.gitignore` 中排除，请勿提交）：

1.  `stock_data.db`: 用于缓存当天的 API 数据和 Watchlist 自选股，加速 Web 访问。
2.  `stock_history.db`: **核心资产**。记录了所有历史入榜股票的每日详细交易数据，随着时间推移，将成为珍贵的回测数据集。

## 常见问题

*   **API 读取超时**：请检查网络连接，部分数据接口可能需要特定网络环境。
*   **IndentationError / SyntaxError**：请确保不要混用 Tab 和空格，并使用 Python 3.8 以上版本。
