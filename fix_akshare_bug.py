"""
修复 akshare 超大单 API 的 bug
"""
import akshare
import inspect
import shutil
from datetime import datetime

print("=" * 80)
print("修复 akshare 超大单 API bug")
print("=" * 80)

# 获取源文件路径
source_file = inspect.getfile(akshare.stock_fund_flow_individual)
print(f"\nakshare源文件: {source_file}")

# 备份原文件
backup_file = source_file + f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
shutil.copy2(source_file, backup_file)
print(f"[OK] 已备份到: {backup_file}")

# 读取源文件
with open(source_file, 'r', encoding='utf-8') as f:
    content = f.read()

# 修复方案：注释掉硬编码的列名赋值，让pandas自动使用原始列名
# 在第125行之后添加注释
old_code = """    else:
        big_df.columns = [
            "序号",
            "股票代码",
            "股票简称",
            "最新价",
            "阶段涨跌幅",
            "连续换手率",
            "资金流入净额",
        ]"""

new_code = """    else:
        # 临时修复：不强制设置列名，使用API返回的原始列名
        # 原因：API返回10列，但硬编码只有7列名，导致 Length mismatch
        # big_df.columns = [
        #     "序",
        #     "股票代码",
        #     "股票简称",
        #     "最新价",
        #     "阶段涨跌幅",
        #     "连续换手率",
        #     "资金流入净额",
        # ]
        pass  # 保持原始列名"""

# 替换
if old_code in content:
    content = content.replace(old_code, new_code)

    # 写回文件
    with open(source_file, 'w', encoding='utf-8') as f:
        f.write(content)

    print("[OK] 修复成功！")
    print("\n修改内容:")
    print("- 注释掉了硬编码的列名赋值")
    print("- 使用API返回的原始列名")
    print("\n现在可以正常获取超大单数据了")
else:
    print("[ERROR] 未找到需要修复的代码")
    print("akshare可能已经更新，或代码结构已变化")

print("\n" + "=" * 80)
print("提示：如果需要恢复，使用备份文件:")
print(f"  copy \"{backup_file}\" \"{source_file}\"")
print("=" * 80)
