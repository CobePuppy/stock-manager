@echo off
echo =====================================================
echo 应用超大单数据修复
echo =====================================================
echo.

cd /d "%~dp0"

echo [1/3] 停止可能正在运行的 Streamlit 应用...
taskkill /F /IM streamlit.exe 2>nul
timeout /t 2 >nul

echo [2/3] 清除缓存数据...
python -c "import sqlite3, os; conn=sqlite3.connect('stock_data.db') if os.path.exists('stock_data.db') else None; conn.execute('DELETE FROM fund_flow_cache WHERE period_type=\"即时\"') if conn else None; conn.commit() if conn else None; conn.close() if conn else None; print('缓存已清除')"

echo [3/3] 说明：
echo.
echo =====================================================
echo akshare库已修复，现在可以正确获取超大单数据
echo.
echo 修改内容：
echo - 修复了akshare库列数不匹配的bug
echo - 更新了rank_flow.py使用真正的超大单净额
echo - 增仓占比 = 超大单净额 / 成交额 x 100
echo.
echo 现在请运行: streamlit run app.py
echo 然后点击页面上的 "🔄 刷新数据" 按钮
echo =====================================================
echo.
pause
