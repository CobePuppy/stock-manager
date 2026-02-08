@echo off
echo =====================================================
echo 清除缓存并准备重新获取数据
echo =====================================================
echo.

cd /d "%~dp0"

echo [1/3] 停止可能正在运行的 Streamlit 应用...
taskkill /F /IM streamlit.exe 2>nul
timeout /t 2 >nul

echo [2/3] 清除数据库缓存...
python -c "import sqlite3, os; conn=sqlite3.connect('stock_data.db') if os.path.exists('stock_data.db') else None; conn.execute('DELETE FROM fund_flow_cache') if conn else None; conn.commit() if conn else None; conn.close() if conn else None; print('缓存已清除')"

echo [3/3] 准备就绪！
echo.
echo =====================================================
echo 现在请运行: streamlit run app.py
echo 然后点击页面上的 "🔄 刷新数据" 按钮
echo =====================================================
echo.
pause
