@echo off
echo Starting Stock Manager AI...
echo.

:: Check for virtual environment
if exist ".venv\Scripts\activate.bat" (
    echo Activating virtual environment...
    call .venv\Scripts\activate.bat
) else (
    echo Virtual environment not found. Ensure dependencies are installed.
    echo Recommendation: python -m venv .venv
    echo Then run: .venv\Scripts\pip install -r requirements.txt
)

:: Run the app
echo.
echo Launching Streamlit...
streamlit run app.py

pause
