@echo off
echo ========================================
echo    BI Platform - Development Setup
echo ========================================
echo.

REM Check if PostgreSQL container exists
docker ps -a | findstr bi-postgres >nul 2>&1
if %errorlevel% neq 0 (
    echo [1/4] Starting PostgreSQL container...
    docker run -d --name bi-postgres -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=bi_warehouse -p 5432:5432 postgres:15-alpine
) else (
    echo [1/4] Starting existing PostgreSQL container...
    docker start bi-postgres
)

echo [2/4] Waiting for PostgreSQL to be ready...
timeout /t 5 /nobreak >nul

echo [3/4] Starting Backend...
cd backend
if not exist venv (
    echo Creating virtual environment...
    python -m venv venv
)
call venv\Scripts\activate
pip install -r requirements.txt -q
start "BI Backend" cmd /k "venv\Scripts\activate && uvicorn main:app --reload --port 8000"
cd ..

echo [4/4] Starting Frontend...
cd frontend
if not exist node_modules (
    echo Installing npm packages...
    call npm install
)
start "BI Frontend" cmd /k "npm run dev"
cd ..

echo.
echo ========================================
echo    All services started!
echo ========================================
echo.
echo    Frontend:  http://localhost:5173
echo    Backend:   http://localhost:8000
echo    API Docs:  http://localhost:8000/docs
echo.
echo Press any key to exit this window...
pause >nul
