@echo off
REM Helper script for running Docker containers for the enrichment pipeline
REM Windows batch version

setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

REM Default colors (Windows doesn't support ANSI colors in cmd.exe by default)
REM Using simple formatting instead

if "%1"=="" goto show_help
if "%1"=="help" goto show_help
if "%1"=="check" goto check_docker
if "%1"=="setup" goto check_env
if "%1"=="build" goto build_image
if "%1"=="test" goto test_setup
if "%1"=="database" goto check_database
if "%1"=="enrich" goto run_enrichment
if "%1"=="batch" goto run_batch
if "%1"=="logs" goto view_logs
if "%1"=="clean" goto cleanup

echo Unknown command: %1
goto show_help

:check_docker
    echo Checking Docker installation...
    docker --version >nul 2>&1
    if errorlevel 1 (
        echo ERROR: Docker is not installed. Please install Docker Desktop.
        exit /b 1
    )
    docker-compose --version >nul 2>&1
    if errorlevel 1 (
        echo ERROR: Docker Compose is not installed.
        exit /b 1
    )
    echo OK - Docker and Docker Compose are installed
    exit /b 0

:check_env
    call :check_docker
    if not exist ".env" (
        echo ERROR: .env file not found
        echo Creating .env from .env.example...
        if exist ".env.example" (
            copy ".env.example" ".env"
            echo Please edit .env and add your NEON_DATABASE_URL
        )
        exit /b 1
    )
    echo OK - .env file is configured
    exit /b 0

:build_image
    echo Building Docker Image...
    call :check_docker
    call :check_env
    docker-compose build --progress=plain
    echo Docker image built successfully
    exit /b 0

:test_setup
    echo Testing Setup (1 company, dry-run)...
    call :check_docker
    call :check_env
    docker-compose run --rm enrichment-worker --limit 1 --dry-run
    exit /b 0

:check_database
    echo Checking Database Configuration...
    call :check_docker
    call :check_env
    docker-compose run --rm enrichment-worker python check_db_columns.py
    exit /b 0

:run_enrichment
    set "batch_size=%2"
    set "delay=%3"
    if "!batch_size!"=="" set "batch_size=50"
    if "!delay!"=="" set "delay=10"

    echo Running Enrichment Pipeline...
    echo Batch size: !batch_size!
    echo Delay: !delay! seconds

    call :check_docker
    call :check_env
    docker-compose run --rm enrichment-worker ^
        --batch-size !batch_size! ^
        --delay !delay! ^
        --api-concurrent 5

    echo Enrichment completed
    exit /b 0

:run_batch
    set "batch_size=%2"
    set "max_batches=%3"
    set "delay=%4"
    if "!batch_size!"=="" set "batch_size=50"
    if "!max_batches!"=="" set "max_batches=5"
    if "!delay!"=="" set "delay=10"

    echo Running Batch Enrichment...
    echo Batch size: !batch_size!
    echo Max batches: !max_batches!
    echo Delay: !delay! seconds

    call :check_docker
    call :check_env
    docker-compose run --rm enrichment-worker ^
        --batch-size !batch_size! ^
        --max-batches !max_batches! ^
        --delay !delay!

    echo Batch enrichment completed
    exit /b 0

:view_logs
    echo Container Logs...
    call :check_docker
    docker-compose logs -f enrichment-worker
    exit /b 0

:cleanup
    echo Cleanup - Stopping containers...
    call :check_docker
    docker-compose down
    echo Containers stopped
    exit /b 0

:show_help
    echo.
    echo BTP Company Enrichment Pipeline - Docker Helper
    echo.
    echo Usage: docker-run.bat [COMMAND] [OPTIONS]
    echo.
    echo Commands:
    echo   check       Check Docker installation
    echo   setup       Check environment configuration
    echo   build       Build Docker image
    echo   test        Test setup (1 company, dry-run)
    echo   database    Check database configuration
    echo   enrich      Run enrichment (default: 50 batch size, 10s delay)
    echo   batch       Run batch enrichment (default: 5 batches, 50 batch size)
    echo   logs        View container logs
    echo   clean       Stop and remove containers
    echo   help        Show this help message
    echo.
    echo Examples:
    echo.
    echo   REM Build the Docker image
    echo   docker-run.bat build
    echo.
    echo   REM Test the setup
    echo   docker-run.bat test
    echo.
    echo   REM Check database configuration
    echo   docker-run.bat database
    echo.
    echo   REM Run enrichment with custom settings
    echo   docker-run.bat enrich 100 15
    echo.
    echo   REM Run batch enrichment
    echo   docker-run.bat batch 50 10 5
    echo.
    echo   REM View logs
    echo   docker-run.bat logs
    echo.
    echo   REM Cleanup
    echo   docker-run.bat clean
    echo.
    echo Full documentation: DOCKER_SETUP.md
    echo.
    exit /b 0
