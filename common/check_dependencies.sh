#!/bin/bash
# =============================================================================
# HR Analysis System - Dependency Checker
# =============================================================================
# Быстрая проверка всех необходимых зависимостей перед запуском
# =============================================================================

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[✅]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[⚠️]${NC} $1"; }
log_error() { echo -e "${RED}[❌]${NC} $1"; }

# Счетчики
CHECKS_PASSED=0
CHECKS_FAILED=0
TOTAL_CHECKS=0

check_item() {
    local description="$1"
    local command="$2"
    local optional="${3:-false}"
    
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
    
    if eval "$command" &>/dev/null; then
        log_success "$description"
        CHECKS_PASSED=$((CHECKS_PASSED + 1))
        return 0
    else
        if [ "$optional" = "true" ]; then
            log_warning "$description (optional)"
        else
            log_error "$description"
            CHECKS_FAILED=$((CHECKS_FAILED + 1))
        fi
        return 1
    fi
}

echo "=================================="
echo "🔍 HR Analysis Dependency Checker"
echo "=================================="
echo

# Проверка системных зависимостей
log_info "Checking system dependencies..."
check_item "Python 3.13" "python3.13 --version"
check_item "Git" "git --version"
check_item "PostgreSQL" "psql --version"
check_item "Redis" "redis-cli --version"
check_item "Build tools (gcc)" "gcc --version" true

echo

# Проверка сервисов
log_info "Checking services..."
if [[ "$OSTYPE" == "darwin"* ]]; then
    check_item "PostgreSQL service" "brew services list | grep postgresql | grep started"
    check_item "Redis service" "brew services list | grep redis | grep started"
else
    check_item "PostgreSQL service" "systemctl is-active --quiet postgresql"
    check_item "Redis service" "systemctl is-active --quiet redis"
fi

echo

# Проверка подключений
log_info "Checking connections..."
check_item "PostgreSQL connection" "psql -d postgres -c 'SELECT 1' -t"
check_item "Redis connection" "redis-cli ping | grep PONG"

echo

# Проверка Python окружения
log_info "Checking Python environment..."
check_item "Virtual environment" "[ -d venv ]"
check_item "Virtual environment activation" "[ -f venv/bin/activate ]"

if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
    
    check_item "Pip in venv" "which pip | grep venv"
    check_item "Python version in venv" "python --version | grep '3.13'"
    
    # Проверка Python пакетов
    log_info "Checking Python packages..."
    check_item "Celery" "python -c 'import celery'"
    check_item "Redis (Python)" "python -c 'import redis'"
    check_item "SQLAlchemy" "python -c 'import sqlalchemy'"
    check_item "psycopg2" "python -c 'import psycopg2'"
    check_item "PyTorch" "python -c 'import torch'"
    check_item "Sentence Transformers" "python -c 'import sentence_transformers'"
    check_item "Pandas" "python -c 'import pandas'"
    check_item "Requests" "python -c 'import requests'"
    check_item "python-dotenv" "python -c 'import dotenv'"
    check_item "Flower" "python -c 'import flower'" true
fi

echo

# Проверка конфигурации
log_info "Checking configuration..."
check_item "Environment file (.env)" "[ -f .env ]"
check_item "Requirements file" "[ -f requirements.txt ]"
check_item "Main script" "[ -f main.py ]"
check_item "Celery start script" "[ -x start_celery.sh ]"

echo

# Проверка структуры проекта
log_info "Checking project structure..."
check_item "Database module" "[ -d database ]"
check_item "Tasks module" "[ -d tasks ]"
check_item "Models module" "[ -d models ]"
check_item "Celery app module" "[ -d celery_app ]"
check_item "Utils module" "[ -d utils ]"

echo

# Проверка Celery задач
if [ -f "venv/bin/activate" ]; then
    log_info "Checking Celery tasks..."
    source venv/bin/activate
    
    python -c "
try:
    import tasks.workflows
    app = tasks.workflows.app
    
    expected_tasks = [
        'tasks.workflows.resume_processing_chain',
        'tasks.workflows.job_processing_chain',
        'tasks.fillout_tasks.pull_fillout_resumes',
        'tasks.fillout_tasks.pull_fillout_jobs'
    ]
    
    registered = list(app.tasks.keys())
    
    for task in expected_tasks:
        if task in registered:
            print(f'✅ {task}')
        else:
            print(f'❌ {task}')
            exit(1)
    
    print(f'✅ Total tasks registered: {len([t for t in registered if t.startswith(\"tasks.\")])}')
    
except Exception as e:
    print(f'❌ Celery tasks check failed: {e}')
    exit(1)
" && {
        CHECKS_PASSED=$((CHECKS_PASSED + 4))
        TOTAL_CHECKS=$((TOTAL_CHECKS + 4))
    } || {
        CHECKS_FAILED=$((CHECKS_FAILED + 4))
        TOTAL_CHECKS=$((TOTAL_CHECKS + 4))
    }
fi

echo
echo "=================================="
echo "📊 SUMMARY"
echo "=================================="
echo "Total checks: $TOTAL_CHECKS"
echo "Passed: $CHECKS_PASSED"
echo "Failed: $CHECKS_FAILED"
echo

if [ $CHECKS_FAILED -eq 0 ]; then
    log_success "All dependencies are satisfied! 🎉"
    echo
    echo "You can now:"
    echo "• Start workers: ./start_celery.sh start"
    echo "• Check status: ./start_celery.sh status"
    echo "• Run workflows: python main.py"
    exit 0
else
    log_error "Some dependencies are missing or not configured properly."
    echo
    echo "To fix issues:"
    echo "• Run installer: ./install.sh"
    echo "• Check .env configuration"
    echo "• Ensure services are running"
    exit 1
fi
