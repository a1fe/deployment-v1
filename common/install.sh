#!/bin/bash
# =============================================================================
# HR Analysis System - Installation Script
# =============================================================================
# Автоматическая установка всех зависимостей для деплоймента
# Поддерживаемые ОС: Ubuntu/Debian, CentOS/RHEL, macOS
# =============================================================================

set -e  # Остановка при ошибке

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Функции логирования
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Определение ОС
detect_os() {
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        if [ -f /etc/debian_version ]; then
            OS="debian"
            log_info "Detected: Debian/Ubuntu"
        elif [ -f /etc/redhat-release ]; then
            OS="redhat"
            log_info "Detected: RedHat/CentOS/Fedora"
        else
            OS="unknown"
            log_warning "Unknown Linux distribution"
        fi
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        OS="macos"
        log_info "Detected: macOS"
    else
        OS="unknown"
        log_error "Unsupported operating system: $OSTYPE"
        exit 1
    fi
}

# Проверка прав администратора
check_privileges() {
    if [[ $EUID -eq 0 ]]; then
        log_warning "Running as root. This is not recommended for development."
        SUDO=""
    else
        SUDO="sudo"
        log_info "Running as user. Will use sudo for system packages."
    fi
}

# Установка системных зависимостей
install_system_dependencies() {
    log_info "Installing system dependencies..."
    
    case $OS in
        "debian")
            $SUDO apt-get update
            $SUDO apt-get install -y \
                python3.13 \
                python3.13-dev \
                python3.13-venv \
                python3-pip \
                postgresql \
                postgresql-contrib \
                redis-server \
                git \
                curl \
                wget \
                build-essential \
                libssl-dev \
                libffi-dev \
                libpq-dev \
                pkg-config \
                cmake
            ;;
        "redhat")
            $SUDO yum update -y
            $SUDO yum install -y \
                python313 \
                python313-devel \
                python3-pip \
                postgresql \
                postgresql-server \
                postgresql-contrib \
                redis \
                git \
                curl \
                wget \
                gcc \
                gcc-c++ \
                make \
                openssl-devel \
                libffi-devel \
                postgresql-devel \
                cmake
            ;;
        "macos")
            # Проверка Homebrew
            if ! command -v brew &> /dev/null; then
                log_info "Installing Homebrew..."
                /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
            fi
            
            brew update
            brew install \
                python@3.13 \
                postgresql@14 \
                redis \
                git \
                cmake \
                pkg-config
            ;;
    esac
    
    log_success "System dependencies installed"
}

# Настройка PostgreSQL
setup_postgresql() {
    log_info "Setting up PostgreSQL..."
    
    case $OS in
        "debian")
            $SUDO systemctl start postgresql
            $SUDO systemctl enable postgresql
            ;;
        "redhat")
            if [ ! -f /var/lib/pgsql/data/postgresql.conf ]; then
                $SUDO postgresql-setup initdb
            fi
            $SUDO systemctl start postgresql
            $SUDO systemctl enable postgresql
            ;;
        "macos")
            brew services start postgresql@14
            ;;
    esac
    
    log_info "PostgreSQL service started"
    log_info "Database initialization will be handled by deployment scripts"
    log_success "PostgreSQL configured"
}

# Настройка Redis
setup_redis() {
    log_info "Setting up Redis..."
    
    case $OS in
        "debian"|"redhat")
            $SUDO systemctl start redis
            $SUDO systemctl enable redis
            ;;
        "macos")
            brew services start redis
            ;;
    esac
    
    log_success "Redis configured"
}

# Установка Ollama
install_ollama() {
    log_info "Installing Ollama..."
    
    case $OS in
        "debian")
            curl -fsSL https://ollama.ai/install.sh | sh
            ;;
        "redhat")
            curl -fsSL https://ollama.ai/install.sh | sh
            ;;
        "macos")
            if ! command -v ollama &> /dev/null; then
                brew install ollama
            else
                log_info "Ollama already installed"
            fi
            ;;
    esac
    
    # Запуск Ollama в фоновом режиме
    if ! pgrep -x "ollama" > /dev/null; then
        log_info "Starting Ollama server..."
        ollama serve &
        sleep 5  # Даем время серверу запуститься
    fi
    
    log_success "Ollama installed and started"
}

# Создание виртуального окружения
setup_python_environment() {
    log_info "Setting up Python virtual environment..."
    
    # Проверка Python 3.13
    if ! command -v python3.13 &> /dev/null; then
        log_error "Python 3.13 not found. Please install Python 3.13 first."
        exit 1
    fi
    
    # Создание виртуального окружения
    python3.13 -m venv venv
    source venv/bin/activate
    
    # Обновление pip
    pip install --upgrade pip setuptools wheel
    
    log_success "Python environment created"
}

# Установка LLM моделей
install_llm_models() {
    log_info "Installing LLM models..."
    
    # Убеждаемся что Ollama запущен
    if ! pgrep -x "ollama" > /dev/null; then
        log_info "Starting Ollama server..."
        ollama serve &
        sleep 10  # Даем больше времени для запуска
    fi
    
    # Установка модели для эмбеддингов
    log_info "Downloading nomic-embed-text model for embeddings..."
    if ! ollama list | grep -q "nomic-embed-text"; then
        ollama pull nomic-embed-text:latest
        log_success "nomic-embed-text model downloaded"
    else
        log_info "nomic-embed-text model already exists"
    fi
    
    log_success "LLM models installed"
}

# Установка Python зависимостей
install_python_dependencies() {
    log_info "Installing Python dependencies..."
    
    if [ ! -f "venv/bin/activate" ]; then
        log_error "Virtual environment not found. Run setup_python_environment first."
        exit 1
    fi
    
    source venv/bin/activate
    
    # Установка зависимостей из requirements.txt
    if [ -f "requirements.txt" ]; then
        pip install -r requirements.txt
    else
        log_error "requirements.txt not found"
        exit 1
    fi
    
    # Дополнительные зависимости для production
    pip install \
        gunicorn \
        flower \
        supervisor \
        psutil
    
    # Предзагрузка BGE Reranker моделей
    log_info "Pre-caching BGE Reranker models..."
    python -c "
import logging
logging.basicConfig(level=logging.INFO)

try:
    from FlagEmbedding import FlagReranker
    # Предзагрузка основной BGE модели
    model_name = 'BAAI/bge-reranker-v2-m3'
    print(f'Downloading and caching BGE model: {model_name}...')
    reranker = FlagReranker(model_name, use_fp16=True)
    print('✅ BGE Reranker model successfully cached')
    del reranker
except Exception as e:
    print(f'⚠️ BGE model pre-caching failed: {e}')
    print('Model will be downloaded on first use')
"
    
    log_success "Python dependencies installed"
}

# Настройка переменных окружения
setup_environment() {
    log_info "Setting up environment variables..."
    
    if [ ! -f ".env" ]; then
        if [ -f ".env.example" ]; then
            cp .env.example .env
            log_info "Created .env from .env.example"
            log_warning "Please edit .env file with your actual configuration"
        else
            log_error ".env.example not found"
            exit 1
        fi
    else
        log_info ".env file already exists"
    fi
    
    log_success "Environment configuration ready"
}

# Инициализация базы данных
initialize_database() {
    log_info "Initializing database..."
    
    source venv/bin/activate
    
    # Проверка подключения к базе данных
    python -c "
from database.config import test_database_connection
if test_database_connection():
    print('✅ Database connection successful')
else:
    print('❌ Database connection failed')
    exit(1)
" || {
        log_error "Database connection failed. Please check your .env configuration."
        exit 1
    }
    
    # Инициализация схемы и данных
    python main.py <<EOF
1
3
0
EOF
    
    log_success "Database initialized"
}

# Проверка установки
verify_installation() {
    log_info "Verifying installation..."
    
    # Проверка сервисов
    case $OS in
        "debian"|"redhat")
            systemctl is-active --quiet postgresql && log_success "PostgreSQL is running" || log_error "PostgreSQL is not running"
            systemctl is-active --quiet redis && log_success "Redis is running" || log_error "Redis is not running"
            ;;
        "macos")
            brew services list | grep postgresql | grep started > /dev/null && log_success "PostgreSQL is running" || log_error "PostgreSQL is not running"
            brew services list | grep redis | grep started > /dev/null && log_success "Redis is running" || log_error "Redis is not running"
            ;;
    esac
    
    # Проверка Python окружения
    if [ -f "venv/bin/activate" ]; then
        source venv/bin/activate
        python -c "
import sys
print(f'Python version: {sys.version}')

# Проверка основных зависимостей
packages = ['celery', 'redis', 'sqlalchemy', 'psycopg2', 'torch', 'chromadb', 'sentence_transformers', 'FlagEmbedding']
for pkg in packages:
    try:
        __import__(pkg)
        print(f'✅ {pkg} imported successfully')
    except ImportError as e:
        print(f'❌ {pkg} import failed: {e}')
"
        log_success "Python environment verified"
    else
        log_error "Virtual environment not found"
    fi
    
    # Проверка Ollama
    if command -v ollama &> /dev/null; then
        log_success "Ollama is installed"
        
        # Проверка моделей Ollama
        if ollama list | grep -q "nomic-embed-text"; then
            log_success "nomic-embed-text model is available"
        else
            log_warning "nomic-embed-text model not found"
        fi
    else
        log_error "Ollama is not installed"
    fi
    
    # Проверка BGE Reranker моделей
    source venv/bin/activate
    python -c "
try:
    from FlagEmbedding import FlagReranker
    model_name = 'BAAI/bge-reranker-v2-m3'
    print(f'Checking BGE model: {model_name}')
    # Не загружаем модель, просто проверяем что FlagEmbedding работает
    print('✅ BGE Reranker is available')
except Exception as e:
    print(f'❌ BGE Reranker check failed: {e}')
"
    
    # Проверка Celery задач
    source venv/bin/activate
    python -c "
try:
    import tasks.workflows
    app = tasks.workflows.app
    task_count = len([t for t in app.tasks.keys() if t.startswith('tasks.')])
    print(f'✅ Celery tasks loaded: {task_count} tasks')
except Exception as e:
    print(f'❌ Celery tasks loading failed: {e}')
"
}

# Создание systemd сервисов (только для Linux)
create_systemd_services() {
    if [[ $OS == "debian" || $OS == "redhat" ]]; then
        log_info "Creating systemd services..."
        
        # Celery worker service
        $SUDO tee /etc/systemd/system/hr-analysis-worker.service > /dev/null <<EOF
[Unit]
Description=HR Analysis Celery Worker
After=network.target postgresql.service redis.service

[Service]
Type=forking
User=$(whoami)
Group=$(whoami)
WorkingDirectory=$(pwd)
Environment=PATH=$(pwd)/venv/bin
ExecStart=$(pwd)/start_celery.sh start
ExecStop=$(pwd)/start_celery.sh stop
Restart=always

[Install]
WantedBy=multi-user.target
EOF
        
        $SUDO systemctl daemon-reload
        $SUDO systemctl enable hr-analysis-worker
        
        log_success "Systemd services created"
    fi
}

# Показать инструкции по запуску
show_usage_instructions() {
    log_success "Installation completed successfully!"
    echo
    echo "=================================="
    echo "🚀 NEXT STEPS:"
    echo "=================================="
    echo
    echo "1. Edit configuration:"
    echo "   nano .env"
    echo
    echo "2. Activate virtual environment:"
    echo "   source venv/bin/activate"
    echo
    echo "3. Start Celery workers:"
    echo "   ./start_celery.sh start"
    echo
    echo "4. Check status:"
    echo "   ./start_celery.sh status"
    echo
    echo "5. Start monitoring:"
    echo "   ./start_celery.sh flower"
    echo "   # Open http://localhost:5555"
    echo
    echo "6. Test workflows:"
    echo "   python main.py"
    echo "   # Choose option 8"
    echo
    echo "=================================="
    echo "🧠 LLM MODELS INSTALLED:"
    echo "=================================="
    echo "• Ollama: nomic-embed-text:latest (for embeddings)"
    echo "• BGE Reranker: BAAI/bge-reranker-v2-m3 (for scoring)"
    echo "• Check Ollama: ollama list"
    echo "• Ollama server: ollama serve"
    echo
    echo "=================================="
    echo "📋 USEFUL COMMANDS:"
    echo "=================================="
    echo "• View logs: ./start_celery.sh logs"
    echo "• Restart workers: ./start_celery.sh restart"
    echo "• Stop all: ./start_celery.sh stop"
    echo "• Test embeddings: python -c 'from tasks.embedding_tasks import *'"
    echo
    if [[ $OS == "debian" || $OS == "redhat" ]]; then
        echo "• Start as service: sudo systemctl start hr-analysis-worker"
        echo "• Enable on boot: sudo systemctl enable hr-analysis-worker"
        echo
    fi
}

# Главная функция
main() {
    echo "=================================="
    echo "🔧 HR Analysis System Installer"
    echo "=================================="
    echo
    
    detect_os
    check_privileges
    
    log_info "Starting installation process..."
    
    # Установка компонентов
    install_system_dependencies
    setup_postgresql
    setup_redis
    install_ollama
    setup_python_environment
    install_llm_models
    install_python_dependencies
    setup_environment
    initialize_database
    
    # Проверка установки
    verify_installation
    
    # Создание сервисов (опционально)
    create_systemd_services
    
    # Показать инструкции
    show_usage_instructions
}

# Проверка аргументов командной строки
case "${1:-install}" in
    "install")
        main
        ;;
    "verify")
        verify_installation
        ;;
    "help"|"--help"|"-h")
        echo "Usage: $0 [install|verify|help]"
        echo
        echo "Commands:"
        echo "  install  - Install all dependencies (default)"
        echo "  verify   - Verify existing installation"
        echo "  help     - Show this help message"
        ;;
    *)
        log_error "Unknown command: $1"
        log_info "Use '$0 help' for usage information"
        exit 1
        ;;
esac
