#!/bin/bash

# =============================================================================
# GPU Server Startup Script для Google Cloud Platform
# =============================================================================
# Автоматически запускается при старте GPU VM инстанса
# Настраивает окружение и запускает GPU-воркеры
# =============================================================================

set -e

# Логирование
exec > >(tee -a /var/log/gpu-startup.log)
exec 2>&1

echo "$(date): Начало инициализации GPU сервера..."

# Переменные окружения
PROJECT_ROOT="/opt/hr-analysis/deployment"
VENV_PATH="$PROJECT_ROOT/venv"
LOGS_DIR="$PROJECT_ROOT/logs"
PIDS_DIR="$PROJECT_ROOT/pids"

# Функции логирования
log_info() {
    echo "$(date) [INFO] $1"
}

log_error() {
    echo "$(date) [ERROR] $1" >&2
}

log_success() {
    echo "$(date) [SUCCESS] $1"
}

# Функция проверки зависимостей
check_dependencies() {
    log_info "Проверка зависимостей..."
    
    # Проверка NVIDIA драйверов
    if ! command -v nvidia-smi &> /dev/null; then
        log_error "NVIDIA драйверы не установлены"
        return 1
    fi
    
    # Проверка Python
    if [ ! -f "$VENV_PATH/bin/activate" ]; then
        log_error "Виртуальное окружение не найдено: $VENV_PATH"
        return 1
    fi
    
    # Проверка подключения к Redis (основной сервер)
    if ! python3 -c "
import redis
import os
r = redis.Redis(
    host=os.environ.get('REDIS_HOST', 'localhost'),
    port=int(os.environ.get('REDIS_PORT', '6379')),
    password=os.environ.get('REDIS_PASSWORD'),
    socket_timeout=5
)
r.ping()
print('Redis connection OK')
" 2>/dev/null; then
        log_error "Не удается подключиться к Redis на основном сервере"
        return 1
    fi
    
    log_success "Все зависимости проверены"
    return 0
}

# Функция установки NVIDIA драйверов (если нужно)
install_nvidia_drivers() {
    if ! command -v nvidia-smi &> /dev/null; then
        log_info "Установка NVIDIA драйверов..."
        
        # Обновление системы
        apt-get update
        
        # Установка драйверов
        apt-get install -y ubuntu-drivers-common
        ubuntu-drivers autoinstall
        
        log_info "NVIDIA драйверы установлены. Требуется перезагрузка."
        return 1
    fi
    
    return 0
}

# Функция настройки окружения
setup_environment() {
    log_info "Настройка окружения..."
    
    # Создание директорий
    mkdir -p "$LOGS_DIR" "$PIDS_DIR"
    
    # Активация виртуального окружения
    source "$VENV_PATH/bin/activate"
    cd "$PROJECT_ROOT"
    
    # Загрузка переменных окружения
    if [ -f "$PROJECT_ROOT/.env" ]; then
        export $(cat "$PROJECT_ROOT/.env" | grep -v '^#' | xargs)
        log_info "Переменные окружения загружены"
    else
        log_error ".env файл не найден"
        return 1
    fi
    
    # Проверка GPU переменных
    if [ -z "$REDIS_HOST" ]; then
        log_error "REDIS_HOST не установлен"
        return 1
    fi
    
    log_success "Окружение настроено"
    return 0
}

# Функция запуска Ollama
start_ollama() {
    log_info "Запуск Ollama..."
    
    # Проверка установки Ollama
    if ! command -v ollama &> /dev/null; then
        log_info "Установка Ollama..."
        curl -fsSL https://ollama.ai/install.sh | sh
    fi
    
    # Запуск Ollama сервера
    if ! pgrep -x "ollama" > /dev/null; then
        nohup ollama serve > "$LOGS_DIR/ollama.log" 2>&1 &
        sleep 10  # Ждем запуска
        
        # Загрузка модели эмбеддингов
        if ! ollama list | grep -q "nomic-embed-text"; then
            log_info "Загрузка модели nomic-embed-text..."
            ollama pull nomic-embed-text:latest
        fi
    fi
    
    log_success "Ollama запущен"
}

# Функция запуска GPU-воркеров
start_gpu_workers() {
    log_info "Запуск GPU-воркеров..."
    
    source "$VENV_PATH/bin/activate"
    cd "$PROJECT_ROOT"
    
    # Воркер для эмбеддингов
    celery -A celery_app.celery_app worker \
        --loglevel=info \
        --concurrency=1 \
        -Q embeddings_gpu \
        --logfile="$LOGS_DIR/embeddings_gpu.log" \
        --pidfile="$PIDS_DIR/embeddings_gpu.pid" \
        --detach
    
    # Воркер для скоринга
    celery -A celery_app.celery_app worker \
        --loglevel=info \
        --concurrency=1 \
        -Q scoring_tasks \
        --logfile="$LOGS_DIR/scoring_gpu.log" \
        --pidfile="$PIDS_DIR/scoring_gpu.pid" \
        --detach
    
    log_success "GPU-воркеры запущены"
}

# Функция мониторинга активности
start_activity_monitor() {
    log_info "Запуск мониторинга активности..."
    
    # Создаем скрипт для обновления метки активности
    cat > "$PROJECT_ROOT/update_activity.py" << 'EOF'
#!/usr/bin/env python3
import os
import time
import redis
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def update_activity():
    try:
        r = redis.Redis(
            host=os.environ.get('REDIS_HOST', 'localhost'),
            port=int(os.environ.get('REDIS_PORT', '6379')),
            password=os.environ.get('REDIS_PASSWORD'),
            decode_responses=True
        )
        
        current_time = time.time()
        r.set('gpu_last_activity', current_time, ex=3600)
        logger.info(f"Обновлена метка активности: {current_time}")
        
    except Exception as e:
        logger.error(f"Ошибка обновления активности: {e}")

if __name__ == "__main__":
    update_activity()
EOF
    
    chmod +x "$PROJECT_ROOT/update_activity.py"
    
    # Добавляем в cron для обновления каждую минуту
    echo "*/1 * * * * cd $PROJECT_ROOT && python3 update_activity.py" | crontab -
    
    log_success "Мониторинг активности настроен"
}

# Основная функция
main() {
    log_info "=== Инициализация GPU Server ==="
    
    # Установка драйверов если нужно
    if ! install_nvidia_drivers; then
        log_info "Требуется перезагрузка для активации драйверов"
        reboot
        exit 0
    fi
    
    # Настройка окружения
    if ! setup_environment; then
        log_error "Ошибка настройки окружения"
        exit 1
    fi
    
    # Проверка зависимостей
    if ! check_dependencies; then
        log_error "Проверка зависимостей не пройдена"
        exit 1
    fi
    
    # Запуск сервисов
    start_ollama
    start_gpu_workers
    start_activity_monitor
    
    log_success "=== GPU Server успешно запущен ==="
    
    # Статус
    echo ""
    echo "🖥️  GPU Info:"
    nvidia-smi --query-gpu=name,memory.total,memory.used --format=csv,noheader,nounits
    echo ""
    echo "📋 Воркеры:"
    pgrep -f "celery.*worker" | wc -l | xargs echo "  - Активных воркеров:"
    echo ""
    echo "📂 Логи: $LOGS_DIR"
    echo ""
}

# Запуск при загрузке системы
if [ "$1" = "startup" ] || [ -z "$1" ]; then
    main
fi
