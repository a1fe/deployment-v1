#!/bin/bash

# =============================================================================
# CPU Server (Основной сервер) - Startup Script
# =============================================================================
# Скрипт запуска для основного сервера (e2-standard-2)
# Запускает API, Redis, PostgreSQL, Celery Beat, CPU-воркеры
# =============================================================================

set -e

# Загрузка общих функций
source ../common/logging_functions.sh

# Конфигурация
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_PATH="$PROJECT_ROOT/venv"
LOGS_DIR="$PROJECT_ROOT/logs"
PIDS_DIR="$PROJECT_ROOT/pids"

# Создание необходимых директорий
mkdir -p "$LOGS_DIR" "$PIDS_DIR"

# Функция запуска CPU-воркеров
start_cpu_workers() {
    log_info "Запуск CPU-воркеров..."
    
    source "$VENV_PATH/bin/activate"
    cd "$PROJECT_ROOT"
    
    # Воркер для default очереди
    celery -A celery_app.celery_app worker \
        --loglevel=info \
        --concurrency=2 \
        -Q default \
        --logfile="$LOGS_DIR/default_worker.log" \
        --pidfile="$PIDS_DIR/default_worker.pid" \
        --detach
    
    # Воркер для fillout очереди
    celery -A celery_app.celery_app worker \
        --loglevel=info \
        --concurrency=2 \
        -Q fillout \
        --logfile="$LOGS_DIR/fillout_worker.log" \
        --pidfile="$PIDS_DIR/fillout_worker.pid" \
        --detach
    
    # Воркер для search_basic очереди
    celery -A celery_app.celery_app worker \
        --loglevel=info \
        --concurrency=2 \
        -Q search_basic \
        --logfile="$LOGS_DIR/search_basic_worker.log" \
        --pidfile="$PIDS_DIR/search_basic_worker.pid" \
        --detach
    
    # Если GPU отключен, запускаем GPU-задачи на CPU
    if [ -z "$GPU_INSTANCE_NAME" ]; then
        log_info "GPU отключен. Запуск GPU-задач на CPU..."
        
        # Воркер для embeddings
        celery -A celery_app.celery_app worker \
            --loglevel=info \
            --concurrency=1 \
            -Q embeddings_gpu \
            --logfile="$LOGS_DIR/embeddings_cpu.log" \
            --pidfile="$PIDS_DIR/embeddings_cpu.pid" \
            --detach
        
        # Воркер для scoring
        celery -A celery_app.celery_app worker \
            --loglevel=info \
            --concurrency=1 \
            -Q scoring_tasks \
            --logfile="$LOGS_DIR/scoring_cpu.log" \
            --pidfile="$PIDS_DIR/scoring_cpu.pid" \
            --detach
    fi
    
    log_success "CPU-воркеры запущены"
}

# Функция запуска Celery Beat
start_celery_beat() {
    log_info "Запуск Celery Beat..."
    
    source "$VENV_PATH/bin/activate"
    cd "$PROJECT_ROOT"
    
    celery -A celery_app.celery_app beat \
        --loglevel=info \
        --logfile="$LOGS_DIR/celery_beat.log" \
        --pidfile="$PIDS_DIR/celery_beat.pid" \
        --detach
    
    log_success "Celery Beat запущен"
}

# Функция запуска мониторинга GPU
start_gpu_monitor() {
    if [ -n "$GPU_INSTANCE_NAME" ]; then
        log_info "Запуск мониторинга GPU..."
        
        source "$VENV_PATH/bin/activate"
        cd "$PROJECT_ROOT"
        
        # Создаем скрипт мониторинга
        cat > "$SCRIPT_DIR/gpu_monitor_daemon.py" << 'EOF'
#!/usr/bin/env python3
import sys
import time
import logging
import signal
import os

# Добавляем путь к проекту
sys.path.insert(0, '/opt/hr-analysis/deployment')

from common.gpu_task_monitor import gpu_monitor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def signal_handler(signum, frame):
    logging.info("Получен сигнал остановки. Остановка мониторинга...")
    gpu_monitor.stop_monitoring()
    sys.exit(0)

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    try:
        gpu_monitor.start_monitoring()
        # Держим процесс живым
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        gpu_monitor.stop_monitoring()
EOF
        
        chmod +x "$SCRIPT_DIR/gpu_monitor_daemon.py"
        
        # Запускаем мониторинг в фоне
        nohup python3 "$SCRIPT_DIR/gpu_monitor_daemon.py" \
            > "$LOGS_DIR/gpu_monitor.log" 2>&1 &
        echo $! > "$PIDS_DIR/gpu_monitor.pid"
        
        log_success "Мониторинг GPU запущен"
    else
        log_info "GPU не настроен. Мониторинг пропущен."
    fi
}

# Основная функция запуска
main() {
    log_info "=== Запуск CPU Server (Основной сервер) ==="
    
    # Проверка окружения
    if [ ! -f "$VENV_PATH/bin/activate" ]; then
        log_error "Виртуальное окружение не найдено: $VENV_PATH"
        exit 1
    fi
    
    # Загрузка переменных окружения
    if [ -f "$PROJECT_ROOT/.env" ]; then
        source "$PROJECT_ROOT/.env"
        log_info "Переменные окружения загружены"
    else
        log_warning ".env файл не найден"
    fi
    
    # Запуск компонентов
    start_cpu_workers
    start_celery_beat
    start_gpu_monitor
    
    log_success "=== CPU Server успешно запущен ==="
    
    # Показать статус
    echo ""
    echo "📋 Статус воркеров:"
    pgrep -f "celery.*worker" | wc -l | xargs echo "  - Активных воркеров:"
    
    echo ""
    echo "📂 Логи находятся в: $LOGS_DIR"
    echo "🔧 PID файлы в: $PIDS_DIR"
    echo ""
    echo "💡 Для мониторинга используйте:"
    echo "   ./start_flower.sh  # http://localhost:5555"
    echo ""
}

# Запуск
main "$@"
