#!/bin/bash
"""
Скрипт запуска CPU-воркеров на основном сервере
Запускает воркеры для обработки задач, которые не требуют GPU
"""

# Выходим при любой ошибке
set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Функция для логирования
log() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] $1${NC}"
}

error() {
    echo -e "${RED}[ERROR] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[WARNING] $1${NC}"
}

info() {
    echo -e "${BLUE}[INFO] $1${NC}"
}

# Проверка переменных окружения
check_env() {
    log "Проверка переменных окружения..."
    
    if [ -z "$REDIS_URL" ]; then
        error "REDIS_URL не установлен"
        exit 1
    fi
    
    if [ -z "$DATABASE_URL" ]; then
        error "DATABASE_URL не установлен"
        exit 1
    fi
    
    # Проверяем, что GPU_INSTANCE_NAME НЕ установлен (это CPU сервер)
    if [ -n "$GPU_INSTANCE_NAME" ]; then
        warn "GPU_INSTANCE_NAME установлен, но это CPU сервер. GPU задачи не будут обрабатываться."
    fi
    
    log "Переменные окружения проверены"
}

# Функция остановки всех воркеров
cleanup() {
    log "Остановка всех воркеров..."
    pkill -f "celery.*worker" || true
    sleep 2
    log "Все воркеры остановлены"
}

# Обработчик сигналов
trap cleanup SIGINT SIGTERM

# Получить параметры воркера из Python-конфига
get_worker_param() {
    local queue_name="$1"
    local param="$2"
    python3 -c "from deployment.celery_app.celery_env_config import get_worker_configs; print(get_worker_configs().get('$queue_name', {}).get('$param', ''))"
}

# Основная функция
main() {
    log "Запуск CPU воркеров на основном сервере..."
    check_env
    export PYTHONPATH="${PYTHONPATH}:."
    log "PYTHONPATH: $PYTHONPATH"
    log "REDIS_URL: $REDIS_URL"
    cleanup

    # Запуск воркера default
    log "Запуск воркера default (основные задачи)..."
    celery -A celery_app.celery_app worker \
        --loglevel=info \
        --queues=default \
        --concurrency=$(get_worker_param default concurrency) \
        --prefetch-multiplier=$(get_worker_param default prefetch_multiplier) \
        --max-tasks-per-child=$(get_worker_param default max_tasks_per_child) \
        --hostname=default@%h &

    log "Запуск воркера fillout (API задачи)..."
    celery -A celery_app.celery_app worker \
        --loglevel=info \
        --queues=fillout \
        --concurrency=$(get_worker_param fillout concurrency) \
        --prefetch-multiplier=$(get_worker_param fillout prefetch_multiplier) \
        --max-tasks-per-child=$(get_worker_param fillout max_tasks_per_child) \
        --hostname=fillout@%h &

    log "Запуск воркера search_basic (поиск)..."
    celery -A celery_app.celery_app worker \
        --loglevel=info \
        --queues=search_basic \
        --concurrency=$(get_worker_param search_basic concurrency) \
        --prefetch-multiplier=$(get_worker_param search_basic prefetch_multiplier) \
        --max-tasks-per-child=$(get_worker_param search_basic max_tasks_per_child) \
        --hostname=search_basic@%h &

    if [ -z "$GPU_INSTANCE_NAME" ]; then
        log "GPU сервер не настроен - обрабатываем embedding и scoring задачи на CPU..."
        info "Все GPU задачи будут выполняться через default воркер"
    else
        warn "GPU_INSTANCE_NAME установлен, но GPU воркеры запускаются только на GPU сервере"
    fi
    log "Все CPU воркеры запущены"
    log "Для остановки нажмите Ctrl+C"
    wait
}

# Запуск основной функции
main "$@"
