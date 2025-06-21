#!/bin/bash
"""
Скрипт запуска GPU-воркеров на GPU сервере
Запускает воркеры для обработки задач, требующих GPU (эмбеддинги, реранкинг)
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
    
    # Проверяем, что GPU_INSTANCE_NAME установлен (это обязательно для GPU сервера)
    if [ -z "$GPU_INSTANCE_NAME" ]; then
        error "GPU_INSTANCE_NAME не установлен. Этот скрипт предназначен для GPU сервера."
        exit 1
    fi
    
    log "GPU Instance: $GPU_INSTANCE_NAME"
    log "Переменные окружения проверены"
}

# Проверка доступности GPU
check_gpu() {
    log "Проверка доступности GPU..."
    
    if command -v nvidia-smi &> /dev/null; then
        nvidia-smi --query-gpu=name,memory.total,memory.free --format=csv,noheader,nounits
        log "GPU доступен"
    else
        warn "nvidia-smi не найден. Проверьте установку CUDA драйверов"
    fi
}

# Проверка Python пакетов для GPU
check_gpu_packages() {
    log "Проверка GPU пакетов Python..."
    
    python3 -c "
try:
    import torch
    print(f'PyTorch: {torch.__version__}')
    print(f'CUDA available: {torch.cuda.is_available()}')
    if torch.cuda.is_available():
        print(f'CUDA device count: {torch.cuda.device_count()}')
        print(f'Current device: {torch.cuda.current_device()}')
        print(f'Device name: {torch.cuda.get_device_name()}')
except ImportError:
    print('PyTorch не установлен')

try:
    import sentence_transformers
    print(f'Sentence Transformers: {sentence_transformers.__version__}')
except ImportError:
    print('Sentence Transformers не установлен')
" || warn "Проблемы с GPU пакетами"
}

# Проверка доступности Redis
check_redis() {
    log "Проверка доступности Redis..."
    # Извлекаем хост и порт из REDIS_URL
    local redis_host
    local redis_port
    redis_host=$(echo "$REDIS_URL" | sed -E 's|redis://([^:/@]*:)?([^@]*)@?([^:/]+):([0-9]+).*|\3|')
    redis_port=$(echo "$REDIS_URL" | sed -E 's|redis://([^:/@]*:)?([^@]*)@?([^:/]+):([0-9]+).*|\4|')
    if [ -z "$redis_host" ] || [ -z "$redis_port" ]; then
        error "Не удалось разобрать REDIS_URL: $REDIS_URL"
        exit 1
    fi
    for i in {1..10}; do
        if timeout 2 bash -c "> /dev/tcp/$redis_host/$redis_port" 2>/dev/null; then
            log "Redis доступен на $redis_host:$redis_port"
            return 0
        else
            warn "Redis $redis_host:$redis_port недоступен, попытка $i/10..."
            sleep 2
        fi
    done
    error "Не удалось подключиться к Redis на $redis_host:$redis_port после 10 попыток"
    exit 1
}

# Функция остановки всех воркеров
cleanup() {
    log "Остановка всех GPU воркеров..."
    pkill -f "celery.*worker" || true
    sleep 2
    log "Все GPU воркеры остановлены"
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
    log "Запуск GPU воркеров на GPU сервере..."
    check_env
    check_redis
    check_gpu
    check_gpu_packages
    export PYTHONPATH="${PYTHONPATH}:."
    log "PYTHONPATH: $PYTHONPATH"
    log "REDIS_URL: $REDIS_URL"
    cleanup

    log "Запуск воркера embeddings_gpu (эмбеддинги)..."
    celery -A celery_app.celery_app worker \
        --loglevel=info \
        --queues=embeddings_gpu \
        --concurrency=$(get_worker_param embeddings_gpu concurrency) \
        --prefetch-multiplier=$(get_worker_param embeddings_gpu prefetch_multiplier) \
        --max-tasks-per-child=$(get_worker_param embeddings_gpu max_tasks_per_child) \
        --hostname=embeddings_gpu@%h &

    log "Запуск воркера scoring_tasks (реранкинг)..."
    celery -A celery_app.celery_app worker \
        --loglevel=info \
        --queues=scoring_tasks \
        --concurrency=$(get_worker_param scoring_tasks concurrency) \
        --prefetch-multiplier=$(get_worker_param scoring_tasks prefetch_multiplier) \
        --max-tasks-per-child=$(get_worker_param scoring_tasks max_tasks_per_child) \
        --hostname=scoring_tasks@%h &

    log "Запуск резервного воркера default (базовые задачи)..."
    celery -A celery_app.celery_app worker \
        --loglevel=info \
        --queues=default \
        --concurrency=$(get_worker_param default concurrency) \
        --prefetch-multiplier=$(get_worker_param default prefetch_multiplier) \
        --max-tasks-per-child=$(get_worker_param default max_tasks_per_child) \
        --hostname=default_gpu@%h &

    log "Все GPU воркеры запущены"
    log "Обрабатываемые очереди: embeddings_gpu, scoring_tasks, default"
    log "Для остановки нажмите Ctrl+C"
    while true; do
        sleep 30
        log "Статус GPU воркеров..."
        celery -A celery_app.celery_app inspect active --destination=embeddings_gpu@* --timeout=5 2>/dev/null || warn "embeddings_gpu воркер недоступен"
        celery -A celery_app.celery_app inspect active --destination=scoring_tasks@* --timeout=5 2>/dev/null || warn "scoring_tasks воркер недоступен"
    done
}

# Запуск основной функции
main "$@"
