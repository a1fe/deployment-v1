#!/bin/bash
"""
Скрипт запуска Celery для HR Analysis (Deployment Version)
Универсальный скрипт для CPU и GPU серверов
"""

# Парсинг аргументов
SERVER_TYPE=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --server-type=*)
            SERVER_TYPE="${1#*=}"
            shift
            ;;
        --server-type)
            SERVER_TYPE="$2"
            shift 2
            ;;
        *)
            # Другие аргументы передаем дальше
            shift
            ;;
    esac
done

# Определение типа сервера
if [[ -z "$SERVER_TYPE" ]]; then
    if [[ -n "$GPU_INSTANCE_NAME" ]]; then
        SERVER_TYPE="gpu"
    else
        SERVER_TYPE="cpu"
    fi
fi

echo "🔧 Тип сервера: $SERVER_TYPE"

# Настройка логирования
LOG_DIR="logs"
PID_DIR="pids"
SCRIPT_LOG="${LOG_DIR}/celery_management_${SERVER_TYPE}.log"

# Функция логирования
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "${SCRIPT_LOG}"
}

# Создание директорий
mkdir -p "${LOG_DIR}" "${PID_DIR}"

# Проверка виртуального окружения
if [[ "$VIRTUAL_ENV" == "" ]]; then
    log_message "❌ Виртуальное окружение не активировано!"
    echo "Активируйте окружение командой: source venv/bin/activate"
    exit 1
fi

log_message "✅ Виртуальное окружение активно: $VIRTUAL_ENV"

# Настройка переменных окружения
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
export ENVIRONMENT="production"
log_message "🔧 Переменные окружения настроены"

# ИСПРАВЛЕНИЕ: Проверка состояния воркеров
check_worker_status() {
    local queue=$1
    local pidfile="pids/celery_${queue}.pid"
    
    if [[ -f "$pidfile" ]]; then
        local pid=$(cat "$pidfile")
        if ps -p "$pid" > /dev/null 2>&1; then
            log_message "✅ Воркер ${queue} работает (PID: $pid)"
            return 0
        else
            log_message "❌ Воркер ${queue} не отвечает (устаревший PID: $pid)"
            rm -f "$pidfile"
            return 1
        fi
    else
        log_message "ℹ️ Воркер ${queue} не запущен"
        return 1
    fi
}

# Функция запуска воркера
start_worker() {
    local queue=$1
    local concurrency=$2
    local worker_name="worker_${queue}"
    
    # ИСПРАВЛЕНИЕ: Проверка существующего воркера
    if check_worker_status "$queue"; then
        log_message "⚠️ Воркер $queue уже запущен, пропускаем"
        return 0
    fi
    
    log_message "🚀 Запуск воркера для очереди: ${queue} (concurrency: ${concurrency})"
    
    celery -A celery_app.celery_app worker \
        --queues="${queue}" \
        --concurrency="${concurrency}" \
        --hostname="${worker_name}@%h" \
        --loglevel=info \
        --logfile="${LOG_DIR}/celery_${queue}.log" \
        --pidfile="${PID_DIR}/celery_${queue}.pid" \
        --detach
    
    # Проверяем успешность запуска
    sleep 2
    if check_worker_status "$queue"; then
        log_message "✅ Воркер $queue успешно запущен"
        return 0
    else
        log_message "❌ Ошибка запуска воркера $queue"
        return 1
    fi
}

# Функция запуска beat
start_beat() {
    local pidfile="${PID_DIR}/celery_beat.pid"
    
    # Проверяем существующий процесс
    if [[ -f "$pidfile" ]]; then
        local pid=$(cat "$pidfile")
        if ps -p "$pid" > /dev/null 2>&1; then
            log_message "⚠️ Celery Beat уже запущен (PID: $pid)"
            return 0
        else
            log_message "🧹 Удаляем устаревший PID файл Beat"
            rm -f "$pidfile"
        fi
    fi
    
    log_message "⏰ Запуск Celery Beat (планировщик)"
    
    celery -A celery_app.celery_app beat \
        --loglevel=info \
        --logfile="${LOG_DIR}/celery_beat.log" \
        --pidfile="$pidfile" \
        --detach
    
    # Проверяем успешность запуска
    sleep 2
    if [[ -f "$pidfile" ]] && ps -p "$(cat "$pidfile")" > /dev/null 2>&1; then
        log_message "✅ Celery Beat успешно запущен"
        return 0
    else
        log_message "❌ Ошибка запуска Celery Beat"
        return 1
    fi
}

# Функция остановки всех процессов
stop_all() {
    echo "🛑 Остановка всех Celery процессов..."
    
    # Остановка воркеров
    if ls pids/celery_*.pid 1> /dev/null 2>&1; then
        for pidfile in pids/celery_*.pid; do
            if [ -f "$pidfile" ]; then
                pid=$(cat "$pidfile")
                echo "Остановка процесса ${pid}..."
                kill "$pid" 2>/dev/null || echo "Процесс ${pid} уже остановлен"
                rm -f "$pidfile"
            fi
        done
    fi
    
    # Дополнительная очистка процессов
    pkill -f "celery.*worker" 2>/dev/null || true
    pkill -f "celery.*beat" 2>/dev/null || true
    
    echo "✅ Все процессы остановлены"
}

# Функция проверки статуса
check_status() {
    echo "📊 Статус Celery процессов:"
    
    if ls pids/celery_*.pid 1> /dev/null 2>&1; then
        for pidfile in pids/celery_*.pid; do
            if [ -f "$pidfile" ]; then
                pid=$(cat "$pidfile")
                queue=$(basename "$pidfile" .pid | sed 's/celery_//')
                if kill -0 "$pid" 2>/dev/null; then
                    echo "✅ ${queue}: работает (PID: ${pid})"
                else
                    echo "❌ ${queue}: не работает (PID файл устарел)"
                    rm -f "$pidfile"
                fi
            fi
        done
    else
        echo "❌ Нет запущенных процессов"
    fi
}

# Создание необходимых папок
mkdir -p logs pids

# Обработка аргументов
case "$1" in
    "start")
        echo "🚀 Запуск воркеров для ${SERVER_TYPE} сервера..."
        
        if [[ "$SERVER_TYPE" == "gpu" ]]; then
            # GPU сервер - только GPU-специфичные задачи
            start_worker "embeddings_gpu" 2    # GPU эмбеддинги
            start_worker "scoring_gpu" 1       # GPU скоринг и реранкинг
            start_worker "ai_analysis" 1       # LLM анализ
        else
            # CPU сервер - основные задачи и fallback
            start_worker "search_basic" 2      # Поисковые задачи
            start_worker "fillout" 2          # Получение данных Fillout
            start_worker "default" 2          # Сохранение результатов
            start_worker "notifications" 1     # Уведомления
            
            # Fallback эмбеддинги, если GPU недоступен
            if [[ -z "$GPU_INSTANCE_NAME" ]]; then
                start_worker "embeddings_cpu" 1    # CPU эмбеддинги (fallback)
                start_worker "scoring_cpu" 1       # CPU скоринг (fallback)
            fi
            
            # Запуск планировщика только на CPU сервере
            start_beat
        fi
        
        echo "✅ Воркеры ${SERVER_TYPE} сервера запущены!"
        echo "📝 Логи в папке: logs/"
        echo "🔧 PID файлы в папке: pids/"
        ;;
        
    "stop")
        stop_all
        ;;
        
    "restart")
        echo "🔄 Перезапуск всех воркеров..."
        stop_all
        sleep 2
        $0 start
        ;;
        
    "status")
        check_status
        ;;
        
    "logs")
        echo "📝 Просмотр логов..."
        if [ -n "$2" ]; then
            tail -f "logs/celery_${2}.log"
        else
            echo "Доступные логи:"
            ls logs/celery_*.log 2>/dev/null || echo "Нет файлов логов"
        fi
        ;;
        
    "flower")
        echo "🌸 Запуск Flower (мониторинг Celery)..."
        flower -A celery_app.celery_app --port=5555 --broker=redis://localhost:6379/0
        ;;
        
    *)
        echo "📖 Использование: $0 {start|stop|restart|status|logs [queue]|flower}"
        echo ""
        echo "Команды:"
        echo "  start   - Запустить все активные воркеры"
        echo "  stop    - Остановить все воркеры"
        echo "  restart - Перезапустить все воркеры"
        echo "  status  - Показать статус воркеров"
        echo "  logs    - Показать логи (опционально указать очередь)"
        echo "  flower  - Запустить веб-интерфейс мониторинга"
        echo ""
        echo "Активные очереди:"
        echo "  • embeddings_gpu - Workflow и эмбеддинги"
        echo "  • search_basic   - Поисковые задачи"
        echo "  • scoring_tasks  - Реранкинг"
        echo "  • fillout        - Получение данных"
        echo "  • default        - Сохранение результатов"
        exit 1
        ;;
esac
