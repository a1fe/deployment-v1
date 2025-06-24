#!/bin/bash
#
# Скрипт запуска Celery для HR Analysis (Deployment Version)
# Универсальный скрипт с бизнес-архитектурой очередей
#

# Парсинг аргументов
SERVER_TYPE=""
COMMAND=""

# Обрабатываем все аргументы
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
        start|stop|restart|status|logs|flower)
            COMMAND="$1"
            shift
            ;;
        *)
            # Если это не флаг и не команда, считаем это командой (для обратной совместимости)
            if [[ -z "$COMMAND" ]]; then
                COMMAND="$1"
            fi
            shift
            ;;
    esac
done

# Определение типа сервера (для совместимости, но не используется в новой архитектуре)
if [[ -z "$SERVER_TYPE" ]]; then
    SERVER_TYPE="unified"  # Единая архитектура для всех типов серверов
fi

echo "🔧 Режим запуска: $SERVER_TYPE"

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
export ENVIRONMENT="development"  # Используем development для тестирования
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
    sleep 5  # Увеличиваем время ожидания для полной инициализации
    if check_worker_status "$queue"; then
        log_message "✅ Воркер $queue успешно запущен"
        return 0
    else
        log_message "⚠️ Воркер $queue может еще инициализироваться"
        return 0  # Не считаем это ошибкой, т.к. процессы запускаются асинхронно
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

# Обработка команд
case "$COMMAND" in
    "start")
        echo "🚀 Запуск воркеров для ${SERVER_TYPE} сервера..."
        
        # Новая бизнес-архитектура очередей
        start_worker "fillout_processing" 2      # Получение данных из внешних источников (Fillout API)
        start_worker "text_processing" 2         # Обработка и парсинг текстов (резюме, вакансии)
        start_worker "embeddings" 2              # Генерация эмбеддингов
        start_worker "reranking" 1               # AI-реранжирование результатов (concurrency=1 для избежания OOM)
        start_worker "orchestration" 2           # Управление workflow и координация задач
        # Запуск планировщика (beat)
        start_beat
        
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
        echo "Активные очереди (бизнес-логика):"
        echo "  • fillout_processing  - Получение данных из внешних источников (Fillout API)"
        echo "  • text_processing     - Обработка и парсинг текстов (резюме, вакансии)"
        echo "  • embeddings          - Генерация эмбеддингов"
        echo "  • reranking           - AI-реранжирование результатов"
        echo "  • orchestration       - Управление workflow и координация задач"
        exit 1
        ;;
esac
