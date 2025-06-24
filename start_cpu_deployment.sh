#!/bin/bash
# Полнофункциональный скрипт запуска CPU сервера HR Analysis
# Включает: PostgreSQL, Redis, Flower, Celery workers, Beat

set -e

DEPLOYMENT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="${DEPLOYMENT_ROOT}/common"
LOG_DIR="${COMMON_DIR}/logs"
PID_DIR="${COMMON_DIR}/pids"

# Создание директорий
mkdir -p "${LOG_DIR}" "${PID_DIR}"

echo "🖥️  Полный запуск CPU сервера HR Analysis"
echo "==========================================="

# Импорт функций логирования
source "${COMMON_DIR}/logging_functions.sh" 2>/dev/null || {
    log_info() { echo "[$(date '+%H:%M:%S')] $1"; }
    log_success() { echo "[$(date '+%H:%M:%S')] ✅ $1"; }
    log_error() { echo "[$(date '+%H:%M:%S')] ❌ $1" >&2; }
    log_warning() { echo "[$(date '+%H:%M:%S')] ⚠️ $1"; }
}

# Функция для проверки и завершения процессов
cleanup() {
    log_info "Завершение работы..."
    
    # Используем stop_all из start_celery.sh
    cd "${COMMON_DIR}"
    ./start_celery.sh stop
    
    # Завершение Flower
    if [ -f "${PID_DIR}/flower.pid" ]; then
        kill $(cat "${PID_DIR}/flower.pid") 2>/dev/null || true
        rm -f "${PID_DIR}/flower.pid"
    fi
    
    log_info "Завершение завершено"
    exit 0
}

# Обработка сигналов завершения
trap cleanup SIGINT SIGTERM

# Проверка виртуального окружения
if [[ "$VIRTUAL_ENV" == "" ]]; then
    log_error "Виртуальное окружение не активировано!"
    log_info "Активируйте окружение командой: source venv/bin/activate"
    exit 1
fi

# Переход в рабочую директорию
cd "${COMMON_DIR}"

# Проверка .env файла
if [ ! -f ".env" ]; then
    log_error "Файл .env не найден!"
    log_info "Создайте .env файл на основе .env.example"
    exit 1
fi

# Загрузка переменных окружения
set -a  # автоматический экспорт переменных
source .env
set +a

log_success "Переменные окружения загружены"

# Экспорт системных переменных
export PYTHONPATH="${PYTHONPATH}:${COMMON_DIR}:${DEPLOYMENT_ROOT}"
export SERVER_TYPE="cpu"
export IS_GPU_SERVER="false"

# Определение переменных для отчетности
CONCURRENCY="10"  # 2*5 воркеров (fillout_processing, text_processing, embeddings, reranking, orchestration)
QUEUES="fillout_processing,text_processing,embeddings,reranking,orchestration"
WORKER_LOG="${LOG_DIR}/celery_workers.log"
BEAT_LOG="${LOG_DIR}/celery_beat.log"

log_info "📁 Рабочая директория: ${COMMON_DIR}"
log_info "🔧 PYTHONPATH: ${PYTHONPATH}"
log_info "💻 Тип сервера: CPU"

# ========== ИНИЦИАЛИЗАЦИЯ POSTGRESQL ==========
log_info "📊 Настройка PostgreSQL..."

# Проверка подключения к PostgreSQL
if ! pg_isready -h "${DB_HOST:-localhost}" -p "${DB_PORT:-5432}" >/dev/null 2>&1; then
    log_error "PostgreSQL недоступен на ${DB_HOST:-localhost}:${DB_PORT:-5432}"
    log_info "Убедитесь, что PostgreSQL запущен"
    exit 1
fi

# Инициализация базы данных через Python скрипт
log_info "Запуск инициализации базы данных..."
if [ -f "database/init/init_database.py" ]; then
    if python database/init/init_database.py; then
        log_success "База данных инициализирована успешно"
    else
        log_error "Ошибка инициализации базы данных"
        log_info "Попробуйте инициализировать БД вручную:"
        log_info "  python database/init/init_database.py"
        exit 1
    fi
else
    log_error "Скрипт инициализации БД не найден: database/init/init_database.py"
    log_info "Текущая директория: $(pwd)"
    log_info "Создайте скрипт инициализации или настройте БД вручную"
    exit 1
fi

# ========== ПРОВЕРКА REDIS ==========
log_info "🔴 Проверка Redis..."

if ! redis-cli -h "${REDIS_HOST:-localhost}" -p "${REDIS_PORT:-6379}" ping >/dev/null 2>&1; then
    log_error "Redis недоступен на ${REDIS_HOST:-localhost}:${REDIS_PORT:-6379}"
    log_info "Убедитесь, что Redis запущен"
    exit 1
fi

log_success "Redis доступен"

# ========== ЗАПУСК FLOWER ==========
log_info "🌸 Запуск Flower мониторинга..."

FLOWER_PORT="${FLOWER_PORT:-5555}"
FLOWER_LOG="${LOG_DIR}/flower.log"

nohup celery -A celery_app.celery_app flower \
    --port="${FLOWER_PORT}" \
    --broker="redis://${REDIS_HOST:-localhost}:${REDIS_PORT:-6379}/${REDIS_DB:-0}" \
    --logging=info \
    > "${FLOWER_LOG}" 2>&1 &

FLOWER_PID=$!
echo $FLOWER_PID > "${PID_DIR}/flower.pid"

log_success "Flower запущен на порту ${FLOWER_PORT} (PID: ${FLOWER_PID})"
log_info "Flower доступен по адресу: http://localhost:${FLOWER_PORT}"

# ========== ЗАПУСК CELERY WORKERS И BEAT ==========
log_info "👷 Запуск Celery Workers и Beat..."

# Используем обновленный start_celery.sh с правильными очередями
# Он сам запустит и воркеров, и beat
cd "${COMMON_DIR}"
./start_celery.sh --server-type=cpu start

# Проверяем статус запущенных воркеров
sleep 2
./start_celery.sh status

# ========== ПРОВЕРКА СТАТУСА ==========
log_info "📋 Проверка статуса сервисов..."

sleep 3  # Даем время на запуск

# Проверка Flower
if curl -s "http://localhost:${FLOWER_PORT}" >/dev/null 2>&1; then
    log_success "Flower работает"
else
    log_warning "Flower может быть еще не готов"
fi

# Проверка Celery Workers
if celery -A celery_app.celery_app inspect active >/dev/null 2>&1; then
    log_success "Celery Workers активны"
else
    log_warning "Celery Workers могут быть еще не готовы"
fi

# ========== ИНФОРМАЦИЯ О ЗАПУСКЕ ==========
echo ""
log_success "🎉 CPU сервер HR Analysis успешно запущен!"
echo ""
echo "� Статус сервисов:"
echo "  • PostgreSQL: ${DB_HOST:-localhost}:${DB_PORT:-5432}/${DB_NAME}"
echo "  • Redis:      ${REDIS_HOST:-localhost}:${REDIS_PORT:-6379}"
echo "  • Flower:     http://localhost:${FLOWER_PORT}"
echo "  • Celery:     ${CONCURRENCY} воркеров на очередях [${QUEUES}]"
echo ""
echo "📁 Логи:"
echo "  • Flower:     ${FLOWER_LOG}"
echo "  • Beat:       ${BEAT_LOG}" 
echo "  • Workers:    ${WORKER_LOG}"
echo ""
echo "🔧 Управление:"
echo "  • Просмотр логов: tail -f ${LOG_DIR}/*.log"
echo "  • Остановка:     Ctrl+C или kill -TERM $$"
echo ""

# ========== АВТОМАТИЧЕСКИЙ ЗАПУСК ЗАДАЧ ==========
log_info "🚀 Запуск автоматических задач обработки данных..."

# Ждем, чтобы все воркеры полностью инициализировались
sleep 5

# Запускаем полный цикл обработки данных
cd "${COMMON_DIR}"
python -c "
import sys
sys.path.append('.')
from celery_app.celery_app import app

# Запускаем полный workflow обработки данных
result = app.send_task('tasks.workflows.run_full_processing_pipeline')
print(f'✅ Запущен полный цикл обработки данных: {result.id}')
print(f'🌐 Отслеживайте прогресс в Flower: http://localhost:5555/task/{result.id}')
"

log_success "🎯 Автоматические задачи запущены!"
echo ""

# ========== МОНИТОРИНГ ==========
log_info "🔍 Мониторинг системы... (Ctrl+C для остановки)"

# Бесконечный цикл мониторинга
while true; do
    sleep 30
    
    # Проверка живости процессов
    if [ -f "${PID_DIR}/flower.pid" ]; then
        if ! kill -0 $(cat "${PID_DIR}/flower.pid") 2>/dev/null; then
            log_warning "Flower процесс завершился"
        fi
    fi
    
    # Проверка воркеров по очередям
    for queue in fillout_processing text_processing embeddings reranking orchestration; do
        if [ -f "${PID_DIR}/celery_${queue}.pid" ]; then
            if ! kill -0 $(cat "${PID_DIR}/celery_${queue}.pid") 2>/dev/null; then
                log_warning "Celery Worker ${queue} завершился"
            fi
        fi
    done
    
    if [ -f "${PID_DIR}/celery_beat.pid" ]; then
        if ! kill -0 $(cat "${PID_DIR}/celery_beat.pid") 2>/dev/null; then
            log_warning "Celery Beat процесс завершился"
        fi
    fi
done
