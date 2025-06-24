#!/bin/bash
# Скрипт проверки статуса сервисов HR Analysis

DEPLOYMENT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="${DEPLOYMENT_ROOT}/common"
PID_DIR="${COMMON_DIR}/pids"

echo "📊 Статус сервисов HR Analysis"
echo "=============================="

# Загрузка переменных окружения
if [ -f "${COMMON_DIR}/.env" ]; then
    set -a
    source "${COMMON_DIR}/.env"
    set +a
fi

# Функции проверки
check_service() {
    local service_name="$1"
    local pid_file="$2"
    local port="$3"
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 $pid 2>/dev/null; then
            echo "✅ $service_name: Работает (PID: $pid)"
            if [ -n "$port" ]; then
                if netstat -an 2>/dev/null | grep ":$port " >/dev/null; then
                    echo "   └─ Порт $port: Доступен"
                else
                    echo "   └─ Порт $port: Недоступен"
                fi
            fi
        else
            echo "❌ $service_name: PID файл существует, но процесс не найден"
            rm -f "$pid_file"
        fi
    else
        echo "❌ $service_name: Не запущен"
    fi
}

# Проверка PostgreSQL
echo ""
echo "🐘 PostgreSQL:"
if pg_isready -h "${DB_HOST:-localhost}" -p "${DB_PORT:-5432}" >/dev/null 2>&1; then
    echo "✅ PostgreSQL: Доступен (${DB_HOST:-localhost}:${DB_PORT:-5432})"
    if psql -h "${DB_HOST:-localhost}" -p "${DB_PORT:-5432}" -U "${DB_USER}" -d "${DB_NAME}" -c "SELECT 1;" >/dev/null 2>&1; then
        echo "   └─ База данных '$DB_NAME': Доступна"
    else
        echo "   └─ База данных '$DB_NAME': Недоступна"
    fi
else
    echo "❌ PostgreSQL: Недоступен"
fi

# Проверка Redis
echo ""
echo "🔴 Redis:"
if redis-cli -h "${REDIS_HOST:-localhost}" -p "${REDIS_PORT:-6379}" ping >/dev/null 2>&1; then
    echo "✅ Redis: Доступен (${REDIS_HOST:-localhost}:${REDIS_PORT:-6379})"
else
    echo "❌ Redis: Недоступен"
fi

# Проверка сервисов Celery
echo ""
echo "🔄 Celery сервисы:"
check_service "Celery Workers" "${PID_DIR}/celery_worker.pid"
check_service "Celery Beat" "${PID_DIR}/celery_beat.pid"
check_service "Flower" "${PID_DIR}/flower.pid" "${FLOWER_PORT:-5555}"

# Проверка активных воркеров через Celery
echo ""
echo "👷 Активные воркеры:"
cd "${COMMON_DIR}"
if celery -A celery_app.celery_app inspect active --timeout=5 2>/dev/null | grep -q "cpu-worker"; then
    echo "✅ CPU воркеры активны"
    
    # Показать статистику очередей
    echo ""
    echo "📋 Статистика очередей:"
    celery -A celery_app.celery_app inspect active_queues --timeout=5 2>/dev/null | grep -E "(cpu-worker|queue)" || echo "   Информация недоступна"
else
    echo "❌ Воркеры неактивны или недоступны"
fi

# Проверка доступности Flower веб-интерфейса
echo ""
echo "🌸 Flower веб-интерфейс:"
if curl -s "http://localhost:${FLOWER_PORT:-5555}" >/dev/null 2>&1; then
    echo "✅ Flower: http://localhost:${FLOWER_PORT:-5555}"
else
    echo "❌ Flower веб-интерфейс недоступен"
fi

echo ""
echo "📁 Логи доступны в: ${COMMON_DIR}/logs/"
echo "🔧 Управление:"
echo "   Запуск:    ./start_cpu_deployment.sh"
echo "   Остановка: ./stop_services.sh"
echo "   Статус:    ./check_status.sh"
