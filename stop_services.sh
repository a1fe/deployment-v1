#!/bin/bash
# Скрипт остановки всех сервисов HR Analysis

set -e

DEPLOYMENT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="${DEPLOYMENT_ROOT}/common"
PID_DIR="${COMMON_DIR}/pids"

echo "🛑 Остановка HR Analysis сервисов..."

# Функция логирования
log_info() { echo "[$(date '+%H:%M:%S')] $1"; }
log_success() { echo "[$(date '+%H:%M:%S')] ✅ $1"; }

# Остановка всех Celery Workers
for worker_file in "${PID_DIR}"/celery_worker_*.pid; do
    if [ -f "$worker_file" ]; then
        PID=$(cat "$worker_file")
        WORKER_NAME=$(basename "$worker_file" .pid)
        if kill -0 $PID 2>/dev/null; then
            log_info "Остановка $WORKER_NAME (PID: $PID)..."
            kill -TERM $PID
            sleep 3
            kill -9 $PID 2>/dev/null || true
            log_success "$WORKER_NAME остановлен"
        fi
        rm -f "$worker_file"
    fi
done

# Остановка старого worker'а (для совместимости)
if [ -f "${PID_DIR}/celery_worker.pid" ]; then
    PID=$(cat "${PID_DIR}/celery_worker.pid")
    if kill -0 $PID 2>/dev/null; then
        log_info "Остановка Celery Workers (PID: $PID)..."
        kill -TERM $PID
        sleep 3
        kill -9 $PID 2>/dev/null || true
        log_success "Celery Workers остановлены"
    fi
    rm -f "${PID_DIR}/celery_worker.pid"
fi

# Остановка Celery Beat
if [ -f "${PID_DIR}/celery_beat.pid" ]; then
    PID=$(cat "${PID_DIR}/celery_beat.pid")
    if kill -0 $PID 2>/dev/null; then
        log_info "Остановка Celery Beat (PID: $PID)..."
        kill -TERM $PID
        sleep 2
        kill -9 $PID 2>/dev/null || true
        log_success "Celery Beat остановлен"
    fi
    rm -f "${PID_DIR}/celery_beat.pid"
fi

# Остановка Flower
if [ -f "${PID_DIR}/flower.pid" ]; then
    PID=$(cat "${PID_DIR}/flower.pid")
    if kill -0 $PID 2>/dev/null; then
        log_info "Остановка Flower (PID: $PID)..."
        kill -TERM $PID
        sleep 2
        kill -9 $PID 2>/dev/null || true
        log_success "Flower остановлен"
    fi
    rm -f "${PID_DIR}/flower.pid"
fi

# Принудительная очистка всех связанных процессов
log_info "Очистка связанных процессов..."
pkill -f "celery.*worker" 2>/dev/null || true
pkill -f "celery.*beat" 2>/dev/null || true
pkill -f "celery.*flower" 2>/dev/null || true

log_success "🎉 Все сервисы HR Analysis остановлены"
