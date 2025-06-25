#!/bin/bash
# Запуск Celery worker для всех очередей (для разработки)

echo "🚀 Запуск Celery worker для всех очередей..."
echo "Проект: HR Analysis System"
echo "Режим: Development (все очереди)"
echo "================================="

cd "$(dirname "$0")"

# Останавливаем все старые процессы Celery
echo "🛑 Останавливаем старые процессы..."
pkill -f celery 2>/dev/null || true

# Очищаем Redis от старых задач (опционально)
echo "🧹 Очищаем Redis от старых задач..."
redis-cli flushall > /dev/null 2>&1 || echo "⚠️ Redis не доступен или уже очищен"

echo ""
echo "🔄 Запускаем worker..."

# Запускаем worker со всеми очередями
exec celery -A common.celery_app.celery_app:celery_app worker \
    --loglevel=info \
    --concurrency=2 \
    --queues=celery,fillout_processing,text_processing,embeddings,reranking,orchestration \
    --hostname=worker_all@%h
