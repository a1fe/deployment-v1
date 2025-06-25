#!/bin/bash
# Запуск Flower для мониторинга Celery задач

echo "🌸 Запуск Flower - мониторинг Celery"
echo "Проект: HR Analysis System"
echo "URL: http://localhost:5555"
echo "============================"

cd "$(dirname "$0")"

# Запускаем Flower
exec celery -A common.celery_app.celery_app:celery_app flower \
    --port=5555 \
    --broker=redis://localhost:6379/0
