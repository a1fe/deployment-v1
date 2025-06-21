#!/bin/bash
"""
Точка входа для запуска CPU сервера
"""

set -e

DEPLOYMENT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CPU_SERVER_DIR="${DEPLOYMENT_ROOT}/cpu-server"
COMMON_DIR="${DEPLOYMENT_ROOT}/common"

echo "🖥️  Запуск CPU сервера HR Analysis"
echo "================================="

# Проверка окружения
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo "❌ Виртуальное окружение не активировано!"
    echo "Активируйте окружение командой: source venv/bin/activate"
    exit 1
fi

# Экспорт переменных
export PYTHONPATH="${PYTHONPATH}:${COMMON_DIR}:${DEPLOYMENT_ROOT}"
export SERVER_TYPE="cpu"
export IS_GPU_SERVER="false"

echo "📁 Общие компоненты: ${COMMON_DIR}"
echo "📁 CPU специфичные: ${CPU_SERVER_DIR}"
echo "🔧 PYTHONPATH: ${PYTHONPATH}"

# Переходим в директорию с общими компонентами
cd "${COMMON_DIR}"

# Запуск через общий скрипт с CPU конфигурацией
echo "🚀 Запуск CPU Celery worker..."
exec "${COMMON_DIR}/start_celery.sh" --server-type=cpu "$@"
