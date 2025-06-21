#!/bin/bash
"""
Точка входа для запуска GPU сервера
"""

set -e

DEPLOYMENT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GPU_SERVER_DIR="${DEPLOYMENT_ROOT}/gpu-server"
COMMON_DIR="${DEPLOYMENT_ROOT}/common"

echo "🚀 Запуск GPU сервера HR Analysis"
echo "================================="

# Проверка окружения
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo "❌ Виртуальное окружение не активировано!"
    echo "Активируйте окружение командой: source venv/bin/activate"
    exit 1
fi

# Проверка CUDA
if ! nvidia-smi &> /dev/null; then
    echo "⚠️  Предупреждение: nvidia-smi недоступен. GPU может быть недоступен."
fi

# Экспорт переменных
export PYTHONPATH="${PYTHONPATH}:${COMMON_DIR}:${DEPLOYMENT_ROOT}"
export SERVER_TYPE="gpu"
export IS_GPU_SERVER="true"
export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0}"

echo "📁 Общие компоненты: ${COMMON_DIR}"
echo "📁 GPU специфичные: ${GPU_SERVER_DIR}"
echo "🔧 PYTHONPATH: ${PYTHONPATH}"
echo "🎮 CUDA_VISIBLE_DEVICES: ${CUDA_VISIBLE_DEVICES}"

# Переходим в директорию с общими компонентами
cd "${COMMON_DIR}"

# Запуск через общий скрипт с GPU конфигурацией
echo "🚀 Запуск GPU Celery worker..."
exec "${COMMON_DIR}/start_celery.sh" --server-type=gpu "$@"
