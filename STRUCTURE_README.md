# Структура деплоймента HR Analysis

## Обзор новой организации

Структура deployment/ теперь разделена на три основные папки:

### 📁 deployment/common/
**Общие компоненты для всех серверов**

Содержит все файлы и папки, которые должны быть размещены на обоих типах серверов (CPU и GPU):

- `architecture_config.py` - общая конфигурация архитектуры
- `main.py` - основной скрипт запуска
- `start_celery.sh` - универсальный скрипт запуска Celery
- `requirements.txt` - общие зависимости
- `.env.example` - шаблон переменных окружения
- `install.sh` - скрипт установки
- `check_dependencies.sh` - проверка зависимостей

#### Поддиректории:
- `celery_app/` - конфигурация Celery
- `database/` - модели и конфигурация БД
- `models/` - модели данных
- `tasks/` - задачи Celery
- `utils/` - утилиты (логирование, мониторинг, секреты)
- `scripts/` - скрипты (health check, backup)

### 📁 deployment/cpu-server/
**Компоненты только для CPU сервера**

Содержит файлы, специфичные для CPU сервера:

- `cpu_server_config.py` - конфигурация CPU сервера
- `start_cpu_celery.sh` - запуск CPU воркеров
- `start_cpu_server.sh` - запуск CPU сервера

### 📁 deployment/gpu-server/
**Компоненты только для GPU сервера**

Содержит файлы, специфичные для GPU сервера:

- `gpu_server_config.py` - конфигурация GPU сервера
- `start_gpu_celery.sh` - запуск GPU воркеров
- `startup_gpu_server.sh` - запуск GPU сервера

## Точки входа

### 🖥️ Запуск CPU сервера
```bash
./start_cpu_deployment.sh
```

### 🚀 Запуск GPU сервера
```bash
./start_gpu_deployment.sh
```

## Принципы разделения

### Общие компоненты (common/)
- Архитектурная логика
- Базовые конфигурации
- Модели данных
- Универсальные задачи Celery
- Утилиты и скрипты
- Системы логирования и мониторинга

### CPU-специфичные компоненты
- Конфигурации для CPU воркеров
- Скрипты запуска CPU сервисов
- Fallback логика для GPU задач

### GPU-специфичные компоненты
- Конфигурации для GPU воркеров
- CUDA специфичные настройки
- Скрипты управления GPU инстансами

## Переменные окружения

### Общие
```bash
# Определение типа сервера (автоматически)
SERVER_TYPE=cpu  # или gpu
IS_GPU_SERVER=false  # или true

# Общие настройки
DATABASE_URL=postgresql://...
REDIS_URL=redis://...
ENVIRONMENT=production
```

### Только для GPU сервера
```bash
GPU_INSTANCE_NAME=gpu-server-instance
CUDA_VISIBLE_DEVICES=0
```

## Workflow запуска

### CPU сервер
1. Проверка окружения
2. Настройка PYTHONPATH
3. Загрузка общих компонентов
4. Применение CPU-специфичной конфигурации
5. Запуск CPU воркеров и Beat

### GPU сервер
1. Проверка окружения и CUDA
2. Настройка PYTHONPATH и CUDA_VISIBLE_DEVICES
3. Загрузка общих компонентов
4. Применение GPU-специфичной конфигурации
5. Запуск GPU воркеров

## Распределение задач

### CPU сервер обрабатывает:
- `search_basic` - базовые поисковые задачи
- `fillout` - интеграция с Fillout API
- `default` - сохранение результатов
- `notifications` - уведомления
- `embeddings_cpu` - fallback эмбеддинги (если GPU недоступен)
- `scoring_cpu` - fallback скоринг (если GPU недоступен)

### GPU сервер обрабатывает:
- `embeddings_gpu` - генерация эмбеддингов
- `scoring_gpu` - скоринг и реранкинг
- `ai_analysis` - LLM анализ

## Мониторинг

- Health check: `common/scripts/health_check.py`
- Worker monitor: `common/utils/worker_monitor.py`
- Логирование: `common/utils/logging_config.py`
- Backup: `common/scripts/create_backup.sh`

## Миграция с старой структуры

1. Все файлы из корня deployment/ перемещены в common/
2. Обновлены импорты и пути
3. Созданы точки входа для каждого типа сервера
4. Обновлены скрипты запуска с поддержкой типа сервера
