# CPU/GPU Архитектура для HR Analysis

Этот проект реализует архитектуру с разделением на CPU и GPU серверы для оптимизации производительности и затрат.

## 🏗️ Архитектура

### CPU-сервер (основной, e2-standard-2)
- **Постоянно работающий**
- Компоненты: API, Celery Beat, Redis, PostgreSQL, CPU воркеры
- Задачи: базовые workflow, API интеграция, поиск, координация

### GPU-сервер (n1-standard-4 + T4)
- **Включается по требованию**
- Компоненты: GPU воркеры, PyTorch + CUDA
- Задачи: эмбеддинги, реранкинг, LLM анализ

## 🚀 Быстрый старт

### 1. Проверка конфигурации
```bash
python deployment/architecture_config.py config
```

### 2. Запуск CPU сервера
```bash
./deployment/cpu-server/start_cpu_celery.sh
```

### 3. Запуск GPU сервера (при наличии)
```bash
./deployment/gpu-server/start_gpu_celery.sh
```

## ⚙️ Конфигурация

### Переменные окружения

#### Основные
```bash
REDIS_URL=redis://localhost:6379/0
DATABASE_URL=postgresql://user:pass@host:5432/db
ENVIRONMENT=production
```

#### Для GPU функциональности
```bash
# Если установлено - включает GPU режим
GPU_INSTANCE_NAME=gpu-server-instance

# Только на GPU сервере
CUDA_VISIBLE_DEVICES=0
```

## 📋 Маршрутизация задач

### Без GPU (CPU режим)
- Все задачи → CPU очереди (`default`, `fillout`, `search_basic`)

### С GPU
- Эмбеддинги → `embeddings_gpu`
- Реранкинг → `scoring_tasks` 
- Остальные → CPU очереди

## 🔧 Управление

### Ручное управление GPU инстансом
```bash
# Запуск
python deployment/utils/gcloud_manager.py start

# Остановка  
python deployment/utils/gcloud_manager.py stop

# Статус
python deployment/utils/gcloud_manager.py status
```

### Автоматический мониторинг
```bash
python deployment/utils/gpu_monitor.py
```

## 📊 Мониторинг

### Проверка воркеров
```bash
celery -A celery_app.celery_app inspect active
```

### Web-интерфейс (Flower)
```bash
celery -A celery_app.celery_app flower --port=5555
```

## 📁 Структура файлов

```
deployment/
├── celery_app/
│   └── celery_env_config.py    # Основная конфигурация
├── cpu-server/
│   └── start_cpu_celery.sh     # Скрипт запуска CPU воркеров
├── gpu-server/
│   └── start_gpu_celery.sh     # Скрипт запуска GPU воркеров
├── utils/
│   ├── gcloud_manager.py       # Управление GPU инстансом
│   └── gpu_monitor.py          # Автоматический мониторинг
├── architecture_config.py      # Менеджер архитектуры
└── DEPLOYMENT_GUIDE.md        # Подробное руководство
```

## 🎯 Принципы работы

1. **Условная маршрутизация**: Задачи автоматически направляются на CPU или GPU в зависимости от наличия `GPU_INSTANCE_NAME`

2. **Автоматическое управление**: GPU сервер включается при появлении задач и выключается при простое

3. **Отказоустойчивость**: При недоступности GPU все задачи выполняются на CPU

4. **Экономия**: GPU сервер работает только при необходимости

## 📖 Документация

- [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) - Подробное руководство по развертыванию
- [architecture_config.py](architecture_config.py) - Конфигурация и валидация
- [celery_env_config.py](celery_app/celery_env_config.py) - Настройки Celery

## 🔍 Диагностика

### Проверка статуса
```bash
python deployment/architecture_config.py validate
```

### Просмотр логов
```bash
# CPU воркеры
sudo journalctl -u hr-celery-cpu -f

# GPU мониторинг
sudo journalctl -u hr-gpu-monitor -f
```

---

## 🎯 Основные возможности (Legacy)
1. **Resume Processing Chain** - Обработка резюме
   - Получение данных из Fillout (резюме)
   - Генерация эмбеддингов
   - Поиск подходящих вакансий
   - Реранкинг результатов
   - Сохранение анализа

2. **Job Processing Chain** - Обработка вакансий
   - Получение данных из Fillout (вакансии)
   - Генерация эмбеддингов
   - Поиск подходящих кандидатов
   - Реранкинг результатов
   - Сохранение анализа

### Архитектура

```
deployment/
├── celery_app/           # Конфигурация Celery
├── tasks/               # Задачи для workflow
├── database/            # База данных и операции
├── models/              # Модели данных
├── utils/               # Утилиты
├── main.py             # Основной скрипт
├── start_celery.sh     # Скрипт запуска Celery
└── requirements.txt    # Зависимости
```

## 🚀 Быстрый старт

### Автоматическая установка (рекомендуется)

```bash
# 1. Проверка системы
./check_dependencies.sh

# 2. Автоматическая установка всего необходимого
./install.sh

# 3. Запуск системы
./start_celery.sh start
```

### Ручная установка

### 1. Установка зависимостей

```bash
# Создание виртуального окружения
python3.13 -m venv venv
source venv/bin/activate

# Установка зависимостей
pip install -r requirements.txt
```

### 2. Настройка переменных окружения

Создайте файл `.env`:

```bash
cp .env.example .env
nano .env
```

### 3. Инициализация базы данных

```bash
python main.py
# Выберите опцию 1 для инициализации БД
# Выберите опцию 3 для инициализации справочников
```

### 4. Запуск Celery

```bash
# Запуск всех воркеров
./start_celery.sh start

# Проверка статуса
./start_celery.sh status

# Просмотр логов
./start_celery.sh logs

# Запуск веб-мониторинга
./start_celery.sh flower
```

## ⚙️ Воркеры и очереди

Система использует 5 специализированных воркеров:

### Активные воркеры
- **embeddings_gpu** (1 процесс) - Workflow и эмбеддинги
- **search_basic** (2 процесса) - Поисковые задачи
- **scoring_tasks** (1 процесс) - Реранкинг с BGE-M3
- **fillout** (2 процесса) - Получение данных из Fillout
- **default** (2 процесса) - Сохранение результатов

### Расписание
- **Resume Processing Chain**: каждые 30 минут
- **Job Processing Chain**: каждые 45 минут

## 📊 Мониторинг

### Проверка зависимостей
```bash
# Полная проверка системы
./check_dependencies.sh
```

### Flower (веб-интерфейс)
```bash
./start_celery.sh flower
# Открыть: http://localhost:5555
```

### Логи
```bash
# Все логи
./start_celery.sh logs

# Конкретный воркер
./start_celery.sh logs embeddings_gpu
```

### Статус системы
```bash
python main.py
# Выберите опцию 7
```

## 🧪 Тестирование

### Тест workflow
```bash
python main.py
# Выберите опцию 8 для запуска тестовых workflow
```

### Ручной запуск задач
```python
from celery_app.celery_app import get_celery_app

app = get_celery_app()

# Запуск цепочки обработки резюме
result = app.send_task('tasks.workflows.resume_processing_chain')
print(f"Resume workflow ID: {result.id}")

# Запуск цепочки обработки вакансий
result = app.send_task('tasks.workflows.job_processing_chain')
print(f"Job workflow ID: {result.id}")
```

## 🔧 Управление

### Запуск/остановка
```bash
# Запуск всех воркеров
./start_celery.sh start

# Остановка всех воркеров
./start_celery.sh stop

# Перезапуск
./start_celery.sh restart
```

### Проверка состояния
```bash
# Статус воркеров
./start_celery.sh status

# Проверка Redis
redis-cli ping

# Проверка PostgreSQL
psql $DATABASE_URL -c "SELECT 1;"
```

## 📋 Требования

### Системные требования
- Python 3.13+
- Redis 6.0+
- PostgreSQL 12+
- GPU (опционально, для ускорения эмбеддингов)

### Основные зависимости
- celery
- redis
- psycopg2-binary
- sqlalchemy
- sentence-transformers
- torch

## 🚫 Отключенные компоненты

Для упрощения деплоймента временно отключены:
- CRUD операции кандидатов и компаний
- Аналитические задачи
- Уведомления
- Дополнительные интеграции
- Расширенный поиск

Эти компоненты можно будет включить в будущих версиях по мере необходимости.

## 📞 Поддержка

При возникновении проблем:
1. Проверьте логи: `./start_celery.sh logs`
2. Проверьте статус: `./start_celery.sh status`
3. Проверьте подключения к Redis и PostgreSQL
4. Убедитесь, что виртуальное окружение активировано
