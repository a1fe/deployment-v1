# Руководство по развертыванию CPU/GPU архитектуры

## Обзор архитектуры

Система разделена на два типа серверов с централизованной организацией кода:

### Структура деплоймента
```
deployment/
├── common/              # Общие компоненты для всех серверов
│   ├── architecture_config.py
│   ├── main.py
│   ├── start_celery.sh
│   ├── requirements.txt
│   ├── celery_app/
│   ├── database/
│   ├── models/
│   ├── tasks/
│   ├── utils/
│   └── scripts/
├── cpu-server/          # CPU-специфичные компоненты
│   ├── cpu_server_config.py
│   ├── start_cpu_celery.sh
│   └── start_cpu_server.sh
├── gpu-server/          # GPU-специфичные компоненты
│   ├── gpu_server_config.py
│   ├── start_gpu_celery.sh
│   └── startup_gpu_server.sh
├── start_cpu_deployment.sh    # Точка входа для CPU
└── start_gpu_deployment.sh    # Точка входа для GPU
```

### CPU-сервер (основной, e2-standard-2)
- **Роль**: Основной сервер, работает постоянно
- **Компоненты**: 
  - API сервер (FastAPI)
  - Celery Beat (планировщик задач)
  - Redis (брокер сообщений)
  - PostgreSQL (база данных)
  - CPU воркеры Celery
- **Задачи**: 
  - Основные workflow
  - Fillout API интеграция
  - Базовый поиск
  - Координация задач

### GPU-сервер (n1-standard-4 + T4)
- **Роль**: Вычислительный сервер, включается по требованию
- **Компоненты**:
  - GPU воркеры Celery
  - PyTorch + CUDA
  - Sentence Transformers
- **Задачи**:
  - Генерация эмбеддингов
  - Реранкинг (BGE-M3)
  - LLM анализ

## Переменные окружения

### Общие для обоих серверов
```bash
# База данных (на CPU-сервере)
DATABASE_URL=postgresql://user:password@cpu-server:5432/hr_analysis

# Redis (на CPU-сервере) 
REDIS_URL=redis://cpu-server:6379/0

# Окружение
ENVIRONMENT=production

# Google Cloud
GOOGLE_CLOUD_PROJECT=your-project-id
GOOGLE_CLOUD_ZONE=us-central1-a
```

### Только для CPU-сервера
```bash
# GPU инстанс (для управления)
GPU_INSTANCE_NAME=gpu-server-instance

# Celery Beat
CELERY_BEAT_ENABLED=true
```

### Только для GPU-сервера
```bash
# Идентификация как GPU сервер
GPU_INSTANCE_NAME=gpu-server-instance

# CUDA
CUDA_VISIBLE_DEVICES=0
```

## Развертывание

### 1. CPU-сервер (основной)

#### Установка зависимостей
```bash
# Обновление системы
sudo apt update && sudo apt upgrade -y

# Python и pip
sudo apt install python3 python3-pip python3-venv -y

# Redis
sudo apt install redis-server -y
sudo systemctl enable redis-server
sudo systemctl start redis-server

# PostgreSQL
sudo apt install postgresql postgresql-contrib -y
sudo systemctl enable postgresql
sudo systemctl start postgresql
```

#### Настройка приложения
```bash
# Клонирование репозитория
git clone <repository-url>
cd hr-analysis

# Создание виртуального окружения
python3 -m venv venv
source venv/bin/activate

# Установка зависимостей
pip install -r deployment/common/requirements.txt

# Установка Google Cloud библиотек для production (опционально)
pip install google-cloud-secret-manager google-cloud-logging

# Настройка переменных окружения
cp deployment/common/.env.example .env
# Редактируем .env (БЕЗ GPU_INSTANCE_NAME)
```

#### Запуск воркеров
```bash
# Запуск CPU воркеров через единую точку входа
./deployment/start_cpu_deployment.sh
```

#### Systemd сервис для CPU воркеров
```bash
# Создаем сервис
sudo tee /etc/systemd/system/hr-celery-cpu.service > /dev/null <<EOF
[Unit]
Description=HR Analysis CPU Celery Workers
After=network.target redis.service postgresql.service

[Service]
Type=forking
User=hr-user
Group=hr-user
WorkingDirectory=/home/hr-user/hr-analysis
Environment=PATH=/home/hr-user/hr-analysis/venv/bin
EnvironmentFile=/home/hr-user/hr-analysis/.env
ExecStart=/home/hr-user/hr-analysis/deployment/start_cpu_deployment.sh
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Включаем и запускаем
sudo systemctl daemon-reload
sudo systemctl enable hr-celery-cpu
sudo systemctl start hr-celery-cpu
```

### 2. GPU-сервер

#### Установка CUDA и драйверов
```bash
# Обновление системы
sudo apt update && sudo apt upgrade -y

# NVIDIA драйверы
sudo apt install nvidia-driver-470 -y

# CUDA Toolkit
wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/cuda-ubuntu2004.pin
sudo mv cuda-ubuntu2004.pin /etc/apt/preferences.d/cuda-repository-pin-600
wget https://developer.download.nvidia.com/compute/cuda/11.8/local_installers/cuda-repo-ubuntu2004-11-8-local_11.8.0-520.61.05-1_amd64.deb
sudo dpkg -i cuda-repo-ubuntu2004-11-8-local_11.8.0-520.61.05-1_amd64.deb
sudo cp /var/cuda-repo-ubuntu2004-11-8-local/cuda-*-keyring.gpg /usr/share/keyrings/
sudo apt update
sudo apt install cuda -y

# Перезагрузка
sudo reboot
```

#### Настройка приложения
```bash
# Python и зависимости
sudo apt install python3 python3-pip python3-venv -y

# Клонирование репозитория
git clone <repository-url>
cd hr-analysis

# Виртуальное окружение
python3 -m venv venv
source venv/bin/activate

# Установка зависимостей с GPU поддержкой
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
pip install -r deployment/common/requirements.txt

# Настройка переменных окружения
cp deployment/common/.env.example .env
# Редактируем .env (ОБЯЗАТЕЛЬНО устанавливаем GPU_INSTANCE_NAME)
```

#### Запуск GPU воркеров
```bash
# Запуск GPU воркеров через единую точку входа
./deployment/start_gpu_deployment.sh
```

#### Systemd сервис для GPU воркеров
```bash
# Создаем сервис
sudo tee /etc/systemd/system/hr-celery-gpu.service > /dev/null <<EOF
[Unit]
Description=HR Analysis GPU Celery Workers
After=network.target

[Service]
Type=forking
User=hr-user
Group=hr-user
WorkingDirectory=/home/hr-user/hr-analysis
Environment=PATH=/home/hr-user/hr-analysis/venv/bin
Environment=CUDA_VISIBLE_DEVICES=0
EnvironmentFile=/home/hr-user/hr-analysis/.env
ExecStart=/home/hr-user/hr-analysis/deployment/start_gpu_deployment.sh
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Включаем и запускаем
sudo systemctl daemon-reload
sudo systemctl enable hr-celery-gpu
sudo systemctl start hr-celery-gpu
```

## Автоматическое управление GPU инстансом

### Настройка мониторинга на CPU-сервере
```bash
# Установка Google Cloud SDK
curl https://sdk.cloud.google.com | bash
exec -l $SHELL
gcloud init

# Аутентификация для сервисного аккаунта
gcloud auth activate-service-account --key-file=path/to/service-account.json

# Запуск мониторинга
python deployment/common/utils/gpu_monitor.py
```

### Systemd сервис для мониторинга
```bash
sudo tee /etc/systemd/system/hr-gpu-monitor.service > /dev/null <<EOF
[Unit]
Description=HR Analysis GPU Monitor
After=network.target redis.service

[Service]
Type=simple
User=hr-user
Group=hr-user
WorkingDirectory=/home/hr-user/hr-analysis
Environment=PATH=/home/hr-user/hr-analysis/venv/bin
EnvironmentFile=/home/hr-user/hr-analysis/.env
ExecStart=/home/hr-user/hr-analysis/venv/bin/python deployment/common/utils/gpu_monitor.py
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable hr-gpu-monitor
sudo systemctl start hr-gpu-monitor
```

## Мониторинг и логирование

### Настройка централизованного логирования

#### Переменные окружения для логирования
```bash
# Логирование
LOG_LEVEL=INFO
LOG_DIR=/var/log/hr-analysis
ENABLE_CLOUD_LOGGING=true  # Для production
ENABLE_FILE_LOGGING=true
ENABLE_CONSOLE_LOGGING=true

# Email алерты
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=alerts@yourcompany.com
SMTP_PASSWORD=app_password
ALERT_RECIPIENTS=admin@yourcompany.com,devops@yourcompany.com
ALERT_FROM_EMAIL=hr-analysis@yourcompany.com
```

#### Создание директории для логов
```bash
sudo mkdir -p /var/log/hr-analysis
sudo chown hr-user:hr-user /var/log/hr-analysis
sudo chmod 755 /var/log/hr-analysis
```

### Мониторинг воркеров

#### Systemd сервис для мониторинга
```bash
sudo tee /etc/systemd/system/hr-worker-monitor.service > /dev/null <<EOF
[Unit]
Description=HR Analysis Worker Monitor
After=network.target redis.service

[Service]
Type=simple
User=hr-user
Group=hr-user
WorkingDirectory=/home/hr-user/hr-analysis
Environment=PATH=/home/hr-user/hr-analysis/venv/bin
EnvironmentFile=/home/hr-user/hr-analysis/.env
ExecStart=/home/hr-user/hr-analysis/venv/bin/python deployment/common/utils/worker_monitor.py
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable hr-worker-monitor
sudo systemctl start hr-worker-monitor
```

#### Проверка алертов
```bash
# Просмотр логов мониторинга
sudo journalctl -u hr-worker-monitor -f

# Проверка статуса воркеров
python -c "
from deployment.common.utils.worker_monitor import WorkerHealthMonitor
import asyncio

async def check():
    monitor = WorkerHealthMonitor()
    summary = monitor.get_health_summary()
    print(summary)

asyncio.run(check())
"
```

### Prometheus и Grafana (Опционально)

#### Установка Prometheus
```bash
# Создание пользователя
sudo useradd --no-create-home --shell /bin/false prometheus

# Скачивание и установка
wget https://github.com/prometheus/prometheus/releases/download/v2.45.0/prometheus-2.45.0.linux-amd64.tar.gz
tar xvfz prometheus-2.45.0.linux-amd64.tar.gz
sudo cp prometheus-2.45.0.linux-amd64/prometheus /usr/local/bin/
sudo cp prometheus-2.45.0.linux-amd64/promtool /usr/local/bin/
sudo chown prometheus:prometheus /usr/local/bin/prometheus
sudo chown prometheus:prometheus /usr/local/bin/promtool

# Создание конфигурации
sudo mkdir /etc/prometheus
sudo mkdir /var/lib/prometheus
sudo chown prometheus:prometheus /etc/prometheus
sudo chown prometheus:prometheus /var/lib/prometheus
```

#### Конфигурация Prometheus
```yaml
# /etc/prometheus/prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'hr-analysis'
    static_configs:
      - targets: ['localhost:5555']  # Flower
    scrape_interval: 30s

  - job_name: 'redis'
    static_configs:
      - targets: ['localhost:6379']
    scrape_interval: 30s
```

## Тестирование и валидация деплоя

### Скрипт проверки готовности системы

Создайте файл `/home/hr-user/hr-analysis/deployment/common/scripts/health_check.py`:
```python
#!/usr/bin/env python3
"""
Скрипт проверки готовности системы после деплоя
"""

import sys
import asyncio
import time
from typing import Dict, Any, List

# Добавляем путь к проекту
sys.path.append('/home/hr-user/hr-analysis')

from deployment.common.utils.embedding_quality_test import check_embedding_quality
from deployment.common.utils.worker_monitor import WorkerHealthMonitor
from deployment.common.utils.secret_manager import secret_manager

async def run_health_checks() -> Dict[str, Any]:
    """Выполнение всех проверок здоровья системы"""
    results = {
        'timestamp': time.time(),
        'overall_status': 'unknown',
        'checks': {}
    }
    
    checks = [
        ('secrets', check_secrets),
        ('database', check_database),
        ('redis', check_redis),
        ('workers', check_workers),
        ('gpu_quality', check_gpu_quality),
        ('queues', check_queues),
    ]
    
    passed = 0
    total = len(checks)
    
    for check_name, check_func in checks:
        try:
            print(f"🔍 Проверка {check_name}...")
            result = await check_func()
            results['checks'][check_name] = result
            
            if result.get('status') == 'ok':
                print(f"✅ {check_name}: OK")
                passed += 1
            else:
                print(f"❌ {check_name}: {result.get('message', 'FAILED')}")
                
        except Exception as e:
            print(f"❌ {check_name}: ERROR - {str(e)}")
            results['checks'][check_name] = {
                'status': 'error',
                'message': str(e)
            }
    
    # Общий статус
    if passed == total:
        results['overall_status'] = 'healthy'
        print(f"\n✅ Все проверки пройдены ({passed}/{total})")
    elif passed >= total * 0.8:
        results['overall_status'] = 'warning'
        print(f"\n⚠️ Большинство проверок пройдено ({passed}/{total})")
    else:
        results['overall_status'] = 'critical'
        print(f"\n❌ Критические проблемы ({passed}/{total})")
    
    return results

async def check_secrets() -> Dict[str, Any]:
    """Проверка доступности секретов"""
    try:
        if not secret_manager.validate_required_secrets():
            return {'status': 'error', 'message': 'Обязательные секреты недоступны'}
        
        return {'status': 'ok', 'message': 'Секреты доступны'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

async def check_database() -> Dict[str, Any]:
    """Проверка подключения к базе данных"""
    try:
        from database.config import Database
        db = Database()
        
        with db.engine.connect() as conn:
            result = conn.execute("SELECT 1").fetchone()
            if result and result[0] == 1:
                return {'status': 'ok', 'message': 'База данных доступна'}
        
        return {'status': 'error', 'message': 'Не удалось выполнить тестовый запрос'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

async def check_redis() -> Dict[str, Any]:
    """Проверка подключения к Redis"""
    try:
        import redis
        from deployment.utils.secret_manager import get_secret
        
        redis_url = get_secret('REDIS_URL', 'redis://localhost:6379/0')
        r = redis.from_url(redis_url)
        
        # Тестовая операция
        r.ping()
        r.set('health_check', 'ok', ex=10)
        value = r.get('health_check')
        
        if value == b'ok':
            return {'status': 'ok', 'message': 'Redis доступен'}
        else:
            return {'status': 'error', 'message': 'Тестовая операция Redis не удалась'}
            
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

async def check_workers() -> Dict[str, Any]:
    """Проверка состояния Celery воркеров"""
    try:
        monitor = WorkerHealthMonitor()
        workers = await monitor.check_workers_health()
        
        if not workers:
            return {'status': 'error', 'message': 'Воркеры не найдены'}
        
        alive_workers = [w for w in workers if w.is_alive]
        
        if len(alive_workers) == len(workers):
            return {
                'status': 'ok', 
                'message': f'Все воркеры активны ({len(workers)})',
                'workers_count': len(workers)
            }
        else:
            return {
                'status': 'warning',
                'message': f'Активно {len(alive_workers)}/{len(workers)} воркеров',
                'workers_count': len(workers),
                'alive_count': len(alive_workers)
            }
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

async def check_gpu_quality() -> Dict[str, Any]:
    """Проверка качества GPU эмбеддингов (если GPU доступен)"""
    try:
        import os
        if not os.getenv('GPU_INSTANCE_NAME'):
            return {'status': 'skip', 'message': 'GPU не настроен'}
        
        result = check_embedding_quality(timeout=120)
        
        if result['success']:
            quality_score = result['metrics'].get('quality_score', 0)
            if quality_score > 0.7:
                return {'status': 'ok', 'message': f'Качество GPU эмбеддингов хорошее ({quality_score:.2f})'}
            else:
                return {'status': 'warning', 'message': f'Качество GPU эмбеддингов среднее ({quality_score:.2f})'}
        else:
            return {'status': 'error', 'message': f'Ошибка тестирования GPU: {result["error"]}'}
            
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

async def check_queues() -> Dict[str, Any]:
    """Проверка состояния очередей"""
    try:
        monitor = WorkerHealthMonitor()
        queues = await monitor.check_queues_health()
        
        if not queues:
            return {'status': 'warning', 'message': 'Очереди не найдены'}
        
        overloaded = [q for q in queues if q.pending_tasks > 100]
        no_workers = [q for q in queues if q.workers_count == 0]
        
        if overloaded:
            return {
                'status': 'warning',
                'message': f'Перегружены очереди: {[q.name for q in overloaded]}',
                'total_queues': len(queues)
            }
        elif no_workers:
            return {
                'status': 'warning',
                'message': f'Нет воркеров для очередей: {[q.name for q in no_workers]}',
                'total_queues': len(queues)
            }
        else:
            return {
                'status': 'ok',
                'message': f'Все очереди в порядке ({len(queues)})',
                'total_queues': len(queues)
            }
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

if __name__ == "__main__":
    print("🚀 Запуск проверки готовности системы...")
    results = asyncio.run(run_health_checks())
    
    # Вывод результатов
    print(f"\n📊 Статус системы: {results['overall_status'].upper()}")
    
    # Завершаем с соответствующим кодом
    if results['overall_status'] == 'healthy':
        sys.exit(0)
    elif results['overall_status'] == 'warning':
        sys.exit(1)
    else:
        sys.exit(2)
```

### Выполнение проверки готовности
```bash
# Делаем скрипт исполняемым
chmod +x deployment/common/scripts/health_check.py

# Запуск проверки
python deployment/common/scripts/health_check.py

# Проверка статуса
echo "Exit code: $?"
```

### Автоматическая проверка после деплоя
```bash
# Добавить в конец скрипта деплоя
echo "🔍 Проверка готовности системы..."
if python deployment/common/scripts/health_check.py; then
    echo "✅ Система готова к работе"
else
    echo "❌ Обнаружены проблемы, проверьте логи"
    exit 1
fi
```

## Процедуры отката (Rollback)

### Подготовка к откату

#### Создание точки восстановления
```bash
# Перед деплоем создаем backup
./deployment/common/scripts/create_backup.sh

# Сохраняем текущую версию кода
git tag -a "pre-deploy-$(date +%Y%m%d-%H%M%S)" -m "Pre-deployment snapshot"
git push origin --tags
```

#### Скрипт автоматического отката
Создайте файл `/home/hr-user/hr-analysis/deployment/scripts/rollback.sh`:
```bash
#!/bin/bash
"""
Скрипт автоматического отката системы
"""

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] $1${NC}"
}

error() {
    echo -e "${RED}[ERROR] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[WARNING] $1${NC}"
}

# Параметры
BACKUP_DIR="/var/backups/hr-analysis"
SERVICE_USER="hr-user"
APP_DIR="/home/${SERVICE_USER}/hr-analysis"

rollback_code() {
    local target_commit="$1"
    
    log "Откат кода к коммиту: $target_commit"
    
    cd "$APP_DIR"
    
    # Остановка сервисов
    sudo systemctl stop hr-celery-cpu || true
    sudo systemctl stop hr-celery-gpu || true
    sudo systemctl stop hr-worker-monitor || true
    sudo systemctl stop hr-gpu-monitor || true
    
    # Откат к предыдущей версии
    git fetch
    git checkout "$target_commit"
    git submodule update --init --recursive
    
    # Откат виртуального окружения
    source venv/bin/activate
    pip install -r requirements.txt
    
    log "Код откачен к версии: $target_commit"
}

rollback_database() {
    local backup_file="$1"
    
    if [ ! -f "$backup_file" ]; then
        error "Файл backup базы данных не найден: $backup_file"
        return 1
    fi
    
    log "Откат базы данных из backup: $backup_file"
    
    # Восстановление из backup
    source "$APP_DIR/.env"
    
    # Извлекаем параметры подключения
    DB_HOST=$(echo "$DATABASE_URL" | sed -E 's|postgresql://[^@]*@([^:/]+).*|\1|')
    DB_PORT=$(echo "$DATABASE_URL" | sed -E 's|postgresql://[^@]*@[^:]+:([0-9]+).*|\1|')
    DB_NAME=$(echo "$DATABASE_URL" | sed -E 's|postgresql://[^@]*@[^/]*/([^?]*).*|\1|')
    DB_USER=$(echo "$DATABASE_URL" | sed -E 's|postgresql://([^:@]*).*|\1|')
    
    # Создаем backup текущей БД
    PGPASSWORD="$DB_PASSWORD" pg_dump \
        -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" \
        "$DB_NAME" > "${BACKUP_DIR}/pre_rollback_$(date +%Y%m%d_%H%M%S).sql"
    
    # Восстанавливаем из backup
    PGPASSWORD="$DB_PASSWORD" psql \
        -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" \
        -d "$DB_NAME" < "$backup_file"
    
    log "База данных восстановлена из backup"
}

rollback_configuration() {
    local config_backup="$1"
    
    if [ ! -f "$config_backup" ]; then
        error "Backup конфигурации не найден: $config_backup"
        return 1
    fi
    
    log "Откат конфигурации из backup: $config_backup"
    
    # Восстанавливаем .env файл
    cp "$config_backup" "$APP_DIR/.env"
    
    log "Конфигурация восстановлена"
}

restart_services() {
    log "Запуск сервисов после отката..."
    
    # Перезапуск основных сервисов
    sudo systemctl restart redis-server
    sudo systemctl restart postgresql
    
    # Запуск Celery сервисов
    sudo systemctl start hr-celery-cpu
    
    # Запуск GPU сервисов (если настроены)
    if grep -q "GPU_INSTANCE_NAME" "$APP_DIR/.env"; then
        sudo systemctl start hr-celery-gpu
        sudo systemctl start hr-gpu-monitor
    fi
    
    # Запуск мониторинга
    sudo systemctl start hr-worker-monitor
    
    sleep 10
    
    # Проверка статуса
    sudo systemctl is-active hr-celery-cpu
    sudo systemctl is-active hr-worker-monitor
    
    log "Сервисы перезапущены"
}

verify_rollback() {
    log "Проверка корректности отката..."
    
    cd "$APP_DIR"
    
    # Запуск проверки готовности
    if python deployment/scripts/health_check.py; then
        log "✅ Откат выполнен успешно, система работает"
        return 0
    else
        error "❌ После отката обнаружены проблемы"
        return 1
    fi
}

show_usage() {
    echo "Использование: $0 [OPTIONS]"
    echo ""
    echo "Опции:"
    echo "  --commit HASH         Откат к указанному коммиту Git"
    echo "  --database FILE       Откат базы данных из backup файла"
    echo "  --config FILE         Откат конфигурации из backup файла"
    echo "  --full TIMESTAMP      Полный откат к указанной временной метке"
    echo "  --verify-only         Только проверка без отката"
    echo "  --help                Показать справку"
    echo ""
    echo "Примеры:"
    echo "  $0 --commit HEAD~1                           # Откат к предыдущему коммиту"
    echo "  $0 --database /var/backups/hr-analysis/db_20231215_120000.sql"
    echo "  $0 --full 20231215_120000                    # Полный откат"
}

main() {
    local commit=""
    local database_backup=""
    local config_backup=""
    local full_rollback=""
    local verify_only=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --commit)
                commit="$2"
                shift 2
                ;;
            --database)
                database_backup="$2"
                shift 2
                ;;
            --config)
                config_backup="$2"
                shift 2
                ;;
            --full)
                full_rollback="$2"
                shift 2
                ;;
            --verify-only)
                verify_only=true
                shift
                ;;
            --help)
                show_usage
                exit 0
                ;;
            *)
                error "Неизвестная опция: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    if [ "$verify_only" = true ]; then
        verify_rollback
        exit $?
    fi
    
    if [ -n "$full_rollback" ]; then
        # Полный откат
        commit=$(git log --format="%H" --until="$full_rollback" -1)
        database_backup="${BACKUP_DIR}/db_${full_rollback}.sql"
        config_backup="${BACKUP_DIR}/config_${full_rollback}.env"
    fi
    
    if [ -z "$commit" ] && [ -z "$database_backup" ] && [ -z "$config_backup" ]; then
        error "Не указаны параметры отката"
        show_usage
        exit 1
    fi
    
    log "🔄 Начинаем процедуру отката..."
    
    # Создаем backup текущего состояния
    log "Создание backup текущего состояния..."
    ./deployment/scripts/create_backup.sh "pre_rollback_$(date +%Y%m%d_%H%M%S)"
    
    # Выполняем откат компонентов
    if [ -n "$commit" ]; then
        rollback_code "$commit"
    fi
    
    if [ -n "$database_backup" ]; then
        rollback_database "$database_backup"
    fi
    
    if [ -n "$config_backup" ]; then
        rollback_configuration "$config_backup"
    fi
    
    # Перезапуск сервисов
    restart_services
    
    # Проверка результата
    if verify_rollback; then
        log "✅ Откат завершен успешно"
        exit 0
    else
        error "❌ Откат завершен с ошибками"
        exit 1
    fi
}

main "$@"
```

### Процедура экстренного отката

#### В случае критических проблем
```bash
# 1. Немедленная остановка проблемных сервисов
sudo systemctl stop hr-celery-cpu hr-celery-gpu hr-worker-monitor

# 2. Откат к последней рабочей версии
cd /home/hr-user/hr-analysis
git checkout $(git describe --tags --abbrev=0)

# 3. Восстановление зависимостей
source venv/bin/activate
pip install -r requirements.txt

# 4. Восстановление базы данных (если необходимо)
latest_backup=$(ls -t /var/backups/hr-analysis/db_*.sql | head -1)
./deployment/scripts/rollback.sh --database "$latest_backup"

# 5. Перезапуск сервисов
sudo systemctl start hr-celery-cpu
sudo systemctl start hr-worker-monitor

# 6. Проверка системы
python deployment/scripts/health_check.py
```

#### Быстрый откат через одну команду
```bash
# Откат к предыдущей версии
./deployment/scripts/rollback.sh --commit HEAD~1

# Полный откат к временной метке
./deployment/scripts/rollback.sh --full 20231215_120000

# Только проверка без отката
./deployment/scripts/rollback.sh --verify-only
```

### Мониторинг после отката
```bash
# Проверка логов после отката
sudo journalctl -u hr-celery-cpu -f --since "5 minutes ago"
sudo journalctl -u hr-worker-monitor -f --since "5 minutes ago"

# Проверка производительности
top -p $(pgrep -f "celery.*worker")

# Проверка очередей
celery -A celery_app.celery_app inspect active
```
