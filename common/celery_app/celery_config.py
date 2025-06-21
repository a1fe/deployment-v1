"""
Конфигурация Celery для системы HR Analysis
"""

import os
import logging
from celery import Celery
from dotenv import load_dotenv
from functools import lru_cache

# Загружаем переменные окружения
load_dotenv()

# Импорт модулей конфигурации
from .logging_config import setup_celery_logging
from .redis_manager import get_redis_urls
from .celery_env_config import (
    get_environment_config, 
    get_task_routes, 
    get_beat_schedule
)

# Условный импорт серверных конфигураций
server_type = os.environ.get('SERVER_TYPE', 'cpu')  # 'cpu' или 'gpu'
gpu_enabled = bool(os.environ.get('GPU_INSTANCE_NAME'))

# Настройка логирования
logger = setup_celery_logging()

# Функция для ленивой инициализации Celery
@lru_cache(maxsize=1)
def _create_celery_app():
    """Создаёт и конфигурирует экземпляр Celery"""
    # Получаем URL Redis
    broker_url, result_backend = get_redis_urls()
    
    # Создаем экземпляр Celery
    app = Celery(
        'hr_analysis',
        broker=broker_url,
        backend=result_backend,
        include=[
            'tasks.workflows',
            'tasks.fillout_tasks',
            'tasks.embedding_tasks',
            'tasks.matching',  # Production-ready matching tasks
            'tasks.scoring_tasks',  # BGE Reranker scoring tasks
            'tasks.analysis_tasks'
        ]
    )
    
    # Принудительный импорт задач для их регистрации
    try:
        import tasks.workflows
        import tasks.fillout_tasks
        import tasks.embedding_tasks
        import tasks.matching
        import tasks.scoring_tasks
        import tasks.analysis_tasks
        logger.info("✅ All task modules imported successfully")
    except Exception as e:
        logger.error(f"❌ Error importing task modules: {e}")
    
    return app

# Создаём экземпляр Celery
celery_app = _create_celery_app()

# Получаем настройки для текущего окружения
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
env_config = get_environment_config(ENVIRONMENT)

# Базовая конфигурация Celery
def _configure_celery_app():
    """Конфигурирует экземпляр Celery с необходимыми настройками"""
    # Временная зона и сериализация
    base_config = {
        # Временная зона
        'timezone': 'UTC',
        'enable_utc': True,
        
        # Сериализация
        'task_serializer': 'json',
        'accept_content': ['json'],
        'result_serializer': 'json',
        
        # Настройки результатов
        'result_expires': 3600,  # Результаты хранятся 1 час
        'result_persistent': True,
        
        # Настройки задач
        'task_track_started': True,
        
        # Таймауты для задач (ИСПРАВЛЕНИЕ БАГА)
        'task_soft_time_limit': 300,  # 5 минут мягкий лимит
        'task_time_limit': 360,       # 6 минут жесткий лимит
        
        # Маршрутизация задач
        'task_routes': get_task_routes(),
        
        # Настройки beat (планировщик)
        'beat_schedule': get_beat_schedule(),
        
        # Мониторинг
        'worker_send_task_events': True,
        'task_send_sent_event': True,
        
        # Безопасность
        'worker_hijack_root_logger': False,
        'worker_log_color': False,
        
        # Настройки подключения к брокеру
        'broker_connection_retry_on_startup': True,
        'broker_connection_retry': True,
        'broker_connection_max_retries': 10,
        'broker_connection_timeout': 30,
        
        # Настройки отказоустойчивости
        'task_acks_late': True,        # Подтверждение после выполнения
        'task_reject_on_worker_lost': True,  # Возвращать задачи при падении воркера
        'task_default_rate_limit': '100/m',  # Лимит для предотвращения перегрузки
        
        # Настройки для автоматического повтора
        'task_default_retry_delay': 60,  # 1 минута между повторами
        'task_max_retries': 3,       # Максимальное число повторов
    }
    
    # Применяем базовые настройки
    celery_app.conf.update(base_config)
    
    # Применяем настройки окружения поверх базовых
    celery_app.conf.update(env_config)
    
    # Регистрируем сигналы Celery для мониторинга и отказоустойчивости
    from .celery_signals import register_signals
    register_signals(celery_app)
    
    return celery_app

# Применяем конфигурацию
_configure_celery_app()

# Функция для получения логгера задач
def get_task_logger():
    """Возвращает логгер для задач Celery"""
    from celery.utils.log import get_task_logger
    return get_task_logger(__name__)

# Функция для получения экземпляра Celery
def get_celery_app():
    """Возвращает сконфигурированный экземпляр Celery"""
    return celery_app


# Проверка работоспособности всех компонентов
def check_system_health():
    """Проверяет работоспособность всей системы"""
    from .health_checks import SystemHealthChecker
    checker = SystemHealthChecker()
    return checker.get_health_status()


if __name__ == "__main__":
    from .health_checks import SystemHealthChecker
    from .redis_manager import redis_manager
    
    print("🔍 Конфигурация Celery для HR Analysis")
    print("=" * 50)
    print(f"Broker URL: {redis_manager.get_connection_string()}")
    print(f"Environment: {ENVIRONMENT}")
    print(f"Настройки окружения:")
    for key, value in env_config.items():
        print(f"  • {key}: {value}")
    print(f"Включенные модули задач:")
    for module in celery_app.conf.include:
        print(f"  • {module}")
    
    print("\n" + "=" * 50)
    # Запускаем полную проверку системы
    health_checker = SystemHealthChecker()
    health_checker.print_health_status()
