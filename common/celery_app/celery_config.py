"""
Конфигурация Celery для системы HR Analysis

ПРИМЕЧАНИЕ: Основная конфигурация Celery находится в celery_app.py.
Этот файл предоставляет дополнительные утилиты и проверки.
"""

import os
import logging
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Импортируем основной экземпляр Celery
try:
    from .celery_app import celery_app, get_celery_app
except ImportError:
    # Fallback для запуска как скрипта
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from celery_app import celery_app, get_celery_app

# Настройка логирования
try:
    from .logging_config import setup_celery_logging
except ImportError:
    # Fallback
    from logging_config import setup_celery_logging
logger = setup_celery_logging()

# Получаем настройки для текущего окружения
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')

# Функция для получения логгера задач
def get_task_logger():
    """Возвращает логгер для задач Celery"""
    from celery.utils.log import get_task_logger
    return get_task_logger(__name__)

# Проверка работоспособности всех компонентов
def check_system_health():
    """Проверяет работоспособность всей системы"""
    try:
        from .health_checks import SystemHealthChecker
    except ImportError:
        from health_checks import SystemHealthChecker
    checker = SystemHealthChecker()
    return checker.get_health_status()

def get_registered_tasks():
    """Возвращает список зарегистрированных задач"""
    app = get_celery_app()
    return list(app.tasks.keys())

def verify_task_modules():
    """Проверяет, что все модули задач корректно импортированы"""
    app = get_celery_app()
    expected_tasks = [
        'tasks.fillout_tasks.fetch_resume_data',
        'tasks.fillout_tasks.fetch_company_data',
        'tasks.parsing_tasks.parse_resume_text',
        'tasks.parsing_tasks.parse_job_text',
        'tasks.embedding_tasks.generate_resume_embeddings',
        'tasks.embedding_tasks.generate_job_embeddings',
        'tasks.reranking_tasks.rerank_resumes_for_job',
        'tasks.reranking_tasks.rerank_jobs_for_resume',
        'tasks.workflows.run_full_processing_pipeline'
    ]
    
    registered_tasks = get_registered_tasks()
    missing_tasks = []
    available_tasks = []
    
    for task_name in expected_tasks:
        if task_name in registered_tasks:
            available_tasks.append(task_name)
        else:
            missing_tasks.append(task_name)
    
    return {
        'available_tasks': available_tasks,
        'missing_tasks': missing_tasks,
        'total_registered': len(registered_tasks)
    }


if __name__ == "__main__":
    try:
        from .health_checks import SystemHealthChecker
        from .redis_manager import redis_manager
    except ImportError:
        from health_checks import SystemHealthChecker  
        from redis_manager import redis_manager
    
    print("🔍 Конфигурация Celery для HR Analysis")
    print("=" * 50)
    print(f"Broker URL: {redis_manager.get_connection_string()}")
    print(f"Environment: {ENVIRONMENT}")
    
    # Показываем информацию о задачах
    task_info = verify_task_modules()
    print(f"\nСтатус задач:")
    print(f"  • Всего зарегистрированных задач: {task_info['total_registered']}")
    print(f"  • Доступных задач: {len(task_info['available_tasks'])}")
    print(f"  • Отсутствующих задач: {len(task_info['missing_tasks'])}")
    
    if task_info['available_tasks']:
        print(f"\n✅ Доступные задачи:")
        for task in task_info['available_tasks']:
            print(f"  • {task}")
    
    if task_info['missing_tasks']:
        print(f"\n❌ Отсутствующие задачи:")
        for task in task_info['missing_tasks']:
            print(f"  • {task}")
    
    print("\n" + "=" * 50)
    # Запускаем полную проверку системы
    health_checker = SystemHealthChecker()
    health_checker.print_health_status()
