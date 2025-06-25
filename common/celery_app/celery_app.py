"""
Celery application for HR Analysis system

This module provides access to the configured Celery application instance.
"""

import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Создаем Celery app здесь, чтобы избежать циклических импортов
from celery import Celery
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get Redis configuration
redis_host = os.getenv('REDIS_HOST', 'localhost')
redis_port = os.getenv('REDIS_PORT', '6379')  
redis_db = os.getenv('REDIS_DB', '0')
redis_password = os.getenv('REDIS_PASSWORD', '')

# Build Redis URL
if redis_password:
    redis_url = f"redis://:{redis_password}@{redis_host}:{redis_port}/{redis_db}"
else:
    redis_url = f"redis://{redis_host}:{redis_port}/{redis_db}"

# Create Celery instance
celery_app = Celery('hr_system')

# Configure Celery
celery_app.conf.update(
    broker_url=redis_url,
    result_backend=redis_url,
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Europe/Moscow',
    enable_utc=True,
    broker_connection_retry_on_startup=True,
    task_track_started=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_disable_rate_limits=True,
)

# Import динамической конфигурации
try:
    from .celery_env_config import get_task_routes, get_beat_schedule
except ImportError:
    # Fallback для запуска как скрипта
    from celery_env_config import get_task_routes, get_beat_schedule

# Применяем дополнительную конфигурацию
celery_app.conf.update(
    # Beat schedule - автоматический пайплайн  
    beat_schedule=get_beat_schedule(),
    
    # Task routing - техническая архитектура очередей
    task_routes=get_task_routes(),
    
    # Include task modules (используем абсолютные пути)
    include=[
        'common.tasks.fillout_tasks',
        'common.tasks.parsing_tasks', 
        'common.tasks.embedding_tasks',
        'common.tasks.reranking_tasks',
        'common.tasks.workflows'
    ]
)

# Export the app for compatibility
app = celery_app

def get_celery_app():
    """Get the configured Celery application instance"""
    return celery_app

# Import tasks to register them (after app is defined to avoid circular imports)
try:
    # Используем абсолютные импорты для избежания ошибок
    import sys
    import os
    
    # Добавляем путь к корню проекта в sys.path если его нет
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    
    # Импортируем модули задач с абсолютными путями
    from common.tasks import fillout_tasks, parsing_tasks, embedding_tasks, reranking_tasks, workflows
    logger.info("✅ All task modules imported successfully")
except Exception as e:
    logger.error(f"❌ Error importing task modules: {e}")
    logger.error(f"Current sys.path: {sys.path[:3]}...")  # Показываем первые 3 пути для отладки

# Export the app instance for Celery CLI
# app is already imported above


def run_test_tasks():
    """Run test tasks to verify system functionality"""
    try:
        # Import our tasks with absolute path
        from common.tasks.fillout_tasks import fetch_resume_data
        
        logger.info("Testing task discovery...")
        
        # Test task registration using the celery_app variable
        required_tasks = ['common.tasks.fillout_tasks.fetch_resume_data']
        missing_tasks = []
        
        for task_name in required_tasks:
            if task_name in celery_app.tasks:
                logger.info(f"✅ {task_name} task discovered")
            else:
                logger.error(f"❌ {task_name} task not found")
                missing_tasks.append(task_name)
        
        if missing_tasks:
            raise RuntimeError(f"❌ Задачи не зарегистрированы в Celery: {missing_tasks}")
            
        # Run a simple test task only if all tasks are registered
        logger.info("🧪 Testing fetch_resume_data task...")
        result = fetch_resume_data.delay()
        logger.info(f"✅ Task submitted with ID: {result.id}")
        
        return "Test tasks completed successfully"
        
    except Exception as e:
        logger.error(f"❌ Error running test tasks: {e}")
        return f"Test failed: {e}"


if __name__ == '__main__':
    print("Celery app configured successfully")
    print(f"Broker: {celery_app.conf.broker_url}")
    print(f"Tasks: {list(celery_app.tasks.keys())}")
