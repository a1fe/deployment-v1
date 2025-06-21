"""
Celery application for HR Analysis system

This module provides access to the configured Celery application instance.
"""

import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from celery import Celery

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Lazy import to avoid circular dependencies
_celery_app = None


def get_celery_app():
    """Get the configured Celery application instance"""
    global _celery_app
    if _celery_app is None:
        # Validate Redis security at startup
        try:
            from celery_app.redis_manager import validate_redis_security
            validate_redis_security()
            logger.info("✅ Redis security validation passed")
        except Exception as e:
            logger.error(f"❌ Redis security validation failed: {e}")
            raise SystemExit(f"Security validation failed: {e}")
        
        from celery_app.celery_config import celery_app as _imported_app
        _celery_app = _imported_app
        
        # Import signals to register them
        import celery_app.celery_signals
        
        # Start monitoring if enabled via env var
        if os.getenv('CELERY_ENABLE_MONITORING', '').lower() in ('true', '1', 'yes'):
            try:
                from celery_app.monitoring import monitor
                monitor.start_monitoring()
                logger.info("Automatic Celery monitoring enabled")
            except Exception as e:
                logger.error(f"Failed to start monitoring: {e}")
        
    return _celery_app


# Export the app instance getter
app = get_celery_app


def run_test_tasks():
    """Run test tasks to verify system functionality"""
    try:
        # Import our tasks
        from tasks.fillout_tasks import pull_fillout_data, parse_documents
        
        logger.info("Testing task discovery...")
        
        # Test task registration using the app variable (which is celery_app)
        required_tasks = ['fillout.pull_fillout_data', 'documents.parse_documents']
        missing_tasks = []
        
        for task_name in required_tasks:
            if task_name in app.tasks:
                logger.info(f"✅ {task_name} task discovered")
            else:
                logger.error(f"❌ {task_name} task not found")
                missing_tasks.append(task_name)
        
        if missing_tasks:
            raise RuntimeError(f"❌ Задачи не зарегистрированы в Celery: {missing_tasks}")
            
        # Run a simple test task only if all tasks are registered
        logger.info("🧪 Testing pull_fillout_data task...")
        result = pull_fillout_data.delay()
        logger.info(f"✅ Task submitted with ID: {result.id}")
        
        return "Test tasks completed successfully"
        
    except Exception as e:
        logger.error(f"❌ Error running test tasks: {e}")
        return f"Test failed: {e}"


if __name__ == '__main__':
    print("Celery app configured successfully")
    print(f"Broker: {app.conf.broker_url}")
    print(f"Tasks: {list(app.tasks.keys())}")
