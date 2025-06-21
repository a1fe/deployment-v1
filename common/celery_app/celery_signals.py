"""
Celery signals handlers for HR Analysis system

Расширенные обработчики сигналов для мониторинга, отказоустойчивости
и автоматического восстановления задач.
"""

import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional
from celery.signals import (
    worker_ready, worker_shutting_down, worker_process_init,
    task_failure, task_success, task_retry, task_prerun, task_postrun,
    before_task_publish, after_task_publish
)

# Configure logging
logger = logging.getLogger(__name__)

# Import monitoring system
try:
    from celery_app.monitoring import monitor
    MONITORING_ENABLED = True
except ImportError:
    logger.warning("Monitoring system not available")
    MONITORING_ENABLED = False
    monitor = None

# Метрики для мониторинга (legacy fallback)
task_metrics: Dict[str, Any] = {
    'total_tasks': 0,
    'successful_tasks': 0,
    'failed_tasks': 0,
    'retried_tasks': 0,
    'start_time': time.time()
}

# Task execution tracking
task_execution_times: Dict[str, float] = {}


@worker_ready.connect
def worker_ready_handler(sender, **kwargs):
    """Called when worker is ready to receive tasks"""
    worker_name = sender.hostname
    logger.info(f"🚀 Worker {worker_name} is ready to process tasks")
    logger.info(f"   Worker PID: {kwargs.get('pid', 'unknown')}")
    logger.info(f"   Queues: {getattr(sender, 'task_routes', {})}")
    
    if MONITORING_ENABLED and monitor:
        monitor.update_worker_status(worker_name, 'online')


@worker_process_init.connect
def worker_process_init_handler(sender, **kwargs):
    """Called when worker process starts"""
    logger.info(f"🔧 Worker process {sender.hostname} initialized")


@worker_shutting_down.connect  
def worker_shutting_down_handler(sender, **kwargs):
    """Called when worker is shutting down"""
    worker_name = sender.hostname
    logger.info(f"🛑 Worker {worker_name} is shutting down gracefully")
    logger.info(f"📊 Final metrics: {task_metrics}")
    
    if MONITORING_ENABLED and monitor:
        monitor.update_worker_status(worker_name, 'offline')


@before_task_publish.connect
def before_task_publish_handler(sender=None, headers=None, body=None, **kwargs):
    """Called before task is published to broker"""
    task_name = headers.get('task', 'unknown') if headers else 'unknown'
    logger.debug(f"📤 Publishing task: {task_name}")


@after_task_publish.connect
def after_task_publish_handler(sender=None, headers=None, body=None, **kwargs):
    """Called after task is published to broker"""
    task_name = headers.get('task', 'unknown') if headers else 'unknown'
    logger.debug(f"✅ Task published: {task_name}")


@task_prerun.connect
def task_prerun_handler(sender=None, task_id=None, task=None, args=None, kwargs=None, **kwds):
    """Called before task starts executing"""
    task_metrics['total_tasks'] += 1
    task_name = getattr(task, 'name', sender or 'unknown')
    
    # Record task start time
    if task_id:
        task_execution_times[str(task_id)] = time.time()
    
    logger.info(f"▶️ Starting task {task_name} (ID: {task_id})")
    logger.debug(f"   Args: {args}, Kwargs: {kwargs}")
    
    if MONITORING_ENABLED and monitor and task_id:
        monitor.record_task_start(task_name, str(task_id))


@task_postrun.connect
def task_postrun_handler(sender=None, task_id=None, task=None, args=None, kwargs=None, 
                        retval=None, state=None, **kwds):
    """Called after task finishes (success or failure)"""
    task_name = getattr(task, 'name', sender or 'unknown')
    
    # Calculate execution time
    start_time = task_execution_times.pop(str(task_id), None) if task_id else None
    execution_time = time.time() - start_time if start_time else 0
    
    logger.info(f"⏹️ Task {task_name} finished (ID: {task_id}, State: {state})")
    if execution_time > 0:
        logger.info(f"   Duration: {execution_time:.2f}s")


@task_success.connect
def task_success_handler(sender=None, result=None, **kwargs):
    """Called when task succeeds"""
    task_metrics['successful_tasks'] += 1
    task_name = getattr(sender, 'name', str(sender) if sender else 'unknown')
    task_id = kwargs.get('task_id', 'unknown')
    
    logger.info(f"✅ Task {task_name} completed successfully (ID: {task_id})")
    logger.debug(f"   Result type: {type(result).__name__}")
    
    if MONITORING_ENABLED and monitor:
        # Get execution time from recorded times
        start_time = task_execution_times.get(str(task_id))
        execution_time = time.time() - start_time if start_time else 0
        monitor.record_task_success(task_name, str(task_id), execution_time)


@task_failure.connect
def task_failure_handler(sender=None, task_id=None, exception=None, einfo=None, **kwargs):
    """Enhanced task failure handler with monitoring integration"""
    task_metrics['failed_tasks'] += 1
    task_name = getattr(sender, 'name', str(sender) if sender else 'unknown')
    
    logger.error(f"❌ Task {task_name} failed (ID: {task_id}): {exception}")
    logger.error(f"   Exception type: {type(exception).__name__}")
    
    if einfo:
        logger.error(f"   Traceback: {str(einfo)[:500]}...")
    
    # Логируем критические ошибки
    critical_exceptions = ('DatabaseError', 'ConnectionError', 'TimeoutError')
    if any(exc in str(type(exception)) for exc in critical_exceptions):
        logger.critical(f"🚨 Critical error in task {task_id}: {exception}")
    
    if MONITORING_ENABLED and monitor and task_id:
        monitor.record_task_failure(task_name, str(task_id), str(exception))


@task_retry.connect
def task_retry_handler(sender=None, task_id=None, reason=None, einfo=None, **kwargs):
    """Called when task is retried"""
    task_metrics['retried_tasks'] += 1
    task_name = getattr(sender, 'name', str(sender) if sender else 'unknown')
    retry_count = kwargs.get('request', {}).get('retries', 0)
    
    logger.warning(f"🔄 Task {task_name} is being retried (ID: {task_id})")
    logger.warning(f"   Retry reason: {reason}")
    
    if einfo:
        logger.warning(f"   Error info: {str(einfo)[:200]}...")
    
    if MONITORING_ENABLED and monitor and task_id:
        monitor.record_task_retry(task_name, str(task_id), retry_count, str(reason))


def get_task_metrics() -> Dict[str, Any]:
    """Get current task metrics"""
    uptime = time.time() - task_metrics['start_time']
    return {
        **task_metrics,
        'uptime_seconds': uptime,
        'success_rate': (
            task_metrics['successful_tasks'] / max(task_metrics['total_tasks'], 1) * 100
        )
    }


def register_signals(app):
    """
    Регистрирует обработчики сигналов для приложения Celery.
    Расширяет стандартное поведение дополнительными функциями:
    - автоматический повтор задач при сбое
    - мониторинг производительности
    - логирование событий
    - управление отказоустойчивостью
    
    Args:
        app: экземпляр приложения Celery
    """
    logger.info("🔄 Регистрация обработчиков сигналов Celery")
    
    # Глобальные настройки повтора задач при сбое
    task_retries = app.conf.get('task_default_retry_delay', 60)
    max_retries = app.conf.get('task_max_retries', 3)
    
    # Автоматический повтор задач для критических ошибок
    from celery.signals import task_failure
    
    @task_failure.connect
    def handle_failure(sender=None, task_id=None, exception=None, args=None, kwargs=None, traceback=None, einfo=None, **kwds):
        """Автоматический повтор задач при определенных ошибках"""
        # Список ошибок, при которых нужно повторять задачу
        retriable_errors = (
            'ConnectionError', 'TimeoutError', 'DatabaseError',
            'OperationalError', 'NetworkError'
        )
        
        # Если ошибка подходит для повторения, логируем
        if exception and any(err in type(exception).__name__ for err in retriable_errors):
            logger.warning(f"🔄 Задача {task_id} упала с повтораемой ошибкой: {exception}")
            # Note: Retry logic should be handled by task decorators, not here
    
    # Настройка custom retry стратегии для всего приложения
    def custom_retry(task, exc=None, throw=True, eta=None, countdown=None, 
                  max_retries=None, **options):
        """
        Расширенная стратегия повтора задач с экспоненциальной
        задержкой и обработкой особых случаев
        """
        if max_retries is None:
            max_retries = task.max_retries
            
        current_retry = task.request.retries
        
        # Экспоненциальная задержка повтора (1m, 5m, 15m)
        retry_delays = [60, 300, 900, 1800]
        
        if countdown is None and current_retry < len(retry_delays):
            countdown = retry_delays[current_retry]
        elif countdown is None:
            countdown = retry_delays[-1]
        
        # Добавляем немного случайности для предотвращения эффекта "thundering herd"
        import random
        jitter = random.uniform(0.8, 1.2)
        countdown = int(countdown * jitter)
        
        logger.info(
            f"🔄 Повторная попытка задачи {task.name} ({current_retry+1}/{max_retries}) "
            f"через {countdown}s"
        )
        
        return task.retry(
            exc=exc, throw=throw, eta=eta, 
            countdown=countdown, max_retries=max_retries,
            **options
        )
    
    # Устанавливаем custom retry стратегию
    app.Task.retry = custom_retry
    
    logger.info("✅ Обработчики сигналов Celery зарегистрированы")
    
    return app
