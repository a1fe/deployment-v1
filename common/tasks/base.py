"""
Базовые утилиты и декораторы для Celery задач

Централизованное управление сессиями БД, retry-политикой, 
сериализацией и безопасностью для всех задач системы.
"""

import logging
from functools import wraps
from typing import Any, Callable, Dict, Optional
from contextlib import contextmanager
from celery.utils.log import get_task_logger
from database.config import database
from tasks.task_utils import serialize_for_json, mask_sensitive_data

logger = get_task_logger(__name__)


@contextmanager
def get_db_session():
    """
    Контекстный менеджер для безопасной работы с БД в Celery задачах
    
    Обеспечивает:
    - Автоматическое закрытие сессии
    - Rollback при ошибках
    - Commit при успешном выполнении
    """
    session = database.get_session()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Database session error: {e}")
        raise
    finally:
        session.close()


def standard_retry_policy(max_retries: int = 3, countdown: int = 60, backoff: float = 2.0):
    """
    Стандартный декоратор для retry политики с экспоненциальным backoff
    
    Args:
        max_retries: Максимальное количество повторов
        countdown: Начальная задержка в секундах
        backoff: Множитель для экспоненциального увеличения задержки
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as exc:
                if self.request.retries < max_retries:
                    retry_countdown = countdown * (backoff ** self.request.retries)
                    logger.warning(
                        f"Task {self.name} failed, retrying in {retry_countdown}s "
                        f"(attempt {self.request.retries + 1}/{max_retries}): {exc}"
                    )
                    raise self.retry(countdown=retry_countdown, exc=exc)
                else:
                    logger.error(f"Task {self.name} failed after {max_retries} retries: {exc}")
                    raise
        return wrapper
    return decorator


def safe_task(func: Callable) -> Callable:
    """
    Декоратор для безопасного выполнения задач
    
    Обеспечивает:
    - Маскирование чувствительных данных в логах
    - Автоматическую сериализацию результатов
    - Стандартизированное логирование
    - Обработку исключений
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        task_name = getattr(self, 'name', func.__name__)
        
        # Маскируем чувствительные данные для логирования
        masked_args = []
        for arg in args:
            if isinstance(arg, str):
                masked_args.append(mask_sensitive_data(arg))
            elif isinstance(arg, dict):
                masked_args.append(mask_sensitive_data(str(arg)))
            else:
                masked_args.append(str(arg)[:100])  # Ограничиваем длину
        
        logger.info(f"Task {task_name} started with args: {masked_args}")
        
        try:
            result = func(self, *args, **kwargs)
            
            # Автоматическая сериализация результата
            serialized_result = serialize_for_json(result)
            
            logger.info(f"Task {task_name} completed successfully")
            return serialized_result
            
        except Exception as e:
            logger.error(f"Task {task_name} failed: {str(e)}")
            raise
    
    return wrapper


def celery_friendly_delay(task_name: str, args: Optional[list] = None, kwargs: Optional[dict] = None, countdown: int = 5):
    """
    Celery-friendly способ запуска задач с задержкой вместо time.sleep()
    
    Args:
        task_name: Имя задачи для запуска
        args: Аргументы задачи
        kwargs: Именованные аргументы задачи
        countdown: Задержка в секундах
    """
    from celery import signature
    
    args = args or []
    kwargs = kwargs or {}
    
    return signature(task_name, args=args, kwargs=kwargs).apply_async(countdown=countdown)


def batch_processor(items: list, batch_size: int = 50, delay_between_batches: int = 2):
    """
    Генератор для обработки данных батчами с Celery-friendly задержками
    
    Args:
        items: Список элементов для обработки
        batch_size: Размер батча
        delay_between_batches: Задержка между батчами в секундах
        
    Yields:
        Tuple[list, bool]: (batch_items, is_last_batch)
    """
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        is_last = (i + batch_size) >= len(items)
        
        yield batch, is_last
        
        # Для всех батчей кроме последнего - планируем задержку
        if not is_last:
            logger.info(f"Processed batch {i//batch_size + 1}, scheduling delay...")


class TaskMetrics:
    """
    Простая система метрик для мониторинга задач
    """
    
    @staticmethod
    def log_task_start(task_name: str, **context):
        """Логирование начала выполнения задачи"""
        logger.info(f"📊 METRIC: Task {task_name} started", extra=context)
    
    @staticmethod
    def log_task_success(task_name: str, duration_seconds: float, **context):
        """Логирование успешного завершения задачи"""
        logger.info(f"📊 METRIC: Task {task_name} completed in {duration_seconds:.2f}s", extra=context)
    
    @staticmethod
    def log_task_failure(task_name: str, error: str, **context):
        """Логирование ошибки в задаче"""
        logger.error(f"📊 METRIC: Task {task_name} failed: {error}", extra=context)
    
    @staticmethod
    def log_batch_progress(task_name: str, batch_num: int, total_batches: int, processed_items: int):
        """Логирование прогресса обработки батчей"""
        progress = (batch_num / total_batches) * 100
        logger.info(f"📊 METRIC: {task_name} progress: {progress:.1f}% ({batch_num}/{total_batches} batches, {processed_items} items)")


def monitored_task(func: Callable) -> Callable:
    """
    Декоратор для автоматического мониторинга задач
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        import time
        
        task_name = getattr(self, 'name', func.__name__)
        start_time = time.time()
        
        TaskMetrics.log_task_start(task_name, task_id=getattr(self.request, 'id', None))
        
        try:
            result = func(self, *args, **kwargs)
            duration = time.time() - start_time
            TaskMetrics.log_task_success(task_name, duration)
            return result
        except Exception as e:
            TaskMetrics.log_task_failure(task_name, str(e))
            raise
    
    return wrapper
