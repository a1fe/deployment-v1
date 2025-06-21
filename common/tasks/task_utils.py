"""
Утилиты для работы с задачами Celery
"""

import functools
import logging
from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional
from uuid import UUID

from database.config import database

logger = logging.getLogger(__name__)


@contextmanager
def get_db_session():
    """
    Контекстный менеджер для безопасной работы с сессией БД.
    Автоматически закрывает сессию и обрабатывает ошибки.
    """
    db = database.get_session()
    try:
        yield db
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        db.close()


def safe_uuid_convert(value: Any) -> Optional[UUID]:
    """
    Безопасно конвертирует значение в UUID.
    
    Args:
        value: Значение для конвертации
        
    Returns:
        UUID объект или None при ошибке
    """
    if isinstance(value, UUID):
        return value
    
    try:
        return UUID(str(value))
    except (ValueError, TypeError):
        logger.warning(f"Invalid UUID format: {value}")
        return None


def mask_sensitive_data(data: str, mask_char: str = "*", keep_chars: int = 4) -> str:
    """
    Маскирует конфиденциальные данные для безопасного логирования.
    
    Args:
        data: Данные для маскирования
        mask_char: Символ для маскирования
        keep_chars: Количество символов для отображения в начале и конце
        
    Returns:
        Маскированная строка
    """
    if not data or len(data) <= keep_chars * 2:
        return mask_char * len(data) if data else ""
    
    return f"{data[:keep_chars]}{mask_char * (len(data) - keep_chars * 2)}{data[-keep_chars:]}"


def serialize_for_json(data: Any) -> Any:
    """
    Преобразует данные в JSON-совместимый формат.
    
    Args:
        data: Данные для сериализации
        
    Returns:
        JSON-совместимые данные
    """
    if hasattr(data, 'isoformat'):  # datetime объекты
        return data.isoformat()
    elif isinstance(data, UUID):
        return str(data)
    elif isinstance(data, dict):
        return {k: serialize_for_json(v) for k, v in data.items()}
    elif isinstance(data, (list, tuple)):
        return [serialize_for_json(item) for item in data]
    
    return data


def retry_on_failure(max_retries: int = 3, countdown: int = 60):
    """
    Декоратор для автоматического повтора задач при ошибках.
    
    Args:
        max_retries: Максимальное количество попыток
        countdown: Задержка между попытками в секундах
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as exc:
                if self.request.retries < max_retries:
                    logger.warning(
                        f"Task {func.__name__} failed (attempt {self.request.retries + 1}/{max_retries}): {exc}"
                    )
                    raise self.retry(countdown=countdown, max_retries=max_retries, exc=exc)
                else:
                    logger.error(f"Task {func.__name__} failed after {max_retries} attempts: {exc}")
                    raise exc
        return wrapper
    return decorator
