"""
Утилиты для работы с моделями данных
"""

import uuid
from datetime import datetime
from typing import Dict, Any, Optional, Union
from sqlalchemy import Column, TIMESTAMP, func


class TimestampMixin:
    """Миксин для добавления полей created_at и updated_at"""
    created_at = Column(
        TIMESTAMP(timezone=True), 
        nullable=False, 
        server_default=func.current_timestamp()
    )
    updated_at = Column(
        TIMESTAMP(timezone=True), 
        nullable=False, 
        server_default=func.current_timestamp(), 
        onupdate=func.current_timestamp()
    )


class SerializationMixin:
    """Миксин для сериализации моделей в JSON-совместимый формат"""
    
    def to_dict(self, exclude_fields: Optional[list] = None) -> Dict[str, Any]:
        """
        Преобразует объект модели в словарь
        
        Args:
            exclude_fields: Список полей для исключения из результата
            
        Returns:
            Словарь с данными модели
        """
        exclude_fields = exclude_fields or []
        result = {}
        
        if hasattr(self, '__table__'):
            for column in self.__table__.columns:
                field_name = column.name
                if field_name in exclude_fields:
                    continue
                    
                value = getattr(self, field_name)
                result[field_name] = self._serialize_value(value)
            
        return result
    
    def _serialize_value(self, value: Any) -> Any:
        """Сериализует значение в JSON-совместимый формат"""
        if value is None:
            return None
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, uuid.UUID):
            return str(value)
        elif hasattr(value, 'to_dict'):
            return value.to_dict()
        else:
            return value
    
    def to_safe_dict(self) -> Dict[str, Any]:
        """
        Безопасная сериализация с маскированием конфиденциальных данных
        
        Returns:
            Словарь с замаскированными конфиденциальными полями
        """
        sensitive_fields = ['email', 'phone', 'mobile_number', 'linkedin_url']
        result = self.to_dict()
        
        for field in sensitive_fields:
            if field in result and result[field]:
                result[field] = self._mask_sensitive_data(result[field])
                
        return result
    
    def _mask_sensitive_data(self, value: str) -> str:
        """Маскирует конфиденциальные данные"""
        if '@' in str(value):  # email
            parts = str(value).split('@')
            if len(parts) == 2:
                return f"{parts[0][:2]}***@{parts[1]}"
        elif str(value).startswith(('http', 'www')):  # URL
            return '***'
        elif any(char.isdigit() for char in str(value)):  # phone
            return '***'
        return '***'


class ValidationMixin:
    """Миксин для валидации данных модели"""
    
    def validate(self) -> tuple[bool, list[str]]:
        """
        Валидирует данные модели
        
        Returns:
            Кортеж (валидно, список ошибок)
        """
        errors = []
        
        # Базовая валидация - проверка обязательных полей
        if hasattr(self, '__table__'):
            for column in self.__table__.columns:
                if not column.nullable and column.default is None:
                    value = getattr(self, column.name)
                    if value is None or (isinstance(value, str) and not value.strip()):
                        errors.append(f"Field '{column.name}' is required")
        
        return len(errors) == 0, errors


def generate_uuid() -> uuid.UUID:
    """Генерирует новый UUID"""
    return uuid.uuid4()


def safe_str_convert(value: Any, max_length: Optional[int] = None) -> Optional[str]:
    """
    Безопасное преобразование значения в строку
    
    Args:
        value: Значение для преобразования
        max_length: Максимальная длина строки
        
    Returns:
        Строковое представление или None
    """
    if value is None:
        return None
    
    str_value = str(value).strip()
    if not str_value:
        return None
        
    if max_length and len(str_value) > max_length:
        return str_value[:max_length]
        
    return str_value


def safe_email_validate(email: str) -> bool:
    """
    Простая валидация email
    
    Args:
        email: Email для проверки
        
    Returns:
        True если email валиден
    """
    if not email or not isinstance(email, str):
        return False
    
    email = email.strip().lower()
    return '@' in email and '.' in email.split('@')[-1]


def format_phone_number(phone: Optional[str]) -> Optional[str]:
    """
    Форматирует номер телефона
    
    Args:
        phone: Номер телефона
        
    Returns:
        Отформатированный номер или None
    """
    if not phone:
        return None
    
    # Удаляем все нецифровые символы кроме +
    cleaned = ''.join(char for char in phone if char.isdigit() or char == '+')
    
    if not cleaned:
        return None
        
    return cleaned


# Константы для ограничений
class ModelConstraints:
    """Константы для ограничений моделей"""
    
    # Длины строк
    MAX_NAME_LENGTH = 255
    MAX_EMAIL_LENGTH = 255
    MAX_PHONE_LENGTH = 50
    MAX_TITLE_LENGTH = 255
    MAX_URL_LENGTH = 500
    
    # Статусы
    CANDIDATE_STATUSES = ['active', 'inactive', 'pending', 'archived']
    JOB_STATUSES = ['draft', 'published', 'closed', 'archived']
    APPLICATION_STATUSES = ['applied', 'reviewed', 'interviewed', 'offered', 'hired', 'rejected']
    
    # Уровни опыта
    EXPERIENCE_LEVELS = ['entry', 'junior', 'middle', 'senior', 'lead', 'executive']
    
    # Типы занятости
    EMPLOYMENT_TYPES = ['full_time', 'part_time', 'contract', 'freelance', 'internship']
