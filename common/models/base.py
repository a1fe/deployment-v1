"""
Базовые элементы для всех моделей
"""

import uuid
from datetime import datetime
from typing import Dict, Any, Optional
from sqlalchemy import (
    Column, Integer, String, Text, Boolean, TIMESTAMP, 
    DECIMAL, ForeignKey, CheckConstraint, Index, UUID, Table
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

# Базовый класс для всех моделей
BaseModel = declarative_base()


class Base(BaseModel):
    """Расширенный базовый класс с утилитами"""
    __abstract__ = True
    
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


# Экспорт для использования в других модулях
__all__ = [
    'Base', 'Column', 'Integer', 'String', 'Text', 'Boolean', 
    'TIMESTAMP', 'DECIMAL', 'ForeignKey', 'CheckConstraint', 
    'Index', 'UUID', 'Table', 'relationship', 'func', 'uuid', 'datetime'
]
