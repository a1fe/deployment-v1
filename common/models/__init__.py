"""
Models package - модульная архитектура для HR Analysis system
"""

# Базовые элементы
from .base import Base
from .utils import (
    TimestampMixin, SerializationMixin, ValidationMixin,
    ModelConstraints, generate_uuid, safe_str_convert,
    safe_email_validate, format_phone_number
)

# Справочники (сохраняем обратную совместимость)
from .dictionaries import Industry, Competency, Role, Location

__all__ = [
    # Базовые
    'Base',
    'TimestampMixin',
    'SerializationMixin', 
    'ValidationMixin',
    'ModelConstraints',
    'generate_uuid',
    'safe_str_convert',
    'safe_email_validate',
    'format_phone_number',
    
    # Справочники
    'Industry',
    'Competency', 
    'Role',
    'Location'
]
