"""
Модель полей образования кандидата
Используется для хранения множественных значений field_of_study
"""

from typing import Optional, Dict, Any, List
from sqlalchemy import Column, Integer, String, Text, ForeignKey, UUID, Boolean
from sqlalchemy.orm import relationship
from ..base import Base


class EducationField(Base):
    """Модель поля образования (для множественных значений field_of_study)"""
    __tablename__ = 'education_fields'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    education_id = Column(Integer, ForeignKey('education.education_id', ondelete='CASCADE'), nullable=False)
    field_name = Column(String(500), nullable=False)  # Название поля образования
    field_category = Column(String(100))  # Категория поля (например, 'technical', 'business')
    is_primary = Column(Boolean, default=False)  # Основное поле образования
    
    # Отношения
    education = relationship("Education", back_populates="fields")
    
    def __repr__(self) -> str:
        field_name = getattr(self, 'field_name', 'Unknown')
        return f"<EducationField(id={self.id}, field='{field_name}')>"
    
    def to_safe_dict(self) -> Dict[str, Any]:
        """Безопасная сериализация поля образования"""
        return {
            'id': self.id,
            'education_id': self.education_id,
            'field_name': self.field_name,
            'field_category': self.field_category,
            'is_primary': self.is_primary
        }


def parse_field_of_study_string(field_string: str) -> List[Dict[str, Any]]:
    """
    Парсит строку с множественными полями образования
    
    Args:
        field_string: Строка вида "Engineering, Computer Science, Business Administration"
        
    Returns:
        Список словарей с полями образования
    """
    if not field_string or not isinstance(field_string, str):
        return []
    
    # Разделяем по запятым и очищаем
    fields = [field.strip() for field in field_string.split(',') if field.strip()]
    
    # Удаляем дубликаты, сохраняя порядок
    unique_fields = []
    seen = set()
    for field in fields:
        field_lower = field.lower()
        if field_lower not in seen and len(field) > 2:  # Минимум 3 символа
            unique_fields.append(field)
            seen.add(field_lower)
    
    # Создаем структуры данных
    field_data = []
    for i, field in enumerate(unique_fields):
        field_data.append({
            'field_name': field[:500],  # Обрезаем до максимальной длины
            'field_category': _categorize_field(field),
            'is_primary': i == 0  # Первое поле считается основным
        })
    
    return field_data


def _categorize_field(field_name: str) -> str:
    """
    Определяет категорию поля образования
    
    Args:
        field_name: Название поля образования
        
    Returns:
        Категория поля
    """
    field_lower = field_name.lower()
    
    # Технические поля
    technical_keywords = [
        'engineering', 'computer', 'software', 'technology', 'science',
        'programming', 'data', 'artificial', 'machine', 'information',
        'systems', 'network', 'security', 'mathematics', 'physics'
    ]
    
    # Бизнес поля
    business_keywords = [
        'business', 'management', 'administration', 'finance', 'marketing',
        'economics', 'accounting', 'operations', 'strategy', 'leadership',
        'mba', 'commerce', 'sales'
    ]
    
    # Медицинские поля
    medical_keywords = [
        'medicine', 'medical', 'health', 'nursing', 'pharmacy', 'biology',
        'biotechnology', 'healthcare', 'clinical', 'biomedical'
    ]
    
    # Гуманитарные поля
    humanities_keywords = [
        'art', 'literature', 'history', 'philosophy', 'language', 'psychology',
        'sociology', 'education', 'journalism', 'communication', 'design'
    ]
    
    # Проверяем категории
    for keyword in technical_keywords:
        if keyword in field_lower:
            return 'technical'
    
    for keyword in business_keywords:
        if keyword in field_lower:
            return 'business'
    
    for keyword in medical_keywords:
        if keyword in field_lower:
            return 'medical'
    
    for keyword in humanities_keywords:
        if keyword in field_lower:
            return 'humanities'
    
    return 'other'


def validate_field_name(field_name: str) -> bool:
    """
    Валидация названия поля образования
    
    Args:
        field_name: Название поля
        
    Returns:
        True если поле валидно
    """
    if not field_name or not isinstance(field_name, str):
        return False
    
    field_name = field_name.strip()
    
    # Минимальная длина
    if len(field_name) < 3:
        return False
    
    # Максимальная длина
    if len(field_name) > 500:
        return False
    
    # Не должно содержать только цифры или специальные символы
    if field_name.isdigit() or not any(c.isalpha() for c in field_name):
        return False
    
    return True
