"""
Модель образования кандидата
"""

from typing import Optional, Dict, Any
from sqlalchemy import Column, Integer, String, Text, ForeignKey, UUID
from sqlalchemy.orm import relationship
from models.base import Base


class Education(Base):
    """Модель образования"""
    __tablename__ = 'education'
    
    education_id = Column(Integer, primary_key=True, autoincrement=True)
    submission_id = Column(UUID(as_uuid=True), ForeignKey('submissions.submission_id', ondelete='CASCADE'), nullable=False)
    degree_level = Column(String(100), nullable=False)
    field_of_study = Column(String(100), nullable=False)
    other_degree_level = Column(Text)  # Для случая когда degree_level = "Other"
    other_field_of_study = Column(Text)  # Для случая когда field_of_study = "Other"
    
    # Константы для уровней образования
    DEGREE_LEVELS = [
        'high_school', 'associate', 'bachelor', 'master', 
        'phd', 'professional', 'certificate', 'other'
    ]
    
    # Отношения
    submission = relationship("Submission", back_populates="educations")
    
    def __repr__(self) -> str:
        degree_val = getattr(self, 'degree_level', 'Unknown')
        field_val = getattr(self, 'field_of_study', 'Unknown')
        return f"<Education(id={self.education_id}, degree='{degree_val}', field='{field_val}')>"
    
    def get_display_degree(self) -> str:
        """Отображаемое название степени"""
        degree_val = getattr(self, 'degree_level', None)
        other_degree = getattr(self, 'other_degree_level', None)
        
        if degree_val == 'other' and other_degree:
            return str(other_degree).strip()
        elif degree_val:
            return self._format_degree_level(degree_val)
        else:
            return "Не указано"
    
    def get_display_field(self) -> str:
        """Отображаемое название специальности"""
        field_val = getattr(self, 'field_of_study', None)
        other_field = getattr(self, 'other_field_of_study', None)
        
        if field_val == 'other' and other_field:
            return str(other_field).strip()
        elif field_val:
            return str(field_val).replace('_', ' ').title()
        else:
            return "Не указано"
    
    def get_full_education(self) -> str:
        """Полное описание образования"""
        degree = self.get_display_degree()
        field = self.get_display_field()
        return f"{degree} в области {field}"
    
    def is_degree_level_valid(self) -> bool:
        """Проверка валидности уровня образования"""
        degree_val = getattr(self, 'degree_level', None)
        return degree_val in self.DEGREE_LEVELS
    
    def _format_degree_level(self, degree: str) -> str:
        """Форматирование названия степени"""
        degree_map = {
            'high_school': 'Среднее образование',
            'associate': 'Среднее специальное',
            'bachelor': 'Бакалавр',
            'master': 'Магистр',
            'phd': 'Доктор наук',
            'professional': 'Профессиональная степень',
            'certificate': 'Сертификат',
            'other': 'Другое'
        }
        return degree_map.get(degree, degree.replace('_', ' ').title())
