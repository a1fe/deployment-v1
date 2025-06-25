"""
Модель зарплатных ожиданий кандидата
"""

from typing import Optional, Dict, Any
from sqlalchemy import Column, Integer, ForeignKey, DECIMAL, String, UUID
from sqlalchemy.orm import relationship
from ..base import Base


class SalaryExpectation(Base):
    """Модель зарплатных ожиданий"""
    __tablename__ = 'salary_expectations'
    
    expectation_id = Column(Integer, primary_key=True, autoincrement=True)
    submission_id = Column(UUID(as_uuid=True), ForeignKey('submissions.submission_id', ondelete='CASCADE'), nullable=False)
    min_salary = Column(DECIMAL(precision=10, scale=2))
    max_salary = Column(DECIMAL(precision=10, scale=2))
    currency = Column(String(3), default='USD')
    
    # Отношения
    submission = relationship("Submission", back_populates="salary_expectations")
    
    def __repr__(self) -> str:
        min_val = getattr(self, 'min_salary', None)
        max_val = getattr(self, 'max_salary', None)
        min_val = float(min_val) if min_val else 0
        max_val = float(max_val) if max_val else 0
        currency_val = getattr(self, 'currency', 'USD')
        return f"<SalaryExpectation(id={self.expectation_id}, range={min_val}-{max_val} {currency_val})>"
    
    def to_dict(self, exclude_fields: Optional[list] = None) -> Dict[str, Any]:
        """Сериализация зарплатных ожиданий"""
        result = super().to_dict(exclude_fields)
        
        # Преобразуем DECIMAL в float для JSON
        if result.get('min_salary'):
            result['min_salary'] = float(result['min_salary'])
        if result.get('max_salary'):
            result['max_salary'] = float(result['max_salary'])
            
        return result
    
    def get_salary_range_formatted(self) -> str:
        """Форматированная строка диапазона зарплат"""
        min_val = getattr(self, 'min_salary', None)
        max_val = getattr(self, 'max_salary', None)
        min_val = float(min_val) if min_val else 0
        max_val = float(max_val) if max_val else 0
        currency_val = getattr(self, 'currency', 'USD')
        
        if min_val and max_val:
            return f"{min_val:,.0f} - {max_val:,.0f} {currency_val}"
        elif min_val:
            return f"от {min_val:,.0f} {currency_val}"
        elif max_val:
            return f"до {max_val:,.0f} {currency_val}"
        else:
            return "Не указано"
    
    def is_range_valid(self) -> bool:
        """Проверка валидности диапазона"""
        min_val = getattr(self, 'min_salary', None)
        max_val = getattr(self, 'max_salary', None)
        min_val = float(min_val) if min_val else 0
        max_val = float(max_val) if max_val else 0
        
        if min_val and max_val:
            return min_val <= max_val
        return True  # Если указана только одна граница, это валидно
