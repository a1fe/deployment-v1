"""
Модель кандидата
"""

from typing import Optional, List, Dict, Any
from sqlalchemy import Column, Integer, String, Text, Boolean
from sqlalchemy.orm import relationship
from ..base import Base
from ..utils import TimestampMixin, ModelConstraints


class Candidate(Base, TimestampMixin):
    """Модель кандидата"""
    __tablename__ = 'candidates'
    
    candidate_id = Column(Integer, primary_key=True, autoincrement=True)
    first_name = Column(String(50), nullable=False)
    last_name = Column(String(50), nullable=False)
    email = Column(String(ModelConstraints.MAX_EMAIL_LENGTH), nullable=False, unique=True)
    mobile_number = Column(String(ModelConstraints.MAX_PHONE_LENGTH))
    linkedin_url = Column(String(ModelConstraints.MAX_URL_LENGTH))
    
    # Отношения
    submissions = relationship("Submission", back_populates="candidate", cascade="all, delete-orphan")
    
    def __repr__(self) -> str:
        email_value = getattr(self, 'email', None)
        email_masked = self._mask_sensitive_data(email_value) if email_value else 'no-email'
        return f"<Candidate(id={self.candidate_id}, name='{self.first_name} {self.last_name}', email='{email_masked}')>"
    
    @property
    def full_name(self) -> str:
        """Полное имя кандидата"""
        first_name = getattr(self, 'first_name', '') or ''
        last_name = getattr(self, 'last_name', '') or ''
        return f"{first_name} {last_name}".strip()
    
    def to_safe_dict(self) -> Dict[str, Any]:
        """Безопасная сериализация кандидата"""
        result = super().to_safe_dict()
        result['full_name'] = self.full_name
        
        # Добавляем статистику по заявкам
        if hasattr(self, 'submissions'):
            result['submissions_count'] = len(self.submissions) if self.submissions else 0
        
        return result
    
    def validate_email(self) -> bool:
        """Валидация email"""
        email_value = getattr(self, 'email', None)
        if not email_value:
            return False
        return '@' in email_value and '.' in email_value.split('@')[-1]
    
    def has_linkedin(self) -> bool:
        """Проверка наличия LinkedIn профиля"""
        linkedin_value = getattr(self, 'linkedin_url', None)
        return bool(linkedin_value and 'linkedin.com' in linkedin_value)
