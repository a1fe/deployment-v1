"""
Модели компаний
"""

from typing import Optional, List, Dict, Any
from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..base import Base
from ..utils import TimestampMixin, ModelConstraints


class Company(Base, TimestampMixin):
    """Модель компании"""
    __tablename__ = 'companies'
    
    company_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(ModelConstraints.MAX_NAME_LENGTH), nullable=False, unique=True)
    website = Column(String(ModelConstraints.MAX_URL_LENGTH))
    description = Column(Text)
    
    # Relationships
    contacts = relationship("CompanyContact", back_populates="company", cascade="all, delete-orphan")
    industries = relationship("CompanyIndustry", back_populates="company", cascade="all, delete-orphan")
    jobs = relationship("Job", back_populates="company", cascade="all, delete-orphan")
    hiring_stages = relationship("HiringStage", back_populates="company", cascade="all, delete-orphan")
    custom_values = relationship("CustomValue", back_populates="company", cascade="all, delete-orphan")
    
    def __repr__(self) -> str:
        return f"<Company(id={self.company_id}, name='{self.name}')>"
    
    def to_safe_dict(self) -> Dict[str, Any]:
        """Безопасная сериализация компании"""
        result = super().to_safe_dict()
        # Добавляем подсчет связанных объектов
        if hasattr(self, 'jobs'):
            result['jobs_count'] = len(self.jobs) if self.jobs else 0
        if hasattr(self, 'contacts'):
            result['contacts_count'] = len(self.contacts) if self.contacts else 0
        return result


class CompanyContact(Base, TimestampMixin):
    """Модель контактов компании"""
    __tablename__ = 'company_contacts'
    
    contact_id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('companies.company_id', ondelete='CASCADE'), nullable=False)
    full_name = Column(String(ModelConstraints.MAX_NAME_LENGTH), nullable=False)
    email = Column(String(ModelConstraints.MAX_EMAIL_LENGTH), nullable=False)
    phone = Column(String(ModelConstraints.MAX_PHONE_LENGTH))
    job_title = Column(String(100))
    is_primary = Column(Boolean, nullable=False, default=False)
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('company_id', 'email', name='uq_company_contact_email'),
    )
    
    # Relationships
    company = relationship("Company", back_populates="contacts")
    created_jobs = relationship("Job", back_populates="creator")
    assigned_actions = relationship("CandidateAction", back_populates="assignee")
    created_custom_values = relationship("CustomValue", back_populates="creator")
    
    def __repr__(self) -> str:
        email_value = getattr(self, 'email', None)
        email_masked = self._mask_sensitive_data(email_value) if email_value else 'no-email'
        return f"<CompanyContact(id={self.contact_id}, name='{self.full_name}', email='{email_masked}')>"
    
    def validate_email(self) -> bool:
        """Валидация email"""
        email_value = getattr(self, 'email', None)
        if not email_value:
            return False
        return '@' in email_value and '.' in email_value.split('@')[-1]


class CompanyIndustry(Base):
    """Связующая таблица компаний и отраслей"""
    __tablename__ = 'company_industries'
    
    company_id = Column(Integer, ForeignKey('companies.company_id', ondelete='CASCADE'), primary_key=True)
    industry_id = Column(Integer, ForeignKey('industries.industry_id'), primary_key=True)
    
    # Relationships
    company = relationship("Company", back_populates="industries")
    industry = relationship("Industry", back_populates="companies")
    
    def __repr__(self) -> str:
        return f"<CompanyIndustry(company_id={self.company_id}, industry_id={self.industry_id})>"
