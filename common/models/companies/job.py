"""
Модели вакансий и связанных сущностей
"""

from typing import Optional, List, Dict, Any
from sqlalchemy import (
    Column, Integer, String, Text, Boolean, DateTime, ForeignKey, 
    CheckConstraint, SmallInteger, UniqueConstraint
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from models.base import Base
from models.utils import TimestampMixin, ModelConstraints


class Job(Base, TimestampMixin):
    """Модель вакансии"""
    __tablename__ = 'jobs'
    
    job_id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('companies.company_id', ondelete='CASCADE'), nullable=False)
    title = Column(String(ModelConstraints.MAX_TITLE_LENGTH), nullable=False)
    description = Column(Text, nullable=False)
    job_description_url = Column(String(ModelConstraints.MAX_URL_LENGTH))
    job_description_raw_text = Column(Text)  # Сырой текст из PDF/DOCX
    # Примечание: job_description_parsed_at перенесен в tasks для бизнес-логики
    employment_type = Column(String(50))
    experience_level = Column(String(50))
    salary_range = Column(String(100))
    currency = Column(String(3))
    location = Column(String(ModelConstraints.MAX_NAME_LENGTH))
    is_active = Column(Boolean, nullable=False, default=True)
    created_by = Column(Integer, ForeignKey('company_contacts.contact_id', ondelete='SET NULL'))
    
    # Constraints с использованием констант
    __table_args__ = (
        CheckConstraint(
            f"employment_type IN {tuple(ModelConstraints.EMPLOYMENT_TYPES)}",
            name='ck_job_employment_type'
        ),
        CheckConstraint(
            f"experience_level IN {tuple(ModelConstraints.EXPERIENCE_LEVELS)}",
            name='ck_job_experience_level'
        ),
    )
    
    # Relationships
    company = relationship("Company", back_populates="jobs")
    creator = relationship("CompanyContact", back_populates="created_jobs")
    competencies = relationship("JobCompetency", back_populates="job", cascade="all, delete-orphan")
    candidates = relationship("JobCandidate", back_populates="job", cascade="all, delete-orphan")
    
    def __repr__(self) -> str:
        return f"<Job(id={self.job_id}, title='{self.title}', company_id={self.company_id})>"
    
    def to_safe_dict(self) -> Dict[str, Any]:
        """Безопасная сериализация вакансии"""
        result = super().to_safe_dict()
        # Добавляем подсчет кандидатов
        if hasattr(self, 'candidates'):
            result['candidates_count'] = len(self.candidates) if self.candidates else 0
        return result
    
    def is_employment_type_valid(self) -> bool:
        """Проверка валидности типа занятости"""
        employment_type_value = getattr(self, 'employment_type', None)
        return employment_type_value in ModelConstraints.EMPLOYMENT_TYPES
    
    def is_experience_level_valid(self) -> bool:
        """Проверка валидности уровня опыта"""
        experience_level_value = getattr(self, 'experience_level', None)
        return experience_level_value in ModelConstraints.EXPERIENCE_LEVELS


class JobCompetency(Base):
    """Связующая таблица вакансий и компетенций с уровнем важности"""
    __tablename__ = 'job_competencies'
    
    job_id = Column(Integer, ForeignKey('jobs.job_id', ondelete='CASCADE'), primary_key=True)
    competency_id = Column(Integer, ForeignKey('competencies.competency_id'), primary_key=True)
    importance_level = Column(SmallInteger, default=3)
    
    # Constraints
    __table_args__ = (
        CheckConstraint('importance_level BETWEEN 1 AND 5', name='ck_importance_level'),
    )
    
    # Relationships
    job = relationship("Job", back_populates="competencies")
    competency = relationship("Competency", back_populates="jobs")
    
    def __repr__(self) -> str:
        return f"<JobCompetency(job_id={self.job_id}, competency_id={self.competency_id}, level={self.importance_level})>"


class JobCandidate(Base):
    """Модель кандидатов на вакансии"""
    __tablename__ = 'job_candidates'
    
    candidate_id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey('jobs.job_id', ondelete='CASCADE'), nullable=False)
    full_name = Column(String(ModelConstraints.MAX_NAME_LENGTH), nullable=False)
    email = Column(String(ModelConstraints.MAX_EMAIL_LENGTH), nullable=False)
    phone = Column(String(ModelConstraints.MAX_PHONE_LENGTH))
    resume_url = Column(String(ModelConstraints.MAX_URL_LENGTH), nullable=False)
    linkedin_url = Column(String(ModelConstraints.MAX_URL_LENGTH))
    current_stage_id = Column(Integer, ForeignKey('hiring_stages.stage_id', ondelete='SET NULL'))
    application_date = Column(DateTime(timezone=True), nullable=False, server_default=func.current_timestamp())
    status = Column(String(50))
    notes = Column(Text)
    
    # Constraints с использованием констант
    __table_args__ = (
        UniqueConstraint('job_id', 'email', name='uq_job_candidate_email'),
        CheckConstraint(
            f"status IN {tuple(ModelConstraints.APPLICATION_STATUSES)}",
            name='ck_candidate_status'
        ),
    )
    
    # Relationships
    job = relationship("Job", back_populates="candidates")
    current_stage = relationship("HiringStage", back_populates="candidates")
    actions = relationship("CandidateAction", back_populates="candidate", cascade="all, delete-orphan")
    
    def __repr__(self) -> str:
        email_value = getattr(self, 'email', None)
        email_masked = self._mask_sensitive_data(email_value) if email_value else 'no-email'
        return f"<JobCandidate(id={self.candidate_id}, name='{self.full_name}', email='{email_masked}')>"
    
    def is_status_valid(self) -> bool:
        """Проверка валидности статуса"""
        status_value = getattr(self, 'status', None)
        return status_value in ModelConstraints.APPLICATION_STATUSES
