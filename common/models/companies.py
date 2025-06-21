"""
SQLAlchemy модели для управления компаниями и вакансиями
"""

from sqlalchemy import (
    Column, Integer, String, Text, Boolean, DateTime, 
    ForeignKey, UniqueConstraint, CheckConstraint, SmallInteger, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from models.base import Base


class Company(Base):
    """Модель компании"""
    __tablename__ = 'companies'
    
    company_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True)
    website = Column(String(255))
    description = Column(Text)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.current_timestamp())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    # Relationships
    contacts = relationship("CompanyContact", back_populates="company", cascade="all, delete-orphan")
    industries = relationship("CompanyIndustry", back_populates="company", cascade="all, delete-orphan")
    jobs = relationship("Job", back_populates="company", cascade="all, delete-orphan")
    hiring_stages = relationship("HiringStage", back_populates="company", cascade="all, delete-orphan")
    custom_values = relationship("CustomValue", back_populates="company", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Company(id={self.company_id}, name='{self.name}')>"


class CompanyContact(Base):
    """Модель контактов компании"""
    __tablename__ = 'company_contacts'
    
    contact_id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('companies.company_id', ondelete='CASCADE'), nullable=False)
    full_name = Column(String(255), nullable=False)
    email = Column(String(255), nullable=False)
    phone = Column(String(50))
    job_title = Column(String(100))
    is_primary = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.current_timestamp())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('company_id', 'email', name='uq_company_contact_email'),
    )
    
    # Relationships
    company = relationship("Company", back_populates="contacts")
    created_jobs = relationship("Job", back_populates="creator")
    assigned_actions = relationship("CandidateAction", back_populates="assignee")
    created_custom_values = relationship("CustomValue", back_populates="creator")
    
    def __repr__(self):
        return f"<CompanyContact(id={self.contact_id}, name='{self.full_name}', email='{self.email}')>"


class CompanyIndustry(Base):
    """Связующая таблица компаний и отраслей"""
    __tablename__ = 'company_industries'
    
    company_id = Column(Integer, ForeignKey('companies.company_id', ondelete='CASCADE'), primary_key=True)
    industry_id = Column(Integer, ForeignKey('industries.industry_id'), primary_key=True)
    
    # Relationships
    company = relationship("Company", back_populates="industries")
    industry = relationship("Industry", back_populates="companies")


class Job(Base):
    """Модель вакансии"""
    __tablename__ = 'jobs'
    
    job_id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('companies.company_id', ondelete='CASCADE'), nullable=False)
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=False)
    job_description_url = Column(Text)
    job_description_raw_text = Column(Text)  # Сырой текст из PDF/DOCX
    job_description_parsed_at = Column(DateTime(timezone=True))  # Время парсинга документа
    employment_type = Column(String(50))
    experience_level = Column(String(50))
    salary_range = Column(String(100))
    currency = Column(String(3))
    location = Column(String(255))
    is_active = Column(Boolean, nullable=False, default=True)
    created_by = Column(Integer, ForeignKey('company_contacts.contact_id', ondelete='SET NULL'))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.current_timestamp())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    # Constraints and Indexes
    __table_args__ = (
        CheckConstraint(
            "employment_type IN ('Full-time', 'Part-time', 'Contract', 'Internship', 'Remote')",
            name='ck_job_employment_type'
        ),
        CheckConstraint(
            "experience_level IN ('Entry', 'Mid', 'Senior', 'Executive')",
            name='ck_job_experience_level'
        ),
        Index('idx_jobs_is_active', 'is_active'),
        Index('idx_jobs_created_at', 'created_at'),
        Index('idx_jobs_company_id', 'company_id'),
        Index('idx_jobs_active_created', 'is_active', 'created_at'),
    )
    
    # Relationships
    company = relationship("Company", back_populates="jobs")
    creator = relationship("CompanyContact", back_populates="created_jobs")
    competencies = relationship("JobCompetency", back_populates="job", cascade="all, delete-orphan")
    candidates = relationship("JobCandidate", back_populates="job", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Job(id={self.job_id}, title='{self.title}', company_id={self.company_id})>"


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


class HiringStage(Base):
    """Модель этапов подбора"""
    __tablename__ = 'hiring_stages'
    
    stage_id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('companies.company_id', ondelete='CASCADE'), nullable=False)
    name = Column(String(100), nullable=False)
    description = Column(Text)
    position = Column(SmallInteger, nullable=False)
    is_default = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.current_timestamp())
    
    # Relationships
    company = relationship("Company", back_populates="hiring_stages")
    candidates = relationship("JobCandidate", back_populates="current_stage")
    actions = relationship("CandidateAction", back_populates="stage")
    
    def __repr__(self):
        return f"<HiringStage(id={self.stage_id}, name='{self.name}', position={self.position})>"


class JobCandidate(Base):
    """Модель кандидатов на вакансии"""
    __tablename__ = 'job_candidates'
    
    candidate_id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey('jobs.job_id', ondelete='CASCADE'), nullable=False)
    full_name = Column(String(255), nullable=False)
    email = Column(String(255), nullable=False)
    phone = Column(String(50))
    resume_url = Column(Text, nullable=False)
    linkedin_url = Column(Text)
    current_stage_id = Column(Integer, ForeignKey('hiring_stages.stage_id', ondelete='SET NULL'))
    application_date = Column(DateTime(timezone=True), nullable=False, server_default=func.current_timestamp())
    status = Column(String(50))
    notes = Column(Text)
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('job_id', 'email', name='uq_job_candidate_email'),
        CheckConstraint(
            "status IN ('active', 'rejected', 'withdrawn', 'hired')",
            name='ck_candidate_status'
        ),
    )
    
    # Relationships
    job = relationship("Job", back_populates="candidates")
    current_stage = relationship("HiringStage", back_populates="candidates")
    actions = relationship("CandidateAction", back_populates="candidate", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<JobCandidate(id={self.candidate_id}, name='{self.full_name}', email='{self.email}')>"


class CandidateAction(Base):
    """Модель действий с кандидатами"""
    __tablename__ = 'candidate_actions'
    
    action_id = Column(Integer, primary_key=True, autoincrement=True)
    candidate_id = Column(Integer, ForeignKey('job_candidates.candidate_id', ondelete='CASCADE'), nullable=False)
    stage_id = Column(Integer, ForeignKey('hiring_stages.stage_id'), nullable=False)
    action_type = Column(String(50))
    action_date = Column(DateTime(timezone=True), nullable=False, server_default=func.current_timestamp())
    notes = Column(Text)
    completed = Column(Boolean, nullable=False, default=False)
    assigned_to = Column(Integer, ForeignKey('company_contacts.contact_id', ondelete='SET NULL'))
    
    # Constraints
    __table_args__ = (
        CheckConstraint(
            "action_type IN ('interview', 'assessment', 'offer', 'rejection', 'note')",
            name='ck_action_type'
        ),
    )
    
    # Relationships
    candidate = relationship("JobCandidate", back_populates="actions")
    stage = relationship("HiringStage", back_populates="actions")
    assignee = relationship("CompanyContact", back_populates="assigned_actions")
    
    def __repr__(self):
        return f"<CandidateAction(id={self.action_id}, type='{self.action_type}', completed={self.completed})>"


class CustomValue(Base):
    """Модель пользовательских значений"""
    __tablename__ = 'custom_values'
    
    custom_value_id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('companies.company_id', ondelete='CASCADE'))
    type = Column(String(20), nullable=False)
    value = Column(String(255), nullable=False)
    created_by = Column(Integer, ForeignKey('company_contacts.contact_id', ondelete='SET NULL'))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.current_timestamp())
    
    # Constraints
    __table_args__ = (
        CheckConstraint("type IN ('industry', 'competency')", name='ck_custom_value_type'),
        UniqueConstraint('company_id', 'type', 'value', name='uq_company_custom_value'),
    )
    
    # Relationships
    company = relationship("Company", back_populates="custom_values")
    creator = relationship("CompanyContact", back_populates="created_custom_values")
    
    def __repr__(self):
        return f"<CustomValue(id={self.custom_value_id}, type='{self.type}', value='{self.value}')>"
