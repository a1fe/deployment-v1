"""
Модели, связанные с кандидатами и их заявками
"""

from .base import *
from sqlalchemy import Index

# Связующие таблицы для many-to-many отношений
submission_competencies = Table(
    'submission_competencies',
    Base.metadata,
    Column('submission_id', UUID(as_uuid=True), ForeignKey('submissions.submission_id', ondelete='CASCADE'), primary_key=True),
    Column('competency_id', Integer, ForeignKey('competencies.competency_id'), primary_key=True)
)

submission_roles = Table(
    'submission_roles',
    Base.metadata,
    Column('submission_id', UUID(as_uuid=True), ForeignKey('submissions.submission_id', ondelete='CASCADE'), primary_key=True),
    Column('role_id', Integer, ForeignKey('roles.role_id'), primary_key=True)
)

submission_industries = Table(
    'submission_industries',
    Base.metadata,
    Column('submission_id', UUID(as_uuid=True), ForeignKey('submissions.submission_id', ondelete='CASCADE'), primary_key=True),
    Column('industry_id', Integer, ForeignKey('industries.industry_id'), primary_key=True)
)

submission_locations = Table(
    'submission_locations',
    Base.metadata,
    Column('submission_id', UUID(as_uuid=True), ForeignKey('submissions.submission_id', ondelete='CASCADE'), primary_key=True),
    Column('location_id', Integer, ForeignKey('locations.location_id'), primary_key=True)
)


class Candidate(Base):
    """Модель кандидата"""
    __tablename__ = 'candidates'
    
    candidate_id = Column(Integer, primary_key=True, autoincrement=True)
    first_name = Column(String(50), nullable=False)
    last_name = Column(String(50), nullable=False)
    email = Column(String(255), nullable=False, unique=True)
    mobile_number = Column(String(50))
    linkedin_url = Column(Text)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp())
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    # Отношения
    submissions = relationship("Submission", back_populates="candidate", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Candidate(id={self.candidate_id}, name='{self.first_name} {self.last_name}', email='{self.email}')>"


class Submission(Base):
    """Модель заявки кандидата"""
    __tablename__ = 'submissions'
    
    submission_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    candidate_id = Column(Integer, ForeignKey('candidates.candidate_id', ondelete='CASCADE'), nullable=False)
    resume_url = Column(Text, nullable=False)
    resume_raw_text = Column(Text)  # Сырой текст из CV
    resume_parsed_at = Column(TIMESTAMP(timezone=True))  # Время парсинга CV
    agree_to_processing = Column(Boolean, nullable=False)
    agree_to_contact = Column(Boolean, nullable=False)
    status = Column(String(50), nullable=False)
    current_step = Column(String(100), nullable=False)
    submission_started = Column(TIMESTAMP(timezone=True), nullable=False)
    last_updated = Column(TIMESTAMP(timezone=True), nullable=False)
    legally_authorized_us = Column(Boolean)
    requires_sponsorship = Column(Boolean)
    pe_license = Column(Boolean)
    work_preference = Column(String(50))
    willingness_to_travel = Column(Integer, CheckConstraint('willingness_to_travel BETWEEN 0 AND 10'))
    willing_to_relocate = Column(String(50))
    work_shift_related = Column(Boolean)
    available_shifts = Column(Text)
    source = Column(String(100))
    utm_source = Column(String(100))
    utm_medium = Column(String(100))
    utm_campaign = Column(String(100))
    errors = Column(Text)
    url = Column(Text)
    network_id = Column(String(100))
    specific_locations_preferred = Column(Text)
    
    # Отношения
    candidate = relationship("Candidate", back_populates="submissions")
    salary_expectations = relationship("SalaryExpectation", back_populates="submission", cascade="all, delete-orphan")
    addresses = relationship("Address", back_populates="submission", cascade="all, delete-orphan")
    educations = relationship("Education", back_populates="submission", cascade="all, delete-orphan")
    
    # Many-to-many отношения
    competencies = relationship("Competency", secondary=submission_competencies, back_populates="submissions")
    roles = relationship("Role", secondary=submission_roles, back_populates="submissions")
    industries = relationship("Industry", secondary=submission_industries, back_populates="submissions")
    locations = relationship("Location", secondary=submission_locations, back_populates="submissions")
    
    @property
    def personal_info(self):
        """Возвращает информацию о кандидате в формате, совместимом с шаблонами"""
        if self.candidate:
            return {
                'first_name': self.candidate.first_name,
                'last_name': self.candidate.last_name,
                'email': self.candidate.email,
                'phone': self.candidate.mobile_number,
                'linkedin': self.candidate.linkedin_url
            }
        return None
    
    @property
    def is_completed(self):
        """Проверяет, завершена ли заявка"""
        return self.status.lower() in ['completed', 'finished', 'done']
    
    @property
    def skills(self):
        """Возвращает список компетенций как навыков"""
        return [comp.name for comp in self.competencies] if self.competencies else []
    
    @property
    def job_id(self):
        """Возвращает ID связанной вакансии (если есть)"""
        # Это заглушка - в реальной системе может быть связь с конкретной вакансией
        return None
    
    # Индексы для оптимизации
    __table_args__ = (
        Index('idx_submissions_candidate_id', 'candidate_id'),
        Index('idx_submissions_started', 'submission_started'),
        Index('idx_submissions_status', 'status'),
        Index('idx_submissions_candidate_started', 'candidate_id', 'submission_started'),
    )
    
    def __repr__(self):
        return f"<Submission(id={self.submission_id}, candidate_id={self.candidate_id}, status='{self.status}')>"


class SalaryExpectation(Base):
    """Модель зарплатных ожиданий"""
    __tablename__ = 'salary_expectations'
    
    expectation_id = Column(Integer, primary_key=True, autoincrement=True)
    submission_id = Column(UUID(as_uuid=True), ForeignKey('submissions.submission_id', ondelete='CASCADE'), nullable=False)
    min_salary = Column(DECIMAL)
    max_salary = Column(DECIMAL)
    currency = Column(String(3))
    
    # Отношения
    submission = relationship("Submission", back_populates="salary_expectations")
    
    def __repr__(self):
    field_of_study = Column(String(100), nullable=False)
    other_degree_level = Column(Text)  # Для случая когда degree_level = "Other"
    other_field_of_study = Column(Text)  # Для случая когда field_of_study = "Other"
    
    # Отношения
    submission = relationship("Submission", back_populates="educations")
    
    def __repr__(self):
        return f"<Education(id={self.education_id}, degree='{self.degree_level}', field='{self.field_of_study}')>"


# Индексы
Index('idx_candidates_email', Candidate.email)
Index('idx_submissions_candidate', Submission.candidate_id)
Index('idx_submissions_status', Submission.status)
Index('idx_salary_submission', SalaryExpectation.submission_id)
Index('idx_addresses_submission', Address.submission_id)
Index('idx_education_submission', Education.submission_id)
Index('idx_subcomp_competency', submission_competencies.c.competency_id)
Index('idx_subrole_role', submission_roles.c.role_id)
Index('idx_subind_industry', submission_industries.c.industry_id)
Index('idx_subloc_location', submission_locations.c.location_id)
