"""
Модель заявки кандидата
"""

import uuid
from typing import Optional, List, Dict, Any
from sqlalchemy import (
    Column, Integer, String, Text, Boolean, ForeignKey, 
    CheckConstraint, UUID, TIMESTAMP
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..base import Base
from ..utils import ModelConstraints
from .associations import (
    submission_competencies, submission_roles, 
    submission_industries, submission_locations
)


class Submission(Base):
    """Модель заявки кандидата"""
    __tablename__ = 'submissions'
    
    submission_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    candidate_id = Column(Integer, ForeignKey('candidates.candidate_id', ondelete='CASCADE'), nullable=False)
    resume_url = Column(String(ModelConstraints.MAX_URL_LENGTH), nullable=False)
    resume_raw_text = Column(Text)  # Сырой текст из CV
    # Примечание: resume_parsed_at перенесен в tasks для бизнес-логики
    agree_to_processing = Column(Boolean, nullable=False)
    agree_to_contact = Column(Boolean, nullable=False)
    status = Column(String(50), nullable=False)
    current_step = Column(String(100), nullable=False)
    submission_started = Column(TIMESTAMP(timezone=True), nullable=False)
    last_updated = Column(TIMESTAMP(timezone=True), nullable=False)
    
    # Поля рабочих предпочтений
    legally_authorized_us = Column(Boolean)
    requires_sponsorship = Column(Boolean)
    pe_license = Column(Boolean)
    work_preference = Column(String(50))
    willingness_to_travel = Column(Integer, CheckConstraint('willingness_to_travel BETWEEN 0 AND 10'))
    willing_to_relocate = Column(String(50))
    work_shift_related = Column(Boolean)
    available_shifts = Column(Text)
    
    # Поля отслеживания источников
    source = Column(String(100))
    utm_source = Column(String(100))
    utm_medium = Column(String(100))
    utm_campaign = Column(String(100))
    network_id = Column(String(100))
    
    # Технические поля
    errors = Column(Text)
    url = Column(String(ModelConstraints.MAX_URL_LENGTH))
    specific_locations_preferred = Column(Text)
    
    # Constraints с использованием констант
    __table_args__ = (
        CheckConstraint(
            f"status IN {tuple(ModelConstraints.CANDIDATE_STATUSES)}",
            name='ck_submission_status'
        ),
    )
    
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
    
    def __repr__(self) -> str:
        return f"<Submission(id={self.submission_id}, candidate_id={self.candidate_id}, status='{self.status}')>"
    
    def to_safe_dict(self) -> Dict[str, Any]:
        """Безопасная сериализация заявки"""
        result = super().to_safe_dict()
        
        # Добавляем статистику по связанным объектам
        if hasattr(self, 'competencies'):
            result['competencies_count'] = len(self.competencies) if self.competencies else 0
        if hasattr(self, 'educations'):
            result['educations_count'] = len(self.educations) if self.educations else 0
        if hasattr(self, 'addresses'):
            result['addresses_count'] = len(self.addresses) if self.addresses else 0
            
        return result
    
    def is_status_valid(self) -> bool:
        """Проверка валидности статуса"""
        status_value = getattr(self, 'status', None)
        return status_value in ModelConstraints.CANDIDATE_STATUSES
    
    def has_valid_agreements(self) -> bool:
        """Проверка обязательных согласий"""
        processing = getattr(self, 'agree_to_processing', False)
        contact = getattr(self, 'agree_to_contact', False)
        return bool(processing and contact)
    
    def get_travel_willingness_description(self) -> str:
        """Описание готовности к командировкам"""
        travel_value = getattr(self, 'willingness_to_travel', None)
        if travel_value is None:
            return "Не указано"
        elif travel_value == 0:
            return "Не готов к командировкам"
        elif travel_value <= 3:
            return "Редкие командировки"
        elif travel_value <= 7:
            return "Умеренные командировки"
        else:
            return "Готов к частым командировкам"

    @property
    def personal_info(self) -> Optional[Dict[str, Any]]:
        """Возвращает персональную информацию кандидата в виде словаря"""
        if self.candidate:
            return {
                'first_name': self.candidate.first_name,
                'last_name': self.candidate.last_name,
                'email': self.candidate.email,
                'phone': self.candidate.mobile_number,
                'linkedin_url': self.candidate.linkedin_url
            }
        return None
