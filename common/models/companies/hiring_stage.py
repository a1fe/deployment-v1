"""
Модели этапов найма и действий с кандидатами
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
    
    def __repr__(self) -> str:
        return f"<HiringStage(id={self.stage_id}, name='{self.name}', position={self.position})>"
    
    def to_dict(self, exclude_fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """Сериализация этапа найма"""
        result = super().to_dict(exclude_fields)
        # Добавляем количество кандидатов на этапе
        if hasattr(self, 'candidates'):
            result['candidates_count'] = len(self.candidates) if self.candidates else 0
        return result


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
    
    # Constraints - типы действий вынесены в константы
    ACTION_TYPES = ['interview', 'assessment', 'offer', 'rejection', 'note', 'review', 'follow_up']
    
    __table_args__ = (
        CheckConstraint(
            f"action_type IN {tuple(ACTION_TYPES)}",
            name='ck_action_type'
        ),
    )
    
    # Relationships
    candidate = relationship("JobCandidate", back_populates="actions")
    stage = relationship("HiringStage", back_populates="actions")
    assignee = relationship("CompanyContact", back_populates="assigned_actions")
    
    def __repr__(self) -> str:
        return f"<CandidateAction(id={self.action_id}, type='{self.action_type}', completed={self.completed})>"
    
    def is_action_type_valid(self) -> bool:
        """Проверка валидности типа действия"""
        action_type_value = getattr(self, 'action_type', None)
        return action_type_value in self.ACTION_TYPES


class CustomValue(Base, TimestampMixin):
    """Модель пользовательских значений"""
    __tablename__ = 'custom_values'
    
    custom_value_id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('companies.company_id', ondelete='CASCADE'))
    type = Column(String(20), nullable=False)
    value = Column(String(ModelConstraints.MAX_NAME_LENGTH), nullable=False)
    created_by = Column(Integer, ForeignKey('company_contacts.contact_id', ondelete='SET NULL'))
    
    # Constraints - типы значений вынесены в константы
    VALUE_TYPES = ['industry', 'competency', 'skill', 'technology', 'location']
    
    __table_args__ = (
        CheckConstraint(f"type IN {tuple(VALUE_TYPES)}", name='ck_custom_value_type'),
        UniqueConstraint('company_id', 'type', 'value', name='uq_company_custom_value'),
    )
    
    # Relationships
    company = relationship("Company", back_populates="custom_values")
    creator = relationship("CompanyContact", back_populates="created_custom_values")
    
    def __repr__(self) -> str:
        return f"<CustomValue(id={self.custom_value_id}, type='{self.type}', value='{self.value}')>"
    
    def is_type_valid(self) -> bool:
        """Проверка валидности типа значения"""
        type_value = getattr(self, 'type', None)
        return type_value in self.VALUE_TYPES
