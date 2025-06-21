"""
Связующие таблицы для модели кандидатов
"""

from sqlalchemy import Table, Column, Integer, ForeignKey, UUID
from models.base import Base

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
