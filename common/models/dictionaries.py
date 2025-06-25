"""
Общие справочники для системы HR Analysis
"""

from .base import Base
from sqlalchemy import Column, Integer, String, Boolean, Text
from sqlalchemy.orm import relationship


class Industry(Base):
    """Модель отрасли"""
    __tablename__ = 'industries'
    
    industry_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    other_description = Column(Text)  # Для поддержки "Other" значений
    is_primary = Column(Boolean, nullable=False, default=True)
    
    # Relationships (используем строковые ссылки для избежания циклических импортов)
    companies = relationship("CompanyIndustry", back_populates="industry", cascade="all, delete-orphan")
    submissions = relationship("Submission", secondary="submission_industries", back_populates="industries")
    
    def __repr__(self):
        return f"<Industry(id={self.industry_id}, name='{self.name}')>"


class Competency(Base):
    """Модель компетенции"""
    __tablename__ = 'competencies'
    
    competency_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    other_description = Column(Text)  # Для поддержки "Other" значений
    is_primary = Column(Boolean, nullable=False, default=True)
    
    # Relationships
    jobs = relationship("JobCompetency", back_populates="competency")
    submissions = relationship("Submission", secondary="submission_competencies", back_populates="competencies")
    
    def __repr__(self):
        return f"<Competency(id={self.competency_id}, name='{self.name}')>"


class Role(Base):
    """Модель роли"""
    __tablename__ = 'roles'
    
    role_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    other_description = Column(Text)  # Для поддержки "Other" значений
    
    # Relationships
    submissions = relationship("Submission", secondary="submission_roles", back_populates="roles")
    
    def __repr__(self):
        return f"<Role(id={self.role_id}, name='{self.name}')>"


class Location(Base):
    """Модель локации"""
    __tablename__ = 'locations'
    
    location_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    specific_country = Column(String(100))  # Для поддержки "Specific Country" значений
    
    # Relationships
    submissions = relationship("Submission", secondary="submission_locations", back_populates="locations")
    
    def __repr__(self):
        return f"<Location(id={self.location_id}, name='{self.name}')>"
