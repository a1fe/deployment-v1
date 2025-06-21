"""
CRUD операции для работы с моделями через SQLAlchemy
"""

import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func

from .base_crud import BaseCRUD
from models.candidates import (
    Candidate, Submission, SalaryExpectation, Address, Education
)
from models.dictionaries import (
    Competency, Role, Industry, Location
)


class CandidateCRUD(BaseCRUD):
    """CRUD операции для кандидатов"""
    
    def __init__(self):
        super().__init__(Candidate)
    
    # Создаем экземпляр для использования базовых методов
    _instance = None
    
    @classmethod
    def _get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    @staticmethod
    def create(db: Session, candidate_data: dict) -> Candidate:
        """Создание нового кандидата"""
        return CandidateCRUD._get_instance().create(db, candidate_data)
    
    @staticmethod
    def get_by_id(db: Session, candidate_id: int) -> Optional[Candidate]:
        """Получение кандидата по ID"""
        return CandidateCRUD._get_instance().get_by_id(db, candidate_id)
    
    @staticmethod
    def get_all(db: Session, skip: int = 0, limit: int = 100) -> List[Candidate]:
        """Получение всех кандидатов с пагинацией"""
        return CandidateCRUD._get_instance().get_all(db, skip, limit)
    
    @staticmethod
    def update(db: Session, candidate_id: int, update_data: dict) -> Optional[Candidate]:
        """Обновление данных кандидата"""
        return CandidateCRUD._get_instance().update(db, candidate_id, update_data)
    
    @staticmethod
    def delete(db: Session, candidate_id: int) -> bool:
        """Удаление кандидата"""
        return CandidateCRUD._get_instance().delete(db, candidate_id)
    
    @staticmethod
    def get_by_email(db: Session, email: str) -> Optional[Candidate]:
        """Получение кандидата по email"""
        return db.query(Candidate).filter(Candidate.email == email).first()
    
    @staticmethod
    def search_by_name(db: Session, name: str) -> List[Candidate]:
        """Поиск кандидатов по имени"""
        search_term = f"%{name}%"
        return db.query(Candidate).filter(
            or_(
                Candidate.first_name.ilike(search_term),
                Candidate.last_name.ilike(search_term)
            )
        ).all()


class SubmissionCRUD(BaseCRUD):
    """CRUD операции для заявок"""
    
    def __init__(self):
        super().__init__(Submission)
    
    # Создаем экземпляр для использования базовых методов
    _instance = None
    
    @classmethod
    def _get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    @staticmethod
    def create(db: Session, submission_data: dict) -> Submission:
        """Создание новой заявки"""
        if 'submission_id' not in submission_data:
            submission_data['submission_id'] = uuid.uuid4()
        return SubmissionCRUD._get_instance().create(db, submission_data)
    
    @staticmethod
    def get_by_id(db: Session, submission_id: uuid.UUID) -> Optional[Submission]:
        """Получение заявки по ID"""
        return db.query(Submission).filter(Submission.submission_id == submission_id).first()
    
    @staticmethod
    def get_by_candidate_id(db: Session, candidate_id: int) -> List[Submission]:
        """Получение всех заявок кандидата"""
        return db.query(Submission).filter(Submission.candidate_id == candidate_id).all()
    
    @staticmethod
    def get_by_status(db: Session, status: str) -> List[Submission]:
        """Получение заявок по статусу"""
        return db.query(Submission).filter(Submission.status == status).all()
    
    @staticmethod
    def update_status(db: Session, submission_id: uuid.UUID, status: str, current_step: Optional[str] = None) -> Optional[Submission]:
        """Обновление статуса заявки"""
        submission = db.query(Submission).filter(Submission.submission_id == submission_id).first()
        if submission:
            submission.status = status
            if current_step:
                submission.current_step = current_step
            submission.last_updated = datetime.utcnow()
            db.commit()
            db.refresh(submission)
        return submission
    
    @staticmethod
    def get_with_salary_range(db: Session, min_salary: float, max_salary: float) -> List[Submission]:
        """Получение заявок в диапазоне зарплат"""
        return db.query(Submission).join(SalaryExpectation).filter(
            and_(
                SalaryExpectation.min_salary >= min_salary,
                SalaryExpectation.max_salary <= max_salary
            )
        ).all()
    
    @staticmethod
    def get_all(db: Session, skip: int = 0, limit: int = 100) -> List[Submission]:
        """Получение всех заявок с пагинацией"""
        return db.query(Submission).offset(skip).limit(limit).all()


class SalaryExpectationCRUD(BaseCRUD):
    """CRUD операции для зарплатных ожиданий"""
    
    def __init__(self):
        super().__init__(SalaryExpectation)
    
    # Создаем экземпляр для использования базовых методов
    _instance = None
    
    @classmethod
    def _get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    @staticmethod
    def create(db: Session, salary_data: dict) -> SalaryExpectation:
        """Создание новой записи о зарплатных ожиданиях"""
        return SalaryExpectationCRUD._get_instance().create(db, salary_data)
    
    @staticmethod
    def get_by_submission_id(db: Session, submission_id: uuid.UUID) -> List[SalaryExpectation]:
        """Получение зарплатных ожиданий по ID заявки"""
        return db.query(SalaryExpectation).filter(SalaryExpectation.submission_id == submission_id).all()


class CompetencyCRUD:
    """CRUD операции для компетенций"""
    
    @staticmethod
    def create(db: Session, name: str, other_description: Optional[str] = None) -> Competency:
        """Создание новой компетенции"""
        competency = Competency(name=name, other_description=other_description)
        db.add(competency)
        db.commit()
        db.refresh(competency)
        return competency
    
    @staticmethod
    def get_or_create(db: Session, name: str, other_description: Optional[str] = None) -> Competency:
        """Получение существующей или создание новой компетенции"""
        competency = db.query(Competency).filter(Competency.name == name).first()
        if not competency:
            competency = CompetencyCRUD.create(db, name, other_description)
        elif name == "Other" and other_description and not competency.other_description:
            # Обновляем описание для Other, если оно не было установлено
            competency.other_description = other_description
            db.commit()
            db.refresh(competency)
        return competency
    
    @staticmethod
    def get_all(db: Session) -> List[Competency]:
        """Получение всех компетенций"""
        return db.query(Competency).all()
    
    @staticmethod
    def get_popular(db: Session, limit: int = 10) -> List[tuple]:
        """Получение самых популярных компетенций"""
        return db.query(
            Competency.name,
            func.count(Submission.submission_id).label('count')
        ).join(
            Submission.competencies
        ).group_by(
            Competency.competency_id, Competency.name
        ).order_by(
            func.count(Submission.submission_id).desc()
        ).limit(limit).all()


class RoleCRUD:
    """CRUD операции для ролей"""
    
    @staticmethod
    def get_or_create(db: Session, name: str, other_description: Optional[str] = None) -> Role:
        """Получение существующей или создание новой роли"""
        role = db.query(Role).filter(Role.name == name).first()
        if not role:
            role = Role(name=name, other_description=other_description)
            db.add(role)
            db.commit()
            db.refresh(role)
        elif name == "Other" and other_description and not role.other_description:
            # Обновляем описание для Other, если оно не было установлено
            role.other_description = other_description
            db.commit()
            db.refresh(role)
        return role


class IndustryCRUD:
    """CRUD операции для отраслей"""
    
    @staticmethod
    def get_or_create(db: Session, name: str, other_description: Optional[str] = None) -> Industry:
        """Получение существующей или создание новой отрасли"""
        industry = db.query(Industry).filter(Industry.name == name).first()
        if not industry:
            industry = Industry(name=name, other_description=other_description)
            db.add(industry)
            db.commit()
            db.refresh(industry)
        elif name == "Other" and other_description and not industry.other_description:
            # Обновляем описание для Other, если оно не было установлено
            industry.other_description = other_description
            db.commit()
            db.refresh(industry)
        return industry


class LocationCRUD:
    """CRUD операции для локаций"""
    
    @staticmethod
    def get_or_create(db: Session, name: str, specific_country: Optional[str] = None) -> Location:
        """Получение существующей или создание новой локации"""
        location = db.query(Location).filter(Location.name == name).first()
        if not location:
            location = Location(name=name, specific_country=specific_country)
            db.add(location)
            db.commit()
            db.refresh(location)
        elif name == "Specific Country (write-in option)" and specific_country and not location.specific_country:
            # Обновляем конкретную страну для Specific Country, если она не была установлена
            location.specific_country = specific_country
            db.commit()
            db.refresh(location)
        return location


class AnalyticsCRUD:
    """Аналитические запросы"""
    
    @staticmethod
    def get_submission_statistics(db: Session) -> Dict[str, Any]:
        """Статистика по заявкам"""
        total_submissions = db.query(Submission).count()
        
        status_stats = db.query(
            Submission.status,
            func.count(Submission.submission_id).label('count')
        ).group_by(Submission.status).all()
        
        source_stats = db.query(
            Submission.source,
            func.count(Submission.submission_id).label('count')
        ).filter(Submission.source.isnot(None)).group_by(Submission.source).all()
        
        return {
            'total_submissions': total_submissions,
            'status_distribution': [{'status': stat.status, 'count': stat.count} for stat in status_stats],
            'source_distribution': [{'source': stat.source, 'count': stat.count} for stat in source_stats]
        }
    
    @staticmethod
    def get_salary_statistics(db: Session) -> List[Dict[str, Any]]:
        """Статистика по зарплатам"""
        salary_stats = db.query(
            func.avg(SalaryExpectation.min_salary).label('avg_min_salary'),
            func.avg(SalaryExpectation.max_salary).label('avg_max_salary'),
            func.min(SalaryExpectation.min_salary).label('min_salary'),
            func.max(SalaryExpectation.max_salary).label('max_salary'),
            SalaryExpectation.currency
        ).group_by(SalaryExpectation.currency).all()
        
        return [
            {
                'currency': stat.currency,
                'avg_min_salary': float(stat.avg_min_salary) if stat.avg_min_salary else 0,
                'avg_max_salary': float(stat.avg_max_salary) if stat.avg_max_salary else 0,
                'min_salary': float(stat.min_salary) if stat.min_salary else 0,
                'max_salary': float(stat.max_salary) if stat.max_salary else 0
            }
            for stat in salary_stats
        ]
    
    @staticmethod
    def get_candidate_profile(db: Session, candidate_id: int) -> Optional[Dict[str, Any]]:
        """Полный профиль кандидата"""
        candidate = db.query(Candidate).filter(Candidate.candidate_id == candidate_id).first()
        if not candidate:
            return None
        
        submissions = db.query(Submission).filter(Submission.candidate_id == candidate_id).all()
        
        profile = {
            'candidate': {
                'id': candidate.candidate_id,
                'name': f"{candidate.first_name} {candidate.last_name}",
                'email': candidate.email,
                'mobile': candidate.mobile_number,
                'linkedin': candidate.linkedin_url,
                'created_at': candidate.created_at,
                'updated_at': candidate.updated_at
            },
            'submissions': []
        }
        
        for submission in submissions:
            submission_data = {
                'id': str(submission.submission_id),
                'status': submission.status,
                'current_step': submission.current_step,
                'work_preference': submission.work_preference,
                'willingness_to_travel': submission.willingness_to_travel,
                'competencies': [comp.name for comp in submission.competencies],
                'roles': [role.name for role in submission.roles],
                'industries': [ind.name for ind in submission.industries],
                'locations': [loc.name for loc in submission.locations],
                'salary_expectations': [],
                'addresses': [],
                'education': []
            }
            
            # Добавляем связанные данные
            for salary in submission.salary_expectations:
                submission_data['salary_expectations'].append({
                    'min_salary': float(salary.min_salary) if salary.min_salary else None,
                    'max_salary': float(salary.max_salary) if salary.max_salary else None,
                    'currency': salary.currency
                })
            
            for address in submission.addresses:
                submission_data['addresses'].append({
                    'address': address.address,
                    'city': address.city,
                    'state': address.state_province,
                    'zip': address.zip_postal_code,
                    'country': address.country
                })
            
            for education in submission.educations:
                submission_data['education'].append({
                    'degree_level': education.degree_level,
                    'field_of_study': education.field_of_study,
                    'other_field': education.other_field_of_study
                })
            
            profile['submissions'].append(submission_data)
        
        return profile
