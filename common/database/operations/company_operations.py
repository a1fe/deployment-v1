"""
CRUD операции для управления компаниями и вакансиями
"""

import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func, desc

from .base_crud import BaseCRUD
from models.companies import (
    Company, CompanyContact, CompanyIndustry, Job, JobCompetency,
    HiringStage, JobCandidate, CandidateAction, CustomValue
)
from models.dictionaries import Industry, Competency


class CompanyCRUD(BaseCRUD):
    """CRUD операции для компаний"""
    
    def __init__(self):
        super().__init__(Company)
    
    # Создаем экземпляр для использования базовых методов
    _instance = None
    
    @classmethod
    def _get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    @staticmethod
    def create(db: Session, company_data: dict) -> Company:
        """Создание новой компании"""
        return CompanyCRUD._get_instance().create(db, company_data)
    
    @staticmethod
    def get_by_id(db: Session, company_id: int) -> Optional[Company]:
        """Получение компании по ID"""
        return CompanyCRUD._get_instance().get_by_id(db, company_id)
    
    @staticmethod
    def get_all(db: Session, skip: int = 0, limit: int = 100) -> List[Company]:
        """Получение всех компаний с пагинацией"""
        return CompanyCRUD._get_instance().get_all(db, skip, limit)
    
    @staticmethod
    def update(db: Session, company_id: int, update_data: dict) -> Optional[Company]:
        """Обновление данных компании"""
        return CompanyCRUD._get_instance().update(db, company_id, update_data)
    
    @staticmethod
    def delete(db: Session, company_id: int) -> bool:
        """Удаление компании"""
        return CompanyCRUD._get_instance().delete(db, company_id)
    
    @staticmethod
    def get_by_name(db: Session, name: str) -> Optional[Company]:
        """Получение компании по названию"""
        return db.query(Company).filter(Company.name == name).first()
    
    @staticmethod
    def search_by_name(db: Session, name: str) -> List[Company]:
        """Поиск компаний по названию с использованием полнотекстового поиска PostgreSQL"""
        from sqlalchemy import text
        
        # Используем полнотекстовый поиск PostgreSQL для эффективного поиска
        search_query = text("""
            SELECT * FROM companies 
            WHERE to_tsvector('english', name || ' ' || COALESCE(description, '')) 
            @@ plainto_tsquery('english', :search_term)
            ORDER BY ts_rank(to_tsvector('english', name || ' ' || COALESCE(description, '')), 
                           plainto_tsquery('english', :search_term)) DESC
        """)
        
        result = db.execute(search_query, {"search_term": name})
        company_rows = result.fetchall()
        
        # Если полнотекстовый поиск не дал результатов, используем обычный ILIKE как fallback
        if not company_rows:
            search_term = f"%{name}%"
            return db.query(Company).filter(Company.name.ilike(search_term)).all()
        
        # Преобразуем результаты в объекты Company
        company_ids = [row[0] for row in company_rows]  # company_id - первое поле
        return db.query(Company).filter(Company.company_id.in_(company_ids)).all()

    @staticmethod
    def search_by_name_paginated(db: Session, name: str, page: int = 1, per_page: int = 10) -> dict:
        """Поиск компаний по названию с пагинацией для больших результатов"""
        from sqlalchemy import text
        
        offset = (page - 1) * per_page
        
        # Подсчет общего количества результатов
        count_query = text("""
            SELECT COUNT(*) FROM companies 
            WHERE to_tsvector('english', name || ' ' || COALESCE(description, '')) 
            @@ plainto_tsquery('english', :search_term)
        """)
        
        total_count = db.execute(count_query, {"search_term": name}).scalar()
        
        if total_count == 0:
            # Fallback поиск
            search_term = f"%{name}%"
            total_count = db.query(Company).filter(Company.name.ilike(search_term)).count()
            companies = db.query(Company).filter(Company.name.ilike(search_term)).offset(offset).limit(per_page).all()
        else:
            # Основной поиск с пагинацией
            search_query = text("""
                SELECT * FROM companies 
                WHERE to_tsvector('english', name || ' ' || COALESCE(description, '')) 
                @@ plainto_tsquery('english', :search_term)
                ORDER BY ts_rank(to_tsvector('english', name || ' ' || COALESCE(description, '')), 
                               plainto_tsquery('english', :search_term)) DESC
                LIMIT :limit OFFSET :offset
            """)
            
            result = db.execute(search_query, {
                "search_term": name, 
                "limit": per_page, 
                "offset": offset
            })
            company_rows = result.fetchall()
            company_ids = [row[0] for row in company_rows]
            companies = db.query(Company).filter(Company.company_id.in_(company_ids)).all()
        
        return {
            "companies": companies,
            "total": total_count,
            "page": page,
            "per_page": per_page,
            "pages": (total_count + per_page - 1) // per_page
        }


class CompanyContactCRUD(BaseCRUD):
    """CRUD операции для контактов компании"""
    
    def __init__(self):
        super().__init__(CompanyContact)
    
    # Создаем экземпляр для использования базовых методов
    _instance = None
    
    @classmethod
    def _get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    @staticmethod
    def create(db: Session, contact_data: dict) -> CompanyContact:
        """Создание нового контакта"""
        return CompanyContactCRUD._get_instance().create(db, contact_data)
        db.commit()
        db.refresh(contact)
        return contact
    
    @staticmethod
    def get_by_company_id(db: Session, company_id: int) -> List[CompanyContact]:
        """Получение всех контактов компании"""
        return db.query(CompanyContact).filter(CompanyContact.company_id == company_id).all()
    
    @staticmethod
    def get_primary_contact(db: Session, company_id: int) -> Optional[CompanyContact]:
        """Получение основного контакта компании"""
        return db.query(CompanyContact).filter(
            and_(CompanyContact.company_id == company_id, CompanyContact.is_primary == True)
        ).first()
    
    @staticmethod
    def set_primary_contact(db: Session, contact_id: int) -> bool:
        """Установка контакта как основного"""
        contact = db.query(CompanyContact).filter(CompanyContact.contact_id == contact_id).first()
        if contact:
            # Сначала убираем флаг primary у всех контактов этой компании
            db.query(CompanyContact).filter(CompanyContact.company_id == contact.company_id).update(
                {'is_primary': False}
            )
            # Устанавливаем флаг для выбранного контакта
            contact.is_primary = True
            db.commit()
            return True
        return False


class JobCRUD:
    """CRUD операции для вакансий"""
    
    @staticmethod
    def create(db: Session, job_data: dict) -> Job:
        """Создание новой вакансии"""
        job = Job(**job_data)
        db.add(job)
        db.commit()
        db.refresh(job)
        return job
    
    @staticmethod
    def get_by_id(db: Session, job_id: int) -> Optional[Job]:
        """Получение вакансии по ID"""
        return db.query(Job).filter(Job.job_id == job_id).first()
    
    @staticmethod
    def get_by_company_id(db: Session, company_id: int, active_only: bool = True) -> List[Job]:
        """Получение всех вакансий компании"""
        query = db.query(Job).filter(Job.company_id == company_id)
        if active_only:
            query = query.filter(Job.is_active == True)
        return query.order_by(desc(Job.created_at)).all()
    
    @staticmethod
    def get_all_active(db: Session) -> List[Job]:
        """Получение всех активных вакансий"""
        return db.query(Job).filter(Job.is_active == True).order_by(desc(Job.created_at)).all()
    
    @staticmethod
    def get_all(db: Session, skip: int = 0, limit: int = 100) -> List[Job]:
        """Получение всех вакансий с пагинацией"""
        return db.query(Job).offset(skip).limit(limit).order_by(desc(Job.created_at)).all()
    
    @staticmethod
    def search(db: Session, title: str = None, location: str = None, 
               employment_type: str = None, experience_level: str = None) -> List[Job]:
        """Поиск вакансий по критериям"""
        query = db.query(Job).filter(Job.is_active == True)
        
        if title:
            query = query.filter(Job.title.ilike(f"%{title}%"))
        if location:
            query = query.filter(Job.location.ilike(f"%{location}%"))
        if employment_type:
            query = query.filter(Job.employment_type == employment_type)
        if experience_level:
            query = query.filter(Job.experience_level == experience_level)
        
        return query.order_by(desc(Job.created_at)).all()
    
    @staticmethod
    def deactivate(db: Session, job_id: int) -> bool:
        """Деактивация вакансии"""
        job = db.query(Job).filter(Job.job_id == job_id).first()
        if job:
            job.is_active = False
            job.updated_at = datetime.utcnow()
            db.commit()
            return True
        return False
    
    @staticmethod
    def add_competency(db: Session, job_id: int, competency_id: int, importance_level: int = 3) -> bool:
        """Добавление компетенции к вакансии"""
        job_competency = JobCompetency(
            job_id=job_id,
            competency_id=competency_id,
            importance_level=importance_level
        )
        db.add(job_competency)
        try:
            db.commit()
            return True
        except:
            db.rollback()
            return False


class HiringStageCRUD:
    """CRUD операции для этапов подбора"""
    
    @staticmethod
    def create_default_stages(db: Session, company_id: int) -> List[HiringStage]:
        """Создание стандартных этапов подбора для компании"""
        default_stages = [
            {'name': 'Application Review', 'position': 1, 'description': 'Review of submitted applications'},
            {'name': 'Screening Call', 'position': 2, 'description': 'Initial phone/video screening'},
            {'name': 'Technical Interview', 'position': 3, 'description': 'Technical assessment and interview'},
            {'name': 'Final Interview', 'position': 4, 'description': 'Final interview with team/management'},
            {'name': 'Offer Sent', 'position': 5, 'description': 'Job offer sent to candidate'},
            {'name': 'Hired', 'position': 6, 'description': 'Candidate accepted offer and hired'},
        ]
        
        stages = []
        for stage_data in default_stages:
            stage = HiringStage(
                company_id=company_id,
                **stage_data,
                is_default=True
            )
            db.add(stage)
            stages.append(stage)
        
        db.commit()
        return stages
    
    @staticmethod
    def get_by_company_id(db: Session, company_id: int) -> List[HiringStage]:
        """Получение всех этапов подбора компании"""
        return db.query(HiringStage).filter(
            HiringStage.company_id == company_id
        ).order_by(HiringStage.position).all()


class JobCandidateCRUD:
    """CRUD операции для кандидатов на вакансии"""
    
    @staticmethod
    def create(db: Session, candidate_data: dict) -> JobCandidate:
        """Добавление кандидата на вакансию"""
        candidate = JobCandidate(**candidate_data)
        db.add(candidate)
        db.commit()
        db.refresh(candidate)
        return candidate
    
    @staticmethod
    def get_by_job_id(db: Session, job_id: int) -> List[JobCandidate]:
        """Получение всех кандидатов на вакансию"""
        return db.query(JobCandidate).filter(JobCandidate.job_id == job_id).all()
    
    @staticmethod
    def get_by_status(db: Session, job_id: int, status: str) -> List[JobCandidate]:
        """Получение кандидатов по статусу"""
        return db.query(JobCandidate).filter(
            and_(JobCandidate.job_id == job_id, JobCandidate.status == status)
        ).all()
    
    @staticmethod
    def move_to_stage(db: Session, candidate_id: int, stage_id: int, notes: str = None) -> bool:
        """Перевод кандидата на следующий этап"""
        candidate = db.query(JobCandidate).filter(JobCandidate.candidate_id == candidate_id).first()
        if candidate:
            old_stage_id = candidate.current_stage_id
            candidate.current_stage_id = stage_id
            
            # Создаем запись о действии
            action = CandidateAction(
                candidate_id=candidate_id,
                stage_id=stage_id,
                action_type='note',
                notes=notes or f'Moved from stage {old_stage_id} to stage {stage_id}',
                completed=True
            )
            db.add(action)
            db.commit()
            return True
        return False


class CustomValueCRUD:
    """CRUD операции для пользовательских значений"""
    
    @staticmethod
    def add_custom_industry(db: Session, company_id: int, industry_name: str, creator_id: int) -> CustomValue:
        """Добавление пользовательской отрасли"""
        custom_value = CustomValue(
            company_id=company_id,
            type='industry',
            value=industry_name,
            created_by=creator_id
        )
        db.add(custom_value)
        db.commit()
        db.refresh(custom_value)
        return custom_value
    
    @staticmethod
    def add_custom_competency(db: Session, company_id: int, competency_name: str, creator_id: int) -> CustomValue:
        """Добавление пользовательской компетенции"""
        custom_value = CustomValue(
            company_id=company_id,
            type='competency',
            value=competency_name,
            created_by=creator_id
        )
        db.add(custom_value)
        db.commit()
        db.refresh(custom_value)
        return custom_value
    
    @staticmethod
    def get_by_company_and_type(db: Session, company_id: int, value_type: str) -> List[CustomValue]:
        """Получение пользовательских значений по типу"""
        return db.query(CustomValue).filter(
            and_(CustomValue.company_id == company_id, CustomValue.type == value_type)
        ).all()


class CompanyAnalyticsCRUD:
    """Аналитические запросы для компаний"""
    
    @staticmethod
    def get_company_statistics(db: Session, company_id: int) -> Dict[str, Any]:
        """Статистика по компании"""
        company = db.query(Company).filter(Company.company_id == company_id).first()
        if not company:
            return None
        
        # Количество активных вакансий
        active_jobs = db.query(Job).filter(
            and_(Job.company_id == company_id, Job.is_active == True)
        ).count()
        
        # Количество всех вакансий
        total_jobs = db.query(Job).filter(Job.company_id == company_id).count()
        
        # Количество кандидатов
        candidates_count = db.query(JobCandidate).join(Job).filter(
            Job.company_id == company_id
        ).count()
        
        # Статистика по статусам кандидатов
        candidate_status_stats = db.query(
            JobCandidate.status,
            func.count(JobCandidate.candidate_id).label('count')
        ).join(Job).filter(Job.company_id == company_id).group_by(
            JobCandidate.status
        ).all()
        
        return {
            'company': {
                'id': company.company_id,
                'name': company.name,
                'website': company.website
            },
            'jobs': {
                'active': active_jobs,
                'total': total_jobs
            },
            'candidates': {
                'total': candidates_count,
                'by_status': [{'status': stat.status, 'count': stat.count} for stat in candidate_status_stats]
            }
        }
    
    @staticmethod
    def get_hiring_funnel(db: Session, company_id: int) -> List[Dict[str, Any]]:
        """Воронка найма по этапам"""
        stages = db.query(HiringStage).filter(
            HiringStage.company_id == company_id
        ).order_by(HiringStage.position).all()
        
        funnel = []
        for stage in stages:
            candidate_count = db.query(JobCandidate).join(Job).filter(
                and_(
                    Job.company_id == company_id,
                    JobCandidate.current_stage_id == stage.stage_id
                )
            ).count()
            
            funnel.append({
                'stage_id': stage.stage_id,
                'stage_name': stage.name,
                'position': stage.position,
                'candidate_count': candidate_count
            })
        
        return funnel
