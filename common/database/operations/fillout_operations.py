"""
CRUD операции для работы с данными Fillout
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func, desc

from database.operations.base_crud import BaseCRUD
from common.models.fillout import FilloutSubmission, FilloutCVData, FilloutCompanyData, FilloutFile, FilloutProcessingLog


class FilloutSubmissionCRUD(BaseCRUD):
    """CRUD операции для заявок Fillout"""
    
    def __init__(self):
        super().__init__(FilloutSubmission)
    
    # Создаем экземпляр для использования базовых методов
    _instance = None
    
    @classmethod
    def _get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    @staticmethod
    def create(db: Session, submission_data: dict) -> FilloutSubmission:
        """Создание новой заявки"""
        return FilloutSubmissionCRUD._get_instance().create(db, submission_data)
    
    @staticmethod
    def get_by_id(db: Session, submission_id: int) -> Optional[FilloutSubmission]:
        """Получение заявки по ID"""
        return FilloutSubmissionCRUD._get_instance().get_by_id(db, submission_id)
    
    @staticmethod
    def get_by_submission_id(db: Session, submission_id: str) -> Optional[FilloutSubmission]:
        """Получение заявки по submission_id"""
        return db.query(FilloutSubmission).filter(
            FilloutSubmission.submission_id == submission_id
        ).first()
    
    @staticmethod
    def get_all(db: Session, skip: int = 0, limit: int = 100) -> List[FilloutSubmission]:
        """Получение всех заявок с пагинацией"""
        return FilloutSubmissionCRUD._get_instance().get_all(db, skip, limit)
    
    @staticmethod
    def update(db: Session, submission_id: int, update_data: dict) -> Optional[FilloutSubmission]:
        """Обновление данных заявки"""
        return FilloutSubmissionCRUD._get_instance().update(db, submission_id, update_data)
    
    @staticmethod
    def delete(db: Session, submission_id: int) -> bool:
        """Удаление заявки"""
        return FilloutSubmissionCRUD._get_instance().delete(db, submission_id)
    
    @staticmethod
    def get_unprocessed(db: Session, limit: int = 100) -> List[FilloutSubmission]:
        """Получение необработанных заявок"""
        return db.query(FilloutSubmission).filter(
            FilloutSubmission.is_processed == False
        ).limit(limit).all()
    
    @staticmethod
    def mark_as_processed(db: Session, submission_id: str) -> bool:
        """Отметить заявку как обработанную"""
        submission = db.query(FilloutSubmission).filter(
            FilloutSubmission.submission_id == submission_id
        ).first()
        
        if submission:
            setattr(submission, 'is_processed', True)
            setattr(submission, 'updated_at', datetime.utcnow())
            db.commit()
            return True
        
        return False


class FilloutCVDataCRUD(BaseCRUD):
    """CRUD операции для CV данных из Fillout"""
    
    def __init__(self):
        super().__init__(FilloutCVData)
    
    _instance = None
    
    @classmethod
    def _get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    @staticmethod
    def create(db: Session, cv_data: dict) -> FilloutCVData:
        """Создание новой записи CV"""
        return FilloutCVDataCRUD._get_instance().create(db, cv_data)
    
    @staticmethod
    def get_by_submission_id(db: Session, submission_id: str) -> Optional[FilloutCVData]:
        """Получение CV данных по submission_id"""
        return db.query(FilloutCVData).filter(
            FilloutCVData.submission_id == submission_id
        ).first()
    
    @staticmethod
    def get_by_email(db: Session, email: str) -> List[FilloutCVData]:
        """Получение CV данных по email"""
        return db.query(FilloutCVData).filter(
            FilloutCVData.email == email
        ).all()
    
    @staticmethod
    def search_by_name(db: Session, name: str) -> List[FilloutCVData]:
        """Поиск кандидатов по имени"""
        search_term = f"%{name}%"
        return db.query(FilloutCVData).filter(
            or_(
                FilloutCVData.first_name.ilike(search_term),
                FilloutCVData.last_name.ilike(search_term)
            )
        ).all()
    
    @staticmethod
    def get_recent(db: Session, days: int = 7, limit: int = 100) -> List[FilloutCVData]:
        """Получение недавних CV заявок"""
        from datetime import timedelta
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        return db.query(FilloutCVData).filter(
            FilloutCVData.created_at >= cutoff_date
        ).order_by(desc(FilloutCVData.created_at)).limit(limit).all()
    
    @staticmethod
    def get_by_competencies(db: Session, competencies: List[str]) -> List[FilloutCVData]:
        """Поиск кандидатов по компетенциям"""
        filters = []
        for comp in competencies:
            filters.append(FilloutCVData.core_competency.ilike(f"%{comp}%"))
        
        return db.query(FilloutCVData).filter(or_(*filters)).all()


class FilloutCompanyDataCRUD(BaseCRUD):
    """CRUD операции для данных компаний из Fillout"""
    
    def __init__(self):
        super().__init__(FilloutCompanyData)
    
    _instance = None
    
    @classmethod
    def _get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    @staticmethod
    def create(db: Session, company_data: dict) -> FilloutCompanyData:
        """Создание новой записи компании"""
        return FilloutCompanyDataCRUD._get_instance().create(db, company_data)
    
    @staticmethod
    def get_by_submission_id(db: Session, submission_id: str) -> Optional[FilloutCompanyData]:
        """Получение данных компании по submission_id"""
        return db.query(FilloutCompanyData).filter(
            FilloutCompanyData.submission_id == submission_id
        ).first()
    
    @staticmethod
    def get_by_email(db: Session, email: str) -> List[FilloutCompanyData]:
        """Получение данных компании по email"""
        return db.query(FilloutCompanyData).filter(
            FilloutCompanyData.email == email
        ).all()
    
    @staticmethod
    def get_by_company_name(db: Session, company_name: str) -> List[FilloutCompanyData]:
        """Получение данных по названию компании"""
        return db.query(FilloutCompanyData).filter(
            FilloutCompanyData.company.ilike(f"%{company_name}%")
        ).all()
    
    @staticmethod
    def get_recent(db: Session, days: int = 7, limit: int = 100) -> List[FilloutCompanyData]:
        """Получение недавних заявок компаний"""
        from datetime import timedelta
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        return db.query(FilloutCompanyData).filter(
            FilloutCompanyData.created_at >= cutoff_date
        ).order_by(desc(FilloutCompanyData.created_at)).limit(limit).all()
    
    @staticmethod
    def get_upcoming_meetings(db: Session, limit: int = 50) -> List[FilloutCompanyData]:
        """Получение предстоящих встреч"""
        now = datetime.utcnow()
        
        return db.query(FilloutCompanyData).filter(
            and_(
                FilloutCompanyData.event_start_time > now,
                FilloutCompanyData.google_meeting_link.isnot(None)
            )
        ).order_by(FilloutCompanyData.event_start_time).limit(limit).all()
    
    @staticmethod
    def get_by_industry(db: Session, industry: str) -> List[FilloutCompanyData]:
        """Поиск компаний по отрасли"""
        return db.query(FilloutCompanyData).filter(
            FilloutCompanyData.industry.ilike(f"%{industry}%")
        ).all()


class FilloutProcessingLogCRUD(BaseCRUD):
    """CRUD операции для логов обработки Fillout"""
    
    def __init__(self):
        super().__init__(FilloutProcessingLog)
    
    _instance = None
    
    @classmethod
    def _get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    @staticmethod
    def create_log(db: Session, submission_id: str, task_name: str, stage: str, 
                   status: str, message: Optional[str] = None, error_details: Optional[str] = None,
                   task_id: Optional[str] = None) -> FilloutProcessingLog:
        """Создание записи лога"""
        log_data = {
            'submission_id': submission_id,
            'task_name': task_name,
            'task_id': task_id,
            'stage': stage,
            'status': status,
            'message': message,
            'error_details': error_details,
            'started_at': datetime.utcnow()
        }
        
        if status in ['success', 'error']:
            log_data['completed_at'] = datetime.utcnow()
        
        return FilloutProcessingLogCRUD._get_instance().create(db, log_data)
    
    @staticmethod
    def get_by_submission_id(db: Session, submission_id: str) -> List[FilloutProcessingLog]:
        """Получение всех логов для заявки"""
        return db.query(FilloutProcessingLog).filter(
            FilloutProcessingLog.submission_id == submission_id
        ).order_by(desc(FilloutProcessingLog.created_at)).all()
    
    @staticmethod
    def get_errors(db: Session, limit: int = 100) -> List[FilloutProcessingLog]:
        """Получение записей с ошибками"""
        return db.query(FilloutProcessingLog).filter(
            FilloutProcessingLog.status == 'error'
        ).order_by(desc(FilloutProcessingLog.created_at)).limit(limit).all()
    
    @staticmethod
    def get_processing_stats(db: Session, days: int = 7) -> Dict[str, Any]:
        """Получение статистики обработки за период"""
        from datetime import timedelta
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        total_logs = db.query(FilloutProcessingLog).filter(
            FilloutProcessingLog.created_at >= cutoff_date
        ).count()
        
        success_logs = db.query(FilloutProcessingLog).filter(
            and_(
                FilloutProcessingLog.created_at >= cutoff_date,
                FilloutProcessingLog.status == 'success'
            )
        ).count()
        
        error_logs = db.query(FilloutProcessingLog).filter(
            and_(
                FilloutProcessingLog.created_at >= cutoff_date,
                FilloutProcessingLog.status == 'error'
            )
        ).count()
        
        processing_logs = db.query(FilloutProcessingLog).filter(
            and_(
                FilloutProcessingLog.created_at >= cutoff_date,
                FilloutProcessingLog.status == 'processing'
            )
        ).count()
        
        return {
            'total': total_logs,
            'success': success_logs,
            'error': error_logs,
            'processing': processing_logs,
            'success_rate': (success_logs / total_logs * 100) if total_logs > 0 else 0
        }
