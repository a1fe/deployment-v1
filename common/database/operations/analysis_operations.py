"""
CRUD операции для результатов анализа BGE Reranker

Операции для сохранения, поиска и управления результатами анализа
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import desc, asc, and_, or_, func
from uuid import UUID

from .base_crud import BaseCRUD
from common.models.analysis_results import RerankerAnalysisResult, RerankerAnalysisSession
from common.database.operations.candidate_operations import SubmissionCRUD
from common.database.operations.company_operations import JobCRUD


class RerankerAnalysisResultCRUD(BaseCRUD):
    """CRUD операции для результатов анализа BGE Reranker"""
    
    def __init__(self):
        super().__init__(RerankerAnalysisResult)
    
    def create_from_enhanced_search(
        self,
        db: Session,
        job_id: int,
        enhanced_matches: List[Dict[str, Any]],
        search_params: Dict[str, Any],
        workflow_stats: Dict[str, Any],
        reranker_model: str,
        session_uuid: Optional[str] = None
    ) -> List[RerankerAnalysisResult]:
        """
        Создание записей анализа из результатов enhanced search
        
        Args:
            db: Сессия базы данных
            job_id: ID вакансии
            enhanced_matches: Список результатов enhanced search
            search_params: Параметры поиска (top_k, min_similarity, min_rerank_score)
            workflow_stats: Статистика выполнения workflow
            reranker_model: Название используемой модели reranker
            session_uuid: UUID сессии анализа (опционально)
            
        Returns:
            Список созданных записей анализа
        """
        if not enhanced_matches:
            return []
        
        # Получаем информацию о вакансии
        job = JobCRUD.get_by_id(db, job_id)
        if not job:
            raise ValueError(f"Вакансия не найдена: {job_id}")
        
        results = []
        for rank, match in enumerate(enhanced_matches, 1):
            submission_id = match.get('submission_id')
            if not submission_id:
                continue
                
            # Получаем информацию о кандидате
            submission = SubmissionCRUD.get_by_id(db, submission_id)
            if not submission or not submission.candidate:
                continue
            
            quality_metrics = match.get('quality_metrics', {})
            
            # Создаем запись анализа
            analysis_data = {
                'job_id': job_id,
                'submission_id': submission_id,
                'original_similarity': quality_metrics.get('original_similarity', 0),
                'rerank_score': quality_metrics.get('rerank_score', 0),
                'final_score': match.get('final_score', 0),
                'score_improvement': quality_metrics.get('score_improvement', 0),
                'rank_position': rank,
                'search_params': search_params,
                'reranker_model': reranker_model,
                'workflow_stats': workflow_stats,
                'job_title': job.title,
                'company_id': job.company_id,
                'candidate_name': f"{submission.candidate.first_name} {submission.candidate.last_name}",
                'candidate_email': submission.candidate.email,
                'total_candidates_found': len(enhanced_matches),
                'analysis_type': 'enhanced_resume_search',
                'quality_metrics': quality_metrics
            }
            
            result = self.create(db, analysis_data)
            results.append(result)
        
        return results
    
    def get_by_job(
        self,
        db: Session,
        job_id: int,
        limit: Optional[int] = None,
        order_by_rank: bool = True
    ) -> List[RerankerAnalysisResult]:
        """Получение результатов анализа по вакансии"""
        query = db.query(self.model).filter(
            self.model.job_id == job_id
        )
        
        if order_by_rank:
            query = query.order_by(asc(self.model.rank_position))
        else:
            query = query.order_by(desc(self.model.processed_at))
        
        if limit:
            query = query.limit(limit)
            
        return query.all()
    
    def get_by_submission(
        self,
        db: Session,
        submission_id: UUID,
        limit: Optional[int] = None
    ) -> List[RerankerAnalysisResult]:
        """Получение результатов анализа по заявке кандидата"""
        query = db.query(self.model).filter(
            self.model.submission_id == submission_id
        ).order_by(desc(self.model.processed_at))
        
        if limit:
            query = query.limit(limit)
            
        return query.all()
    
    def get_latest_by_job(
        self,
        db: Session,
        job_id: int,
        limit: Optional[int] = 20
    ) -> List[RerankerAnalysisResult]:
        """Получение последних результатов анализа для вакансии"""
        subquery = db.query(
            func.max(self.model.processed_at).label('latest_processed')
        ).filter(
            self.model.job_id == job_id
        ).scalar_subquery()
        
        query = db.query(self.model).filter(
            and_(
                self.model.job_id == job_id,
                self.model.processed_at == subquery
            )
        ).order_by(asc(self.model.rank_position))
        
        if limit:
            query = query.limit(limit)
            
        return query.all()
    
    def get_top_candidates_for_job(
        self,
        db: Session,
        job_id: int,
        min_rerank_score: Optional[float] = None,
        limit: int = 10
    ) -> List[RerankerAnalysisResult]:
        """Получение топ-кандидатов для вакансии по rerank score"""
        query = db.query(self.model).filter(
            self.model.job_id == job_id
        )
        
        if min_rerank_score is not None:
            query = query.filter(self.model.rerank_score >= min_rerank_score)
        
        return query.order_by(
            desc(self.model.rerank_score),
            asc(self.model.rank_position)
        ).limit(limit).all()
    
    def get_analytics_by_job(
        self,
        db: Session,
        job_id: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Получение аналитики по вакансии"""
        query = db.query(self.model).filter(self.model.job_id == job_id)
        
        if start_date:
            query = query.filter(self.model.processed_at >= start_date)
        if end_date:
            query = query.filter(self.model.processed_at <= end_date)
        
        results = query.all()
        
        if not results:
            return {
                'total_analyses': 0,
                'avg_rerank_score': 0,
                'avg_improvement': 0,
                'top_score': 0,
                'analyses_count_by_date': {}
            }
        
        # Базовая статистика
        rerank_scores = [float(r.rerank_score) for r in results]
        improvements = [float(r.score_improvement) for r in results]
        
        # Группировка по датам
        analyses_by_date = {}
        for result in results:
            date_key = result.processed_at.date().isoformat()
            analyses_by_date[date_key] = analyses_by_date.get(date_key, 0) + 1
        
        return {
            'total_analyses': len(results),
            'avg_rerank_score': sum(rerank_scores) / len(rerank_scores),
            'avg_improvement': sum(improvements) / len(improvements),
            'top_score': max(rerank_scores),
            'analyses_count_by_date': analyses_by_date,
            'latest_analysis': results[-1].processed_at.isoformat() if results else None
        }


class RerankerAnalysisSessionCRUD(BaseCRUD):
    """CRUD операции для сессий анализа BGE Reranker"""
    
    def __init__(self):
        super().__init__(RerankerAnalysisSession)
    
    def create_session(
        self,
        db: Session,
        job_id: int,
        company_id: int,
        search_params: Dict[str, Any],
        reranker_model: str,
        session_stats: Dict[str, Any],
        total_results: int,
        started_at: datetime,
        completed_at: Optional[datetime] = None
    ) -> RerankerAnalysisSession:
        """Создание новой сессии анализа"""
        session_data = {
            'job_id': job_id,
            'company_id': company_id,
            'total_results': total_results,
            'search_params': search_params,
            'reranker_model': reranker_model,
            'session_stats': session_stats,
            'started_at': started_at,
            'completed_at': completed_at or datetime.utcnow()
        }
        
        return self.create(db, session_data)
    
    def get_recent_sessions(
        self,
        db: Session,
        limit: int = 20,
        job_id: Optional[int] = None
    ) -> List[RerankerAnalysisSession]:
        """Получение последних сессий анализа"""
        query = db.query(self.model)
        
        if job_id:
            query = query.filter(self.model.job_id == job_id)
        
        return query.order_by(
            desc(self.model.completed_at)
        ).limit(limit).all()


# Создаем экземпляры CRUD
reranker_analysis_result_crud = RerankerAnalysisResultCRUD()
reranker_analysis_session_crud = RerankerAnalysisSessionCRUD()
