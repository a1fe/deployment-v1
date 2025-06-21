"""
Модели для хранения результатов анализа BGE Reranker

Сохранение детальных результатов сравнения резюме и вакансий после BGE rerank
"""

from .base import *
from sqlalchemy import JSON, DECIMAL, Index, ForeignKey, UniqueConstraint, CheckConstraint
from sqlalchemy.orm import relationship


class RerankerAnalysisResult(Base):
    """
    Модель для хранения результатов анализа BGE Reranker
    
    Сохраняет детальные результаты сравнения резюме и вакансий
    после выполнения enhanced search с BGE reranker scoring
    """
    __tablename__ = 'reranker_analysis_results'
    
    # Первичный ключ
    analysis_id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Основные связи
    job_id = Column(Integer, ForeignKey('jobs.job_id', ondelete='CASCADE'), nullable=False)
    submission_id = Column(UUID(as_uuid=True), ForeignKey('submissions.submission_id', ondelete='CASCADE'), nullable=False)
    
    # Метрики качества
    original_similarity = Column(DECIMAL(10, 6), nullable=False)
    rerank_score = Column(DECIMAL(10, 6), nullable=False)
    final_score = Column(DECIMAL(10, 6), nullable=False)
    score_improvement = Column(DECIMAL(10, 6), nullable=False)
    rank_position = Column(Integer, nullable=False)
    
    # Параметры анализа
    search_params = Column(JSON, nullable=False)  # top_k, min_similarity, min_rerank_score
    reranker_model = Column(String(100), nullable=False)
    
    # Статистика workflow
    workflow_stats = Column(JSON, nullable=False)  # comprehensive_statistics
    
    # Контекст поиска
    job_title = Column(String(255), nullable=False)
    company_id = Column(Integer, ForeignKey('companies.company_id', ondelete='CASCADE'), nullable=False)
    candidate_name = Column(String(255), nullable=False)
    candidate_email = Column(String(255), nullable=False)
    
    # Метаданные
    total_candidates_found = Column(Integer, nullable=False)
    analysis_type = Column(String(50), nullable=False, default='enhanced_resume_search')
    processed_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp())
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp())
    
    # Дополнительные метрики (опциональные)
    quality_metrics = Column(JSON)  # Дополнительные метрики качества
    
    # Constraints
    __table_args__ = (
        # Уникальность результата для конкретной пары job-submission в рамках одного анализа
        UniqueConstraint('job_id', 'submission_id', 'processed_at', name='uq_analysis_job_submission_time'),
        
        # Проверки валидности данных
        CheckConstraint('original_similarity >= 0 AND original_similarity <= 1', name='ck_original_similarity_range'),
        CheckConstraint('rank_position > 0', name='ck_rank_position_positive'),
        CheckConstraint('total_candidates_found >= 0', name='ck_total_candidates_positive'),
        CheckConstraint('final_score >= 0', name='ck_final_score_positive'),
        
        # Индексы для быстрого поиска
        Index('idx_reranker_analysis_job_id', 'job_id'),
        Index('idx_reranker_analysis_submission_id', 'submission_id'),
        Index('idx_reranker_analysis_company_id', 'company_id'),
        Index('idx_reranker_analysis_processed_at', 'processed_at'),
        Index('idx_reranker_analysis_rerank_score', 'rerank_score'),
        Index('idx_reranker_analysis_rank_position', 'rank_position'),
        Index('idx_reranker_analysis_job_processed', 'job_id', 'processed_at'),
        
        # Составной индекс для аналитики по качеству
        Index('idx_reranker_analysis_quality', 'rerank_score', 'final_score', 'rank_position'),
    )
    
    # Relationships
    job = relationship("Job", lazy="select")
    company = relationship("Company", lazy="select")
    submission = relationship("Submission", lazy="select")
    
    def __repr__(self):
        return (f"<RerankerAnalysisResult(id={self.analysis_id}, "
                f"job_id={self.job_id}, submission_id={str(self.submission_id)[:8]}..., "
                f"rerank_score={self.rerank_score}, rank={self.rank_position})>")
    
    def to_summary_dict(self) -> dict:
        """Краткая сводка результата анализа"""
        return {
            'analysis_id': self.analysis_id,
            'job_id': self.job_id,
            'job_title': self.job_title,
            'company_id': self.company_id,
            'submission_id': str(self.submission_id),
            'candidate_name': self.candidate_name,
            'metrics': {
                'original_similarity': float(self.original_similarity) if self.original_similarity is not None else 0.0,
                'rerank_score': float(self.rerank_score) if self.rerank_score is not None else 0.0,
                'final_score': float(self.final_score) if self.final_score is not None else 0.0,
                'score_improvement': float(self.score_improvement) if self.score_improvement is not None else 0.0,
                'rank_position': self.rank_position
            },
            'analysis_type': self.analysis_type,
            'reranker_model': self.reranker_model,
            'processed_at': self.processed_at.isoformat() if hasattr(self.processed_at, 'isoformat') else None
        }
    
    def to_detailed_dict(self) -> dict:
        """Детальная информация результата анализа"""
        summary = self.to_summary_dict()
        summary.update({
            'search_params': self.search_params,
            'workflow_stats': self.workflow_stats,
            'quality_metrics': self.quality_metrics,
            'total_candidates_found': self.total_candidates_found,
            'candidate_email': self.candidate_email,
            'created_at': self.created_at.isoformat() if hasattr(self.created_at, 'isoformat') else None
        })
        return summary


class RerankerAnalysisSession(Base):
    """
    Модель для группировки результатов анализа в сессии
    
    Позволяет группировать множественные результаты одного enhanced search
    """
    __tablename__ = 'reranker_analysis_sessions'
    
    session_id = Column(Integer, primary_key=True, autoincrement=True)
    session_uuid = Column(UUID(as_uuid=True), nullable=False, unique=True, default=uuid.uuid4)
    
    # Основная информация
    job_id = Column(Integer, ForeignKey('jobs.job_id', ondelete='CASCADE'), nullable=False)
    company_id = Column(Integer, ForeignKey('companies.company_id', ondelete='CASCADE'), nullable=False)
    
    # Метаданные сессии
    total_results = Column(Integer, nullable=False, default=0)
    search_params = Column(JSON, nullable=False)
    reranker_model = Column(String(100), nullable=False)
    
    # Статистика сессии
    session_stats = Column(JSON, nullable=False)  # Полная статистика workflow
    
    # Временные метки
    started_at = Column(TIMESTAMP(timezone=True), nullable=False)
    completed_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp())
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp())
    
    # Constraints
    __table_args__ = (
        Index('idx_analysis_session_job_id', 'job_id'),
        Index('idx_analysis_session_uuid', 'session_uuid'),
        Index('idx_analysis_session_completed', 'completed_at'),
        Index('idx_analysis_session_job_completed', 'job_id', 'completed_at'),
    )
    
    # Relationships
    job = relationship("Job")
    company = relationship("Company")
    
    def __repr__(self):
        return (f"<RerankerAnalysisSession(id={self.session_id}, "
                f"uuid={str(self.session_uuid)[:8]}..., job_id={self.job_id}, "
                f"total_results={self.total_results})>")
