"""
Models package - модульная архитектура для HR Analysis system
"""

# Базовые элементы
from .base import Base
from .utils import (
    TimestampMixin, SerializationMixin, ValidationMixin,
    ModelConstraints, generate_uuid, safe_str_convert,
    safe_email_validate, format_phone_number
)

# Справочники (сохраняем обратную совместимость)
from .dictionaries import Industry, Competency, Role, Location

# Модели кандидатов
from .candidates import (
    Candidate, Submission, SalaryExpectation, Address, Education,
    submission_competencies, submission_roles, submission_industries, submission_locations
)

# Модели анализа BGE Reranker
from .analysis_results import (
    RerankerAnalysisResult, RerankerAnalysisSession
)

# Модели компаний
from .companies import (
    Company, CompanyContact, CompanyIndustry, Job, JobCompetency,
    HiringStage, JobCandidate, CandidateAction, CustomValue
)

# Эмбеддинги (сохраняем обратную совместимость)
from .embeddings import EmbeddingMetadata

__all__ = [
    # Базовые
    'Base',
    'TimestampMixin',
    'SerializationMixin', 
    'ValidationMixin',
    'ModelConstraints',
    'generate_uuid',
    'safe_str_convert',
    'safe_email_validate',
    'format_phone_number',
    
    # Справочники
    'Industry',
    'Competency', 
    'Role',
    'Location',
    
    # Кандидаты
    'Candidate',
    'Submission',
    'SalaryExpectation',
    'Address',
    'Education',
    'submission_competencies',
    'submission_roles', 
    'submission_industries',
    'submission_locations',
    
    # Компании
    'Company',
    'CompanyContact',
    'CompanyIndustry',
    'Job',
    'JobCompetency',
    'HiringStage',
    'JobCandidate',
    'CandidateAction',
    'CustomValue',
    
    # Эмбеддинги
    'EmbeddingMetadata'
]
