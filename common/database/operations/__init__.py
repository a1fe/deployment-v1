"""
CRUD operations package for HR Analysis system
"""

from .candidate_operations import (
    CandidateCRUD,
    SubmissionCRUD, 
    SalaryExpectationCRUD,
    CompetencyCRUD,
    RoleCRUD,
    IndustryCRUD,
    LocationCRUD,
    AnalyticsCRUD
)
from .company_operations import (
    CompanyCRUD,
    CompanyContactCRUD,
    JobCRUD,
    HiringStageCRUD,
    JobCandidateCRUD,
    CustomValueCRUD,
    CompanyAnalyticsCRUD
)

__all__ = [
    # Кандидаты
    'CandidateCRUD',
    'SubmissionCRUD',
    'SalaryExpectationCRUD', 
    'CompetencyCRUD',
    'RoleCRUD',
    'IndustryCRUD',
    'LocationCRUD',
    'AnalyticsCRUD',
    # Компании
    'CompanyCRUD',
    'CompanyContactCRUD',
    'JobCRUD',
    'HiringStageCRUD',
    'JobCandidateCRUD',
    'CustomValueCRUD',
    'CompanyAnalyticsCRUD'
]
