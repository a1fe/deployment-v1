"""
Модели кандидатов - инициализация пакета
"""

from .candidate import Candidate
from .submission import Submission
from .salary_expectation import SalaryExpectation
from .address import Address
from .education import Education
from .associations import (
    submission_competencies, submission_roles, 
    submission_industries, submission_locations
)

__all__ = [
    'Candidate',
    'Submission',
    'SalaryExpectation',
    'Address',
    'Education',
    'submission_competencies',
    'submission_roles',
    'submission_industries',
    'submission_locations'
]
