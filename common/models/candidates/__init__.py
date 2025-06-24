"""
Модели кандидатов - инициализация пакета
"""

from .candidate import Candidate
from .submission import Submission
from .salary_expectation import SalaryExpectation
from .address import Address
from .education import Education
from .education_field import EducationField, parse_field_of_study_string, validate_field_name
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
    'EducationField',
    'parse_field_of_study_string',
    'validate_field_name',
    'submission_competencies',
    'submission_roles',
    'submission_industries',
    'submission_locations'
]
