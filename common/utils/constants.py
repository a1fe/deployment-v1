"""
Константы и базовые значения для системы оценки кандидатов
"""

# Базовые уровни образования (Degree Levels)
DEGREE_LEVELS = [
    'High School Diploma / GED',
    'Associate Degree',
    "Bachelor's Degree",
    "Master's Degree",
    'Doctorate / PhD',
    'MBA / EMBA',
    'Other'
]

# Базовые области изучения (Field of Study)
FIELDS_OF_STUDY = [
    'Electrical Engineering',
    'Software Engineering',
    'Mechanical Engineering',
    'Aerospace Engineering',
    'Civil Engineering',
    'Business Administration (Finance, Marketing, etc.)',
    'Law',
    'Other'
]

# Базовые компетенции (Core Competency)
CORE_COMPETENCIES = [
    'Automation & Robotics',
    'Circuit Design & Analysis',
    'Computer-Aided Engineering (CAE)',
    'Control Systems',
    'Dissolved Gas Analysis',
    'Electromagnetics & EMC/EMI',
    'Embedded Systems',
    'High Voltage',
    'Instrumentation & Measurement',
    'Low Voltage',
    'Marketing & Business Development',
    'Medium Voltage',
    'Partial Discharge',
    'Power Systems',
    'Project & Lifecycle Management',
    'Safety & Regulatory Compliance',
    'Sales',
    'Signal Processing & Communications',
    'Team Lead',
    'Other'
]

# Константы для компаний и вакансий

# Отрасли компаний (Industries)
COMPANY_INDUSTRIES = [
    'Aerospace, Defense & Avionics',
    'Automotive & Electric-Vehicle (EV) Systems',
    'Building Services & Smart-Building Construction',
    'Data Centers & Cloud Infrastructure',
    'Engineering Consulting & EPC Firms',
    'Industrial Automation & Manufacturing Plants',
    'Medical Devices & Healthcare Technology',
    'Oil, Gas & Petro-chemical Facilities',
    'Power Generation & Electric Utilities',
    'Rail, Metro & Transportation Systems',
    'Renewable Energy (Solar, Wind, Battery Storage)',
    'Semiconductor & Electronics Hardware',
    'Telecom & Wireless Infrastructure',
    'Other'
]

# Типы занятости (Employment Types)
EMPLOYMENT_TYPES = [
    'Full-time',
    'Part-time',
    'Contract',
    'Internship',
    'Remote'
]

# Уровни опыта (Experience Levels)
EXPERIENCE_LEVELS = [
    'Entry',
    'Mid',
    'Senior',
    'Executive'
]

# Типы действий с кандидатами (Action Types)
CANDIDATE_ACTION_TYPES = [
    'interview',
    'assessment',
    'offer',
    'rejection',
    'note'
]

# Статусы кандидатов (Candidate Status)
CANDIDATE_STATUSES = [
    'active',
    'rejected',
    'withdrawn',
    'hired'
]

# Стандартные этапы подбора (Default Hiring Stages)
DEFAULT_HIRING_STAGES = [
    {'name': 'Application Review', 'position': 1, 'description': 'Review of submitted applications'},
    {'name': 'Screening Call', 'position': 2, 'description': 'Initial phone/video screening'},
    {'name': 'Technical Interview', 'position': 3, 'description': 'Technical assessment and interview'},
    {'name': 'Final Interview', 'position': 4, 'description': 'Final interview with team/management'},
    {'name': 'Offer Sent', 'position': 5, 'description': 'Job offer sent to candidate'},
    {'name': 'Hired', 'position': 6, 'description': 'Candidate accepted offer and hired'},
]

# Уровни важности компетенций (1-5)
COMPETENCY_IMPORTANCE_LEVELS = {
    1: 'Nice to have',
    2: 'Helpful',
    3: 'Important',
    4: 'Very important',
    5: 'Critical'
}

# Валюты для зарплат
CURRENCIES = [
    'USD',
    'EUR',
    'GBP',
    'CAD',
    'AUD',
    'JPY',
    'CHF',
    'RUB'
]

def get_base_data_summary():
    """Возвращает сводку всех базовых данных"""
    return {
        'degree_levels': len(DEGREE_LEVELS),
        'fields_of_study': len(FIELDS_OF_STUDY),
        'core_competencies': len(CORE_COMPETENCIES),
        'company_industries': len(COMPANY_INDUSTRIES),
        'employment_types': len(EMPLOYMENT_TYPES),
        'experience_levels': len(EXPERIENCE_LEVELS),
        'candidate_statuses': len(CANDIDATE_STATUSES),
        'default_hiring_stages': len(DEFAULT_HIRING_STAGES),
        'currencies': len(CURRENCIES)
    }
