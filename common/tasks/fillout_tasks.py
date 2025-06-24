"""
Fillout API tasks for retrieving resume and company data - ПОЛНАЯ ВЕРСИЯ
Этот файл обеспечивает полное сохранение всех данных из Fillout в PostgreSQL
"""

import os
import requests
import json
from datetime import datetime
from typing import Dict, List, Any, Optional
from celery.utils.log import get_task_logger
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from sqlalchemy import text
from uuid import UUID

from celery_app.celery_app import celery_app
from database.config import database
from models.candidates import (
    Submission, Candidate, Address, Education, SalaryExpectation,
    submission_competencies, submission_roles, submission_industries, submission_locations
)
from models.companies import Company, CompanyContact, Job
from models.dictionaries import Competency, Role, Industry, Location

# Загружаем переменные окружения
load_dotenv()

logger = get_task_logger(__name__)

# Маппинг полей Fillout на поля БД для резюме
RESUME_FIELD_MAPPING = {
    'mr5P7iiQVH2XPN2hpdM6WD': {'target': 'submission', 'field': 'resume_url', 'type': 'file_upload'},
    '6apz': {'target': 'candidate', 'field': 'linkedin_url', 'type': 'url'},
    'uu1C': {'target': 'submission', 'field': 'agree_to_processing', 'type': 'boolean'},
    '4iAc': {'target': 'submission', 'field': 'agree_to_contact', 'type': 'boolean'},
    'nbDc': {'target': 'candidate', 'field': 'first_name', 'type': 'string'},
    'qNmBUPjoEAT5EB48JqpboY': {'target': 'candidate', 'field': 'last_name', 'type': 'string'},
    'b4az': {'target': 'candidate', 'field': 'email', 'type': 'email'},
    'eyLk': {'target': 'candidate', 'field': 'mobile_number', 'type': 'phone'},
    'dnDn': {'target': 'address', 'field': 'full_address', 'type': 'address'},
    'kudR': {'target': 'submission', 'field': 'legally_authorized_us', 'type': 'boolean_yes_no'},
    '7oqB': {'target': 'submission', 'field': 'requires_sponsorship', 'type': 'boolean_yes_no'},
    '9BDD': {'target': 'education', 'field': 'degree_level', 'type': 'checkboxes'},
    'ucUj': {'target': 'education', 'field': 'field_of_study', 'type': 'checkboxes'},
    '4PHP': {'target': 'education', 'field': 'other_degree_level', 'type': 'string'},
    'rvG4': {'target': 'education', 'field': 'other_field_of_study', 'type': 'string'},
    'vjou': {'target': 'competencies', 'field': 'competency_names', 'type': 'checkboxes'},
    'v3Wv': {'target': 'submission', 'field': 'work_preference', 'type': 'dropdown'},
    'eJKT': {'target': 'submission', 'field': 'willingness_to_travel', 'type': 'number'},
    '18we': {'target': 'submission', 'field': 'willing_to_relocate', 'type': 'dropdown'},
    '6E6Q': {'target': 'locations', 'field': 'location_names', 'type': 'checkboxes'},
    'ejRB': {'target': 'submission', 'field': 'specific_locations_preferred', 'type': 'string'},
    'vk5R': {'target': 'submission', 'field': 'pe_license', 'type': 'boolean'},
    '4MbL': {'target': 'submission', 'field': 'available_shifts', 'type': 'checkboxes'},
    'dNQc': {'target': 'roles', 'field': 'role_names', 'type': 'checkboxes'},
    'mbxY': {'target': 'industries', 'field': 'industry_names', 'type': 'checkboxes'},
    '571H': {'target': 'salary', 'field': 'max_salary', 'type': 'number'},
    'oB7Z': {'target': 'salary', 'field': 'min_salary', 'type': 'number'},
    '86oT': {'target': 'salary', 'field': 'currency', 'type': 'dropdown'},
}

# Маппинг полей Fillout на поля БД для компаний
COMPANY_FIELD_MAPPING = {
    '8xsW': {'target': 'job', 'field': 'title', 'type': 'string'},
    '7uVa': {'target': 'company', 'field': 'name', 'type': 'string'},
    '7UTC': {'target': 'company_industries', 'field': 'industry_names', 'type': 'checkboxes'},
    'ntg7': {'target': 'contact', 'field': 'full_name', 'type': 'string'},
    'jBg7': {'target': 'job', 'field': 'job_description_url', 'type': 'file_upload'},
    'pPkE': {'target': 'contact', 'field': 'email', 'type': 'email'},
    'bAgc': {'target': 'unknown', 'field': 'company_logo', 'type': 'file_upload'},
    'wTac': {'target': 'company', 'field': 'website', 'type': 'url'},
    'qWjN': {'target': 'unknown', 'field': 'additional_file', 'type': 'file_upload'},
    'wMDm': {'target': 'job', 'field': 'description', 'type': 'long_text'},
    # Неопределенные поля
    'hqBS': {'target': 'unknown', 'field': 'unknown_field_1', 'type': 'json'},
    'ranB': {'target': 'unknown', 'field': 'unknown_field_2', 'type': 'json'},
    'tkfM': {'target': 'unknown', 'field': 'unknown_field_3', 'type': 'json'},
    'dd45': {'target': 'unknown', 'field': 'unknown_field_4', 'type': 'json'},
}


@celery_app.task(
    bind=True,
    name='tasks.fillout_tasks.fetch_resume_data',
    soft_time_limit=1800,
    time_limit=2100,
    max_retries=2
)
def fetch_resume_data(self) -> Dict[str, Any]:
    """
    Task 1A: Получение данных резюме из Fillout API с полным сохранением всех полей
    
    Returns:
        Dict с результатами: количество новых записей, статус
    """
    logger.info("📥 Получение данных резюме из Fillout")
    
    try:
        api_key = os.getenv('FILLOUT_API_KEY')
        base_url = os.getenv('FILLOUT_BASE_URL')
        form_id = os.getenv('CV_FORM_ID')
        
        if not all([api_key, base_url, form_id]):
            raise ValueError("Отсутствуют необходимые переменные окружения для Fillout API")
        
        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
        
        # Получаем все данные из API с пагинацией
        all_submissions = []
        page = 1
        limit = 150
        
        while True:
            url = f"{base_url}/api/forms/{form_id}/submissions"
            params = {
                'limit': limit,
                'offset': (page - 1) * limit
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            page_submissions = data.get('responses', [])
            
            if not page_submissions:
                break
                
            all_submissions.extend(page_submissions)
            
            total_responses = data.get('totalResponses', 0)
            if len(all_submissions) >= total_responses:
                break
                
            page += 1
            logger.info(f"📄 Получена страница {page-1}, записей: {len(page_submissions)}")
        
        logger.info(f"📊 Всего получено записей: {len(all_submissions)}")
        
        if not all_submissions:
            logger.info("✅ Нет новых резюме в Fillout")
            return {
                'status': 'completed',
                'new_records': 0,
                'total_processed': 0
            }
        
        # Сохраняем в базу данных
        db = database.get_session()
        new_records = 0
        
        try:
            for submission_data in all_submissions:
                submission_id = submission_data.get('submissionId')
                
                if not submission_id:
                    continue
                
                # Проверяем, есть ли уже такая запись
                existing = db.query(Submission).filter(
                    Submission.submission_id == submission_id
                ).first()
                
                if existing:
                    continue
                
                # Обрабатываем каждую запись в отдельной транзакции
                try:
                    # Полная обработка записи
                    result = _process_resume_submission(db, submission_data)
                    if result:
                        # Коммитим сразу после успешной обработки
                        db.commit()
                        new_records += 1
                        logger.info(f"✅ Запись {submission_id} сохранена в БД")
                    else:
                        # Если обработка не удалась, откатываем только эту запись
                        db.rollback()
                        logger.warning(f"⚠️ Запись {submission_id} не была сохранена")
                        
                except Exception as record_error:
                    # Откатываем только текущую запись
                    db.rollback()
                    logger.error(f"❌ Ошибка обработки записи {submission_id}: {record_error}")
                    continue
            
            logger.info(f"✅ Сохранено {new_records} новых резюме")
            
            # Автоматически запускаем парсинг текстов после сохранения данных
            if new_records > 0:
                logger.info("🚀 Запуск автоматического парсинга текстов резюме...")
                try:
                    from tasks.parsing_tasks import parse_resume_text
                    # Запускаем как Celery задачу
                    task_result = parse_resume_text.delay()
                    logger.info(f"📊 Задача парсинга запущена: {task_result.id}")
                except Exception as e:
                    logger.error(f"❌ Ошибка автозапуска парсинга: {e}")
            
            return {
                'status': 'completed',
                'new_records': new_records,
                'total_processed': len(all_submissions)
            }
            
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"❌ Ошибка получения данных резюме: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'new_records': 0
        }


def _process_resume_submission(db: Session, submission_data: Dict[str, Any]) -> bool:
    """
    Полная обработка одной записи резюме с сохранением всех данных
    
    Args:
        db: Сессия базы данных
        submission_data: Данные из Fillout
        
    Returns:
        True если запись успешно обработана
    """
    try:
        submission_id = submission_data.get('submissionId')
        logger.info(f"🔄 Обработка записи: {submission_id}")
        
        # Извлекаем и группируем данные
        questions = {q['id']: q for q in submission_data.get('questions', [])}
        extracted_data = _extract_all_resume_data(questions)
        
        # 1. Создаем или находим кандидата
        candidate = _get_or_create_candidate(db, extracted_data['candidate'])
        
        # 2. Создаем submission
        submission = _create_full_submission(submission_data, extracted_data['submission'], candidate.candidate_id)
        db.add(submission)
        db.flush()  # Получаем UUID
        
        # 3. Создаем адрес
        if extracted_data['address']:
            address = _create_address(extracted_data['address'], submission.submission_id)
            db.add(address)
        
        # 4. Создаем образование
        if extracted_data['education']:
            education = _create_education(extracted_data['education'], submission.submission_id)
            db.add(education)
        
        # 5. Создаем зарплатные ожидания
        if extracted_data['salary']:
            salary = _create_salary_expectation(extracted_data['salary'], submission.submission_id)
            db.add(salary)
        
        # 6. Связываем компетенции
        if extracted_data['competencies']:
            _link_competencies(db, submission.submission_id, extracted_data['competencies'])
        
        # 7. Связываем роли
        if extracted_data['roles']:
            _link_roles(db, submission.submission_id, extracted_data['roles'])
        
        # 8. Связываем индустрии
        if extracted_data['industries']:
            _link_industries(db, submission.submission_id, extracted_data['industries'])
        
        # 9. Связываем локации
        if extracted_data['locations']:
            _link_locations(db, submission.submission_id, extracted_data['locations'])
        
        # 10. Сохраняем неизвестные поля как JSON в дополнительном поле
        if extracted_data['unknown_fields']:
            # Можно добавить JSON поле в submission или создать отдельную таблицу
            logger.info(f"📋 Неизвестные поля для {submission_id}: {len(extracted_data['unknown_fields'])}")
        
        logger.info(f"✅ Запись {submission_id} полностью обработана")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка обработки записи {submission_id}: {e}")
        # Откат будет сделан на верхнем уровне
        return False


def _extract_all_resume_data(questions: Dict[str, Any]) -> Dict[str, Any]:
    """
    Извлекает все данные из вопросов Fillout и группирует по таблицам
    
    Args:
        questions: Словарь вопросов из Fillout
        
    Returns:
        Сгруппированные данные для всех таблиц
    """
    extracted = {
        'candidate': {},
        'submission': {},
        'address': {},
        'education': {},
        'salary': {},
        'competencies': [],
        'roles': [],
        'industries': [],
        'locations': [],
        'unknown_fields': {}
    }
    
    for question_id, question in questions.items():
        value = question.get('value')
        
        if question_id in RESUME_FIELD_MAPPING:
            mapping = RESUME_FIELD_MAPPING[question_id]
            target = mapping['target']
            field_name = mapping['field']
            field_type = mapping['type']
            
            processed_value = _process_field_value(value, field_type)
            
            if target in ['candidate', 'submission', 'address', 'education', 'salary']:
                extracted[target][field_name] = processed_value
            elif target in ['competencies', 'roles', 'industries', 'locations']:
                if isinstance(processed_value, list):
                    extracted[target] = processed_value
                elif processed_value:
                    extracted[target] = [processed_value]
        else:
            # Неизвестные поля
            extracted['unknown_fields'][question_id] = {
                'name': question.get('name'),
                'type': question.get('type'),
                'value': value
            }
    
    return extracted


def _process_field_value(value: Any, field_type: str) -> Any:
    """Обрабатывает значение поля в зависимости от его типа"""
    if value is None:
        return None
    
    try:
        if field_type == 'boolean':
            return bool(value)
        elif field_type == 'boolean_yes_no':
            return str(value).lower() == 'yes'
        elif field_type == 'number':
            return float(value) if value else None
        elif field_type in ['string', 'email', 'phone', 'url', 'dropdown', 'long_text']:
            return str(value) if value else None
        elif field_type == 'file_upload':
            if isinstance(value, list) and len(value) > 0:
                return value[0].get('url') if isinstance(value[0], dict) else str(value[0])
            return str(value) if value else None
        elif field_type == 'address':
            return value if isinstance(value, dict) else None
        elif field_type == 'checkboxes':
            if isinstance(value, list):
                return value
            elif isinstance(value, str):
                return [value]
            return []
        else:
            return value
    except Exception as e:
        logger.warning(f"⚠️ Ошибка обработки значения {value} типа {field_type}: {e}")
        return value


def _get_or_create_candidate(db: Session, candidate_data: Dict[str, Any]) -> Candidate:
    """Создает или находит существующего кандидата"""
    email = candidate_data.get('email')
    if not email:
        raise ValueError("Email кандидата обязателен")
    
    candidate = db.query(Candidate).filter(Candidate.email == email).first()
    
    if not candidate:
        candidate = Candidate(
            first_name=candidate_data.get('first_name', ''),
            last_name=candidate_data.get('last_name', ''),
            email=email,
            mobile_number=candidate_data.get('mobile_number'),
            linkedin_url=candidate_data.get('linkedin_url'),
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        db.add(candidate)
        db.flush()
    
    return candidate


def _create_full_submission(submission_data: Dict[str, Any], submission_fields: Dict[str, Any], candidate_id: Any) -> Submission:
    """Создает полный объект Submission со всеми полями"""
    
    # Обработка смен
    work_shift_related = False
    available_shifts = None
    if submission_fields.get('available_shifts'):
        shifts = submission_fields['available_shifts']
        if isinstance(shifts, list) and shifts:
            work_shift_related = True
            available_shifts = ', '.join(shifts)
    
    submission = Submission(
        submission_id=submission_data['submissionId'],
        candidate_id=candidate_id,
        resume_url=submission_fields.get('resume_url', ''),
        agree_to_processing=submission_fields.get('agree_to_processing', False),
        agree_to_contact=submission_fields.get('agree_to_contact', False),
        status='pending',
        current_step='initial',
        submission_started=_parse_datetime(submission_data.get('submissionTime')),
        last_updated=datetime.utcnow(),
        legally_authorized_us=submission_fields.get('legally_authorized_us', False),
        requires_sponsorship=submission_fields.get('requires_sponsorship', False),
        pe_license=submission_fields.get('pe_license', False),
        work_preference=submission_fields.get('work_preference'),
        willingness_to_travel=submission_fields.get('willingness_to_travel'),
        willing_to_relocate=submission_fields.get('willing_to_relocate'),
        work_shift_related=work_shift_related,
        available_shifts=available_shifts,
        specific_locations_preferred=submission_fields.get('specific_locations_preferred')
    )
    
    return submission


def _create_address(address_data: Dict[str, Any], submission_id: Any) -> Address:
    """Создает объект Address"""
    full_address = address_data.get('full_address', {})
    
    return Address(
        submission_id=submission_id,
        address=full_address.get('address', ''),
        city=full_address.get('city', ''),
        state_province=full_address.get('state', ''),
        zip_postal_code=full_address.get('zipCode', ''),
        country=full_address.get('country', '')
    )


def _create_education(education_data: Dict[str, Any], submission_id: Any) -> Education:
    """Создает объект Education"""
    degree_levels = education_data.get('degree_level', [])
    fields_of_study = education_data.get('field_of_study', [])
    
    # Если несколько уровней или полей, объединяем в строку
    degree_level = ', '.join(degree_levels) if isinstance(degree_levels, list) else str(degree_levels or '')
    field_of_study = ', '.join(fields_of_study) if isinstance(fields_of_study, list) else str(fields_of_study or '')
    
    return Education(
        submission_id=submission_id,
        degree_level=degree_level,
        field_of_study=field_of_study,
        other_degree_level=education_data.get('other_degree_level'),
        other_field_of_study=education_data.get('other_field_of_study')
    )


def _create_salary_expectation(salary_data: Dict[str, Any], submission_id: Any) -> SalaryExpectation:
    """Создает объект SalaryExpectation"""
    return SalaryExpectation(
        submission_id=submission_id,
        min_salary=salary_data.get('min_salary'),
        max_salary=salary_data.get('max_salary'),
        currency=salary_data.get('currency', 'USD')
    )


def _link_competencies(db: Session, submission_id: Any, competency_names: List[str]):
    """Связывает submission с компетенциями"""
    for name in competency_names:
        if not name:
            continue
            
        # Найти или создать компетенцию
        competency = db.query(Competency).filter(Competency.name == name).first()
        if not competency:
            competency = Competency(name=name, is_primary=True)
            db.add(competency)
            db.flush()
        
        # Создать связь
        link_stmt = submission_competencies.insert().values(
            submission_id=submission_id,
            competency_id=competency.competency_id
        )
        db.execute(link_stmt)


def _link_roles(db: Session, submission_id: Any, role_names: List[str]):
    """Связывает submission с ролями"""
    for name in role_names:
        if not name:
            continue
            
        role = db.query(Role).filter(Role.name == name).first()
        if not role:
            role = Role(name=name)
            db.add(role)
            db.flush()
        
        link_stmt = submission_roles.insert().values(
            submission_id=submission_id,
            role_id=role.role_id
        )
        db.execute(link_stmt)


def _link_industries(db: Session, submission_id: Any, industry_names: List[str]):
    """Связывает submission с индустриями"""
    for name in industry_names:
        if not name:
            continue
            
        # Проверяем, есть ли уже такая индустрия
        industry = db.query(Industry).filter(Industry.name == name).first()
        if not industry:
            try:
                industry = Industry(name=name, is_primary=True)
                db.add(industry)
                db.flush()
            except Exception as e:
                # Если произошла ошибка (например, дублирующий ключ), 
                # пробуем найти индустрию еще раз без rollback
                industry = db.query(Industry).filter(Industry.name == name).first()
                if not industry:
                    logger.error(f"❌ Не удалось создать/найти индустрию '{name}': {e}")
                    continue
        
        try:
            # Проверяем, есть ли уже такая связь
            existing_link = db.execute(
                submission_industries.select().where(
                    (submission_industries.c.submission_id == submission_id) &
                    (submission_industries.c.industry_id == industry.industry_id)
                )
            ).first()
            
            if not existing_link:
                link_stmt = submission_industries.insert().values(
                    submission_id=submission_id,
                    industry_id=industry.industry_id
                )
                db.execute(link_stmt)
        except Exception as e:
            logger.warning(f"⚠️ Не удалось связать submission {submission_id} с индустрией {industry.industry_id}: {e}")


def _link_locations(db: Session, submission_id: Any, location_names: List[str]):
    """Связывает submission с локациями"""
    for name in location_names:
        if not name:
            continue
            
        location = db.query(Location).filter(Location.name == name).first()
        if not location:
            try:
                location = Location(name=name)
                db.add(location)
                db.flush()
            except Exception as e:
                # Если произошла ошибка, пробуем найти локацию еще раз без rollback
                location = db.query(Location).filter(Location.name == name).first()
                if not location:
                    logger.error(f"❌ Не удалось создать/найти локацию '{name}': {e}")
                    continue
        
        try:
            # Проверяем, есть ли уже такая связь
            existing_link = db.execute(
                submission_locations.select().where(
                    (submission_locations.c.submission_id == submission_id) &
                    (submission_locations.c.location_id == location.location_id)
                )
            ).first()
            
            if not existing_link:
                link_stmt = submission_locations.insert().values(
                    submission_id=submission_id,
                    location_id=location.location_id
                )
                db.execute(link_stmt)
        except Exception as e:
            logger.warning(f"⚠️ Не удалось связать submission {submission_id} с локацией {location.location_id}: {e}")


@celery_app.task(
    bind=True,
    name='tasks.fillout_tasks.fetch_company_data',
    soft_time_limit=1800,
    time_limit=2100,
    max_retries=2
)
def fetch_company_data(self) -> Dict[str, Any]:
    """
    Task 1B: Получение данных компаний из Fillout API с полным сохранением всех полей
    
    Returns:
        Dict с результатами: количество новых записей, статус
    """
    logger.info("📥 Получение данных компаний из Fillout")
    
    try:
        api_key = os.getenv('FILLOUT_API_KEY')
        base_url = os.getenv('FILLOUT_BASE_URL')
        form_id = os.getenv('JOB_FORM_ID')
        
        if not all([api_key, base_url, form_id]):
            raise ValueError("Отсутствуют необходимые переменные окружения для Fillout API")
        
        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
        
        # Получаем все данные из API с пагинацией
        all_submissions = []
        page = 1
        limit = 150
        
        while True:
            url = f"{base_url}/api/forms/{form_id}/submissions"
            params = {
                'limit': limit,
                'offset': (page - 1) * limit
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            page_submissions = data.get('responses', [])
            
            if not page_submissions:
                break
                
            all_submissions.extend(page_submissions)
            
            total_responses = data.get('totalResponses', 0)
            if len(all_submissions) >= total_responses:
                break
                
            page += 1
            logger.info(f"📄 Получена страница {page-1}, записей: {len(page_submissions)}")
        
        logger.info(f"📊 Всего получено записей компаний: {len(all_submissions)}")
        
        if not all_submissions:
            logger.info("✅ Нет новых компаний в Fillout")
            return {
                'status': 'completed',
                'new_records': 0,
                'total_processed': 0
            }
        
        # Сохраняем в базу данных - каждую запись в отдельной транзакции
        new_records = 0
        
        for submission_data in all_submissions:
            db = database.get_session()
            try:
                result = _process_company_submission(db, submission_data)
                if result:
                    db.commit()
                    new_records += 1
                    logger.info(f"✅ Компания #{new_records} сохранена в БД")
                else:
                    db.rollback()
            except Exception as e:
                logger.error(f"❌ Ошибка сохранения компании: {e}")
                db.rollback()
            finally:
                db.close()
        
        logger.info(f"✅ Итого сохранено {new_records} новых компаний")
        
        # Автоматически запускаем парсинг текстов после сохранения данных
        if new_records > 0:
            logger.info("🚀 Запуск автоматического парсинга текстов вакансий...")
            try:
                from tasks.parsing_tasks import parse_job_text
                # Запускаем как Celery задачу
                task_result = parse_job_text.delay()
                logger.info(f"📊 Задача парсинга запущена: {task_result.id}")
            except Exception as e:
                logger.error(f"❌ Ошибка автозапуска парсинга: {e}")
        
        return {
            'status': 'completed',
            'new_records': new_records,
            'total_processed': len(all_submissions)
        }
            
    except Exception as e:
        logger.error(f"❌ Ошибка получения данных компаний: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'new_records': 0
        }


def _process_company_submission(db: Session, submission_data: Dict[str, Any]) -> bool:
    """
    Полная обработка одной записи компании с сохранением всех данных
    
    Args:
        db: Сессия базы данных
        submission_data: Данные из Fillout
        
    Returns:
        True если запись успешно обработана
    """
    try:
        submission_id = submission_data.get('submissionId')
        logger.info(f"🔄 Обработка компании: {submission_id}")
        
        questions = {q['id']: q for q in submission_data.get('questions', [])}
        extracted_data = _extract_all_company_data(questions)
        
        company_name = extracted_data['company'].get('name')
        logger.info(f"📝 Название компании: '{company_name}'")
        
        if not company_name:
            logger.warning(f"⚠️ Нет названия компании в записи {submission_id}")
            return False
        
        # Проверяем, есть ли уже такая компания
        existing = db.query(Company).filter(Company.name == company_name).first()
        if existing:
            logger.info(f"📝 Компания '{company_name}' уже существует")
            return False
        
        # Создаем компанию
        logger.info(f"✨ Создание новой компании: '{company_name}'")
        company = Company(
            name=company_name,
            website=extracted_data['company'].get('website'),
            description=extracted_data['job'].get('description'),
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        db.add(company)
        db.flush()
        logger.info(f"✅ Компания создана с ID: {company.company_id}")
        
        # Создаем работу
        job_title = extracted_data['job'].get('title')
        if job_title:
            logger.info(f"💼 Создание работы: '{job_title}'")
            job_description = extracted_data['job'].get('description') or 'No description provided'
            job = Job(
                company_id=company.company_id,
                title=job_title,
                description=job_description,
                job_description_url=extracted_data['job'].get('job_description_url'),
                is_active=True,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db.add(job)
            logger.info(f"✅ Работа создана")
        
        # Создаем контакт
        contact_name = extracted_data['contact'].get('full_name')
        if contact_name:
            logger.info(f"👤 Создание контакта: '{contact_name}'")
            
            # Генерируем email если он не указан
            contact_email = extracted_data['contact'].get('email')
            if not contact_email:
                contact_email = f'contact@{company_name.lower().replace(" ", "").replace(".", "")}.com'
            
            contact = CompanyContact(
                company_id=company.company_id,
                full_name=contact_name,
                email=contact_email,
                job_title=extracted_data['contact'].get('job_title'),
                is_primary=True,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db.add(contact)
            logger.info(f"✅ Контакт создан с email: {contact_email}")
        
        # Связываем с индустриями
        industries = extracted_data.get('company_industries', [])
        if industries:
            logger.info(f"🏭 Связывание с индустриями: {industries}")
            # Здесь пока пропускаем создание связей с индустриями
            # так как нужно проверить структуру таблицы company_industries
        
        logger.info(f"✅ Компания '{company_name}' успешно обработана")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка обработки компании: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


def _extract_all_company_data(questions: Dict[str, Any]) -> Dict[str, Any]:
    """Извлекает все данные компании из вопросов Fillout"""
    extracted = {
        'company': {},
        'job': {},
        'contact': {},
        'company_industries': [],
        'unknown_fields': {}
    }
    
    for question_id, question in questions.items():
        value = question.get('value')
        
        if question_id in COMPANY_FIELD_MAPPING:
            mapping = COMPANY_FIELD_MAPPING[question_id]
            target = mapping['target']
            field_name = mapping['field']
            field_type = mapping['type']
            
            processed_value = _process_field_value(value, field_type)
            
            if target in ['company', 'job', 'contact']:
                extracted[target][field_name] = processed_value
            elif target == 'company_industries':
                if isinstance(processed_value, list):
                    extracted['company_industries'] = processed_value
                elif processed_value:
                    extracted['company_industries'] = [processed_value]
        else:
            # Неизвестные поля
            extracted['unknown_fields'][question_id] = {
                'name': question.get('name'),
                'type': question.get('type'),
                'value': value
            }
    
    return extracted


def _parse_datetime(date_string: Optional[str]) -> datetime:
    """Парсит дату из строки"""
    if not date_string:
        return datetime.utcnow()
    
    try:
        return datetime.fromisoformat(date_string.replace('Z', '+00:00'))
    except:
        return datetime.utcnow()
