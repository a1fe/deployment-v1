"""
Celery задачи для работы с Fillout API
"""

import os
import csv
import hashlib
import requests
import uuid
import tempfile
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Set
from io import StringIO
from urllib.parse import urlparse

from celery import Celery
from celery.utils.log import get_task_logger
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from database.config import database
from models.candidates import Candidate, Submission, SalaryExpectation, Address, Education
from models.companies import Company, CompanyContact, Job
from models.dictionaries import Competency, Role, Industry, Location

# Создаем экземпляр Celery прямо здесь
# Безопасное подключение к Redis через Secret Manager
from deployment.common.utils.secret_manager import get_redis_url_with_auth
redis_url = get_redis_url_with_auth()
celery_app = Celery('hr_analysis', broker=redis_url, backend=redis_url)
logger = get_task_logger(__name__)

from PyPDF2 import PdfReader
from docx import Document
import docx2txt
import olefile


@celery_app.task(bind=True, name='fillout.pull_fillout_data')
def pull_fillout_data(self):
    """
    Периодическая задача для получения данных из Fillout через CSV export
    Сохраняет данные в существующие таблицы candidates, companies
    """
    try:
        task_id = self.request.id
        print(f"🔄 Запуск задачи pull_fillout_data [ID: {task_id}]")
        
        # Получаем данные кандидатов
        cv_results = _pull_and_save_cv_data()
        
        # Получаем данные компаний
        company_results = _pull_and_save_company_data()
        
        result = {
            'task_id': task_id,
            'timestamp': datetime.utcnow().isoformat(),
            'cv_data': cv_results,
            'company_data': company_results,
            'status': 'completed'
        }
        
        print(f"✅ Задача pull_fillout_data завершена [ID: {task_id}]")
        return result
        
    except Exception as e:
        print(f"❌ Ошибка в задаче pull_fillout_data: {e}")
        return {
            'task_id': self.request.id,
            'timestamp': datetime.utcnow().isoformat(),
            'error': str(e),
            'status': 'failed'
        }


def _pull_and_save_cv_data() -> Dict[str, Any]:
    """Получение и сохранение данных кандидатов из Fillout в существующие таблицы"""
    
    api_key = os.getenv('FILLOUT_API_KEY')
    cv_form_id = os.getenv('CV_FORM_ID')
    
    if not api_key or not cv_form_id:
        raise ValueError("Отсутствуют FILLOUT_API_KEY или CV_FORM_ID в переменных окружения")
    
    try:
        # Вызов Fillout API для получения JSON данных
        url = f"https://api.fillout.com/v1/api/forms/{cv_form_id}/submissions"
        headers = {
            'Authorization': f'Bearer {api_key}',
            'Accept': 'application/json'
        }
        
        # Параметры для получения только новых данных
        db = database.get_session()
        try:
            # Получаем последнюю дату обновления из submissions
            last_update = db.query(Submission.last_updated).order_by(Submission.last_updated.desc()).first()
            if last_update and last_update[0]:
                params = {
                    'after': last_update[0].isoformat()
                }
            else:
                params = {}
                
        finally:
            db.close()
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        # Парсим JSON данные
        json_data = response.json()
        cv_records = _parse_cv_json(json_data)
        
        # Сохраняем данные в существующие таблицы
        saved_count = _save_cv_to_existing_tables(cv_records)
        
        return {
            'records_received': len(cv_records),
            'records_saved': saved_count,
            'status': 'success'
        }
        
    except Exception as e:
        print(f"❌ Ошибка при получении CV данных: {e}")
        return {
            'records_received': 0,
            'records_saved': 0,
            'error': str(e),
            'status': 'error'
        }


def _pull_and_save_company_data() -> Dict[str, Any]:
    """Получение и сохранение данных компаний из Fillout в существующие таблицы"""
    
    api_key = os.getenv('FILLOUT_API_KEY')
    job_form_id = os.getenv('JOB_FORM_ID')
    
    if not api_key or not job_form_id:
        raise ValueError("Отсутствуют FILLOUT_API_KEY или JOB_FORM_ID в переменных окружения")
    
    try:
        url = f"https://api.fillout.com/v1/api/forms/{job_form_id}/submissions"
        headers = {
            'Authorization': f'Bearer {api_key}',
            'Accept': 'application/json'
        }
        
        # Параметры для получения только новых данных
        db = database.get_session()
        try:
            # Получаем последнюю дату обновления из company_contacts
            last_update = db.query(CompanyContact.updated_at).order_by(CompanyContact.updated_at.desc()).first()
            if last_update and last_update[0]:
                params = {
                    'after': last_update[0].isoformat()
                }
            else:
                params = {}
                
        finally:
            db.close()
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        # Парсим JSON данные
        json_data = response.json()
        company_records = _parse_company_json(json_data)
        
        # Сохраняем данные в существующие таблицы
        saved_count = _save_company_to_existing_tables(company_records)
        
        return {
            'records_received': len(company_records),
            'records_saved': saved_count,
            'status': 'success'
        }
        
    except Exception as e:
        print(f"❌ Ошибка при получении данных компаний: {e}")
        return {
            'records_received': 0,
            'records_saved': 0,
            'error': str(e),
            'status': 'error'
        }


def _parse_cv_json(json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Парсинг JSON данных кандидатов из Fillout"""
    
    records = []
    responses = json_data.get('responses', [])
    
    for response in responses:
        try:
            # Основные поля submission
            submission_id = response.get('submissionId')
            submission_time = _parse_datetime(response.get('submissionTime'))
            last_updated = _parse_datetime(response.get('lastUpdatedAt'))
            started_at = _parse_datetime(response.get('startedAt'))
            
            # Создаем словарь для mapping вопросов по названию
            questions_map = {}
            for question in response.get('questions', []):
                name = question.get('name', '')
                value = question.get('value')
                questions_map[name] = value
            
            # Извлекаем адрес как объект
            address_obj = _extract_address_object(questions_map.get('Your Address'))
            
            # Извлекаем данные из вопросов
            record = {
                'submission_id': submission_id,
                'last_updated': last_updated,
                'submission_started': started_at,
                'submission_time': submission_time,
                'status': 'submitted',  # Default status
                
                # Контактная информация
                'first_name': _extract_text_value(questions_map.get('First Name')),
                'last_name': _extract_text_value(questions_map.get('Last Name')),
                'email': _extract_text_value(questions_map.get('Email')),
                'phone': _extract_text_value(questions_map.get('Mobile Number')),
                'linkedin': _extract_text_value(questions_map.get('LinkedIn Profile')),
                
                # Позиция и компания
                'position': _extract_text_value(questions_map.get('What position are you applying for?')),
                'current_company': _extract_text_value(questions_map.get('Current Company')),
                'current_role': _extract_text_value(questions_map.get('Current Role')),
                
                # Локация из адреса
                'address': address_obj.get('address'),
                'city': address_obj.get('city'),
                'state': address_obj.get('state'),
                'country': address_obj.get('country'),
                'zip_code': address_obj.get('zipCode'),
                
                # Опыт работы
                'years_experience': _parse_int(_extract_text_value(questions_map.get('Years of Experience'))),
                
                # Зарплатные ожидания
                'min_salary': _parse_float(_extract_text_value(questions_map.get('Min Salary'))),
                'max_salary': _parse_float(_extract_text_value(questions_map.get('Max Salary'))),
                'salary_currency': _extract_text_value(questions_map.get('Currency')),
                
                # Образование (массивы)
                'education_level': _extract_array_value(questions_map.get('Degree Level')),
                'field_of_study': _extract_array_value(questions_map.get('Field of Study')),
                'other_field_of_study': _extract_text_value(questions_map.get('Other field of Study')),
                'university': _extract_text_value(questions_map.get('University')),
                'graduation_year': _parse_int(_extract_text_value(questions_map.get('Graduation Year'))),
                
                # Компетенции и роли (массивы)
                'core_competency': _extract_array_value(questions_map.get('Core Competency')),
                'preferred_role_type': _extract_array_value(questions_map.get('Preferred Role Type')),
                'preferred_industry': _extract_array_value(questions_map.get('Preferred Industry')),
                'preferred_work_locations': _extract_array_value(questions_map.get('Preferred Work Locations')),
                
                # Рабочие предпочтения
                'work_preference': _extract_text_value(questions_map.get('Work Preference')),
                'willingness_to_travel': _parse_int(_extract_text_value(questions_map.get('Willingness to travel'))),
                'willing_to_relocate': _extract_text_value(questions_map.get('Are you willing to relocate for the right opportunity?')),
                'available_shifts': _extract_text_value(questions_map.get('What shifts are you available to work?')),
                'work_shift_related': _extract_boolean_value(questions_map.get('Is your work shift related?')),
                
                # Разрешения на работу
                'us_work_authorized': _extract_boolean_value(questions_map.get('Are you legally authorized to work in the United States?')),
                'visa_sponsorship_required': _extract_boolean_value(questions_map.get('Will you now or in the future require sponsorship for an employment visa?')),
                'pe_license': _extract_boolean_value(questions_map.get('Do you currently or in the near future hold a U.S. Professional Engineer (PE) license?')),
                
                # Файлы
                'resume_url': _extract_file_url(questions_map.get('For us to get to know you better, please upload your resume here:')),
                
                # Согласия
                'data_processing_agreed': _extract_boolean_value(questions_map.get('I agree to the processing of my resume and personal data for recruitment purposes. This includes the use of automated tools to extract relevant details, sharing data with trusted third parties, and potential international transfers. I confirm that I have read and agree to the Privacy Policy, Terms & Conditions, AI & Compliance Policies, and Legal Notices.')),
                'future_opportunities_agreed': _extract_boolean_value(questions_map.get('I would like to be contacted about future hiring events and job opportunities that match my profile.')),
                
                # Дополнительная информация
                'cover_letter': _extract_text_value(questions_map.get('Cover Letter')),
                'additional_info': _extract_text_value(questions_map.get('Additional Information')),
            }
            
            records.append(record)
            
        except Exception as e:
            print(f"❌ Ошибка при парсинге submission {response.get('submissionId', 'unknown')}: {e}")
            continue
    
    return records


def _extract_text_value(value):
    """Извлекает текстовое значение из различных типов ответов Fillout"""
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip() if value.strip() else None
    if isinstance(value, list) and len(value) > 0:
        return str(value[0]).strip() if str(value[0]).strip() else None
    return str(value).strip() if str(value).strip() else None


def _extract_array_value(value):
    """Извлекает значения из массивов Fillout и объединяет их в строку"""
    if value is None:
        return None
    if isinstance(value, list) and len(value) > 0:
        # Фильтруем пустые значения и объединяем через запятую
        filtered_values = [str(v).strip() for v in value if v and str(v).strip()]
        return ', '.join(filtered_values) if filtered_values else None
    if isinstance(value, str):
        return value.strip() if value.strip() else None
    return str(value).strip() if str(value).strip() else None


def _extract_file_url(value):
    """Извлекает URL файла из объекта Fillout"""
    if value is None:
        return None
    if isinstance(value, list) and len(value) > 0:
        file_obj = value[0]
        if isinstance(file_obj, dict) and 'url' in file_obj:
            return file_obj['url']
    if isinstance(value, dict) and 'url' in value:
        return value['url']
    return None


def _extract_address_object(value):
    """Извлекает данные адреса из объекта Fillout"""
    if value is None:
        return {}
    if isinstance(value, dict):
        return {
            'address': value.get('address', ''),
            'city': value.get('city', ''),
            'state': value.get('state', ''),
            'country': value.get('country', ''),
            'zipCode': value.get('zipCode', '')
        }
    return {}


def _extract_boolean_value(value):
    """Извлекает булевое значение из ответов Fillout"""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ['true', 'yes', '1', 'on']
    if isinstance(value, list) and len(value) > 0:
        first_val = str(value[0]).lower()
        return first_val in ['true', 'yes', '1', 'on']
    return False


def _parse_cv_csv(csv_data: str) -> List[Dict[str, Any]]:
    """Парсинг CSV данных кандидатов"""
    
    records = []
    csv_reader = csv.DictReader(StringIO(csv_data))
    
    for row in csv_reader:
        try:
            # Парсим datetime поля
            last_updated = _parse_datetime(row.get('Last updated'))
            submission_started = _parse_datetime(row.get('Submission started'))
            
            # Парсим зарплатные ожидания
            min_salary = _parse_float(row.get('Min Salary'))
            max_salary = _parse_float(row.get('Max Salary'))
            
            # Парсим boolean поля
            data_processing_agreed = _parse_boolean(row.get('I agree to the processing of my resume and personal data for recruitment purposes. This includes the use of automated tools to extract relevant details, sharing data with trusted third parties, and potential international transfers. I confirm that I have read and agree to the Privacy Policy, Terms & Conditions, AI & Compliance Policies, and Legal Notices.'))
            future_opportunities_agreed = _parse_boolean(row.get('I would like to be contacted about future hiring events and job opportunities that match my profile.'))
            
            record = {
                'submission_id': row.get('Submission ID'),
                'last_updated': last_updated,
                'submission_started': submission_started,
                'status': row.get('Status'),
                'current_step': row.get('Current step'),
                'first_name': row.get('First Name'),
                'last_name': row.get('Last Name'),
                'email': row.get('Email'),
                'mobile_number': row.get('Mobile Number'),
                'linkedin_profile': row.get('LinkedIn Profile'),
                'address': row.get('Address (Your Address)'),
                'city': row.get('City (Your Address)'),
                'state_province': row.get('State/Province (Your Address)'),
                'zip_postal_code': row.get('Zip/Postal code (Your Address)'),
                'country': row.get('Country (Your Address)'),
                'us_work_authorized': row.get('Are you legally authorized to work in the United States?'),
                'visa_sponsorship_required': row.get('Will you now or in the future require sponsorship for an employment visa?'),
                'degree_level': row.get('Degree Level'),
                'field_of_study': row.get('Field of Study'),
                'other_field_of_study': row.get('Other field of Study'),
                'pe_license': row.get('Do you currently or in the near future hold a U.S. Professional Engineer (PE) license?'),
                'core_competency': row.get('Core Competency'),
                'work_preference': row.get('Work Preference'),
                'willingness_to_travel': row.get('Willingness to travel'),
                'willing_to_relocate': row.get('Are you willing to relocate for the right opportunity?'),
                'preferred_work_locations': row.get('Preferred Work Locations'),
                'specific_countries_cities': row.get('Please specify any specific countries or cities you\'d prefer.'),
                'shift_related': row.get('Is your work shift related?'),
                'available_shifts': row.get('What shifts are you available to work?'),
                'preferred_role_type': row.get('Preferred Role Type'),
                'preferred_industry': row.get('Preferred Industry'),
                'min_salary': min_salary,
                'max_salary': max_salary,
                'currency': row.get('Currency'),
                'resume_url': row.get('For us to get to know you better, please upload your resume here:'),
                'data_processing_agreed': data_processing_agreed,
                'future_opportunities_agreed': future_opportunities_agreed,
                'source': row.get('source'),
                'utm_source': row.get('utm_source'),
                'utm_medium': row.get('utm_medium'),
                'utm_campaign': row.get('utm_campaign'),
                'errors': row.get('Errors'),
                'url': row.get('Url'),
                'network_id': row.get('Network ID')
            }
            
            records.append(record)
            
        except Exception as e:
            print(f"⚠️ Ошибка при парсинге CV записи: {e}, данные: {row}")
            continue
    
    return records


def _parse_company_json(json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Парсинг JSON данных компаний из Fillout"""
    
    records = []
    responses = json_data.get('responses', [])
    
    for response in responses:
        try:
            # Основные поля submission
            submission_id = response.get('submissionId')
            submission_time = _parse_datetime(response.get('submissionTime'))
            last_updated = _parse_datetime(response.get('lastUpdatedAt'))
            started_at = _parse_datetime(response.get('startedAt'))
            
            # Создаем словарь для mapping вопросов по названию
            questions_map = {}
            for question in response.get('questions', []):
                name = question.get('name', '')
                value = question.get('value')
                questions_map[name] = value
            
            # Извлекаем данные компании с правильными именами полей
            record = {
                'submission_id': submission_id,
                'last_updated': last_updated,
                'submission_started': started_at,
                'submission_time': submission_time,
                'status': 'submitted',
                
                # Информация о компании (правильные поля из формы)
                'company_name': _extract_text_value(questions_map.get('Your company')),
                'industry': _extract_array_value(questions_map.get('Your industry')),  # Массив
                'company_size': _extract_text_value(questions_map.get('Company Size')),
                'website': _extract_text_value(questions_map.get('Website')),
                'description': _extract_text_value(questions_map.get('Feel free to share any details or context you\'d like us to know in advance.')),
                
                # Информация о вакансии
                'job_title': _extract_text_value(questions_map.get('Main vacancy (required)')),
                'job_description': _extract_text_value(questions_map.get('Job Description')),
                'job_description_url': _extract_file_url(questions_map.get('Upload job description (PDF or Word) (1)')),
                'requirements': _extract_array_value(questions_map.get('What technical competencies should candidates have?')),  # Массив
                'responsibilities': _extract_text_value(questions_map.get('Responsibilities')),
                'employment_type': _extract_text_value(questions_map.get('Employment Type')),
                'experience_level': _extract_text_value(questions_map.get('Experience Level')),
                
                # Дополнительные вакансии
                'second_vacancy': _extract_text_value(questions_map.get('Second vacancy (optional)')),
                'second_vacancy_description_url': _extract_file_url(questions_map.get('Upload job description (PDF or Word) (2)')),
                'third_vacancy': _extract_text_value(questions_map.get('Third vacancy (optional)')),
                'third_vacancy_description_url': _extract_file_url(questions_map.get('Upload job description (PDF or Word) (3)')),
                'further_vacancies': _extract_array_value(questions_map.get('Further vacancies')),  # Массив
                'hiring_beyond_sales': _extract_boolean_value(questions_map.get('Are you also hiring for roles beyond Sales?')),
                
                # Локация
                'job_location': _extract_text_value(questions_map.get('Job Location')),
                'remote_work': _extract_boolean_value(questions_map.get('Remote Work Available')),
                
                # Зарплата
                'salary_min': _parse_float(_extract_text_value(questions_map.get('Salary Min'))),
                'salary_max': _parse_float(_extract_text_value(questions_map.get('Salary Max'))),
                'salary_currency': _extract_text_value(questions_map.get('Currency')),
                
                # Контактная информация (пока что поля не видны в форме)
                'contact_name': _extract_text_value(questions_map.get('Contact Name')),
                'contact_email': _extract_text_value(questions_map.get('Contact Email')),
                'contact_phone': _extract_text_value(questions_map.get('Contact Phone')),
                'contact_position': _extract_text_value(questions_map.get('Your job title')),
                
                # Адрес компании
                'company_address': _extract_text_value(questions_map.get('Company Address')),
                'city': _extract_text_value(questions_map.get('City')),
                'country': _extract_text_value(questions_map.get('Country')),
                'postal_code': _extract_text_value(questions_map.get('Postal Code')),
                
                # Дополнительные поля
                'answer_optional_questions': _extract_boolean_value(questions_map.get('Would you like to answer a few optional questions to help us better understand your hiring needs?')),
            }
            
            records.append(record)
            
        except Exception as e:
            print(f"❌ Ошибка при парсинге company submission {response.get('submissionId', 'unknown')}: {e}")
            continue
    
    return records


def _save_cv_to_existing_tables(records: List[Dict[str, Any]]) -> int:
    """Сохранение данных кандидатов в существующие таблицы candidates и submissions"""
    
    if not records:
        return 0
    
    db = database.get_session()
    saved_count = 0
    
    try:
        for record in records:
            try:
                # Проверяем, существует ли уже такая заявка по submission_id
                submission_id = record.get('submission_id')
                existing_submission = None
                
                if submission_id:
                    # Сначала проверяем по submission_id
                    existing_submission = db.query(Submission).filter(
                        Submission.submission_id == submission_id
                    ).first()
                
                if existing_submission:
                    # Обновляем существующую заявку если данные изменились
                    if existing_submission.last_updated != record['last_updated']:
                        _update_existing_submission(db, existing_submission, record)
                        saved_count += 1
                else:
                    # Создаем нового кандидата и заявку
                    if _create_new_candidate_submission(db, record):
                        saved_count += 1
                    
            except Exception as e:
                print(f"⚠️ Ошибка при сохранении CV записи {record.get('submission_id')}: {e}")
                continue
        
        db.commit()
        return saved_count
        
    except Exception as e:
        print(f"❌ Ошибка при сохранении CV данных: {e}")
        db.rollback()
        return 0
    finally:
        db.close()


def _save_company_to_existing_tables(records: List[Dict[str, Any]]) -> int:
    """Сохранение данных компаний в существующие таблицы companies, company_contacts и jobs"""
    
    if not records:
        return 0
    
    db = database.get_session()
    saved_count = 0
    
    try:
        for record in records:
            try:
                # Проверяем, существует ли уже такая компания
                company_name = record.get('company_name')
                if not company_name:
                    continue
                
                existing_company = db.query(Company).filter(
                    Company.name.ilike(f"%{company_name}%")
                ).first()
                
                if not existing_company:
                    # Создаем новую компанию
                    # Собираем дополнительную информацию о контакте для описания
                    contact_info = ""
                    if record.get('contact_position'):
                        contact_info += f"Contact: {record.get('contact_position')}"
                    if record.get('contact_name'):
                        contact_info += f" - {record.get('contact_name')}"
                    if record.get('contact_phone'):
                        contact_info += f" (Phone: {record.get('contact_phone')})"
                    
                    description = record.get('description', '')
                    if contact_info:
                        description = f"{description}\n\n{contact_info}".strip()
                    
                    company = Company(
                        name=company_name,
                        website=record.get('website'),
                        description=description
                    )
                    db.add(company)
                    db.flush()  # Получаем ID
                    existing_company = company
                
                # Проверяем контакт (только если есть email)
                contact_email = record.get('contact_email')
                contact = None
                if contact_email:
                    existing_contact = db.query(CompanyContact).filter(
                        CompanyContact.company_id == existing_company.company_id,
                        CompanyContact.email == contact_email
                    ).first()
                    
                    if not existing_contact:
                        # Создаем новый контакт
                        contact = CompanyContact(
                            company_id=existing_company.company_id,
                            full_name=record.get('contact_name', ''),
                            email=contact_email,
                            phone=record.get('contact_phone'),
                            job_title=record.get('contact_position'),
                            is_primary=True
                        )
                        db.add(contact)
                        db.flush()
                    else:
                        contact = existing_contact
                else:
                    # Если нет email, ищем существующий контакт по имени и должности
                    contact_name = record.get('contact_name')
                    contact_position = record.get('contact_position')
                    if contact_name or contact_position:
                        # Используем placeholder email основанный на данных компании
                        placeholder_email = f"contact@{company_name.lower().replace(' ', '')}.placeholder"
                        
                        existing_contact = db.query(CompanyContact).filter(
                            CompanyContact.company_id == existing_company.company_id,
                            CompanyContact.email == placeholder_email
                        ).first()
                        
                        if not existing_contact:
                            contact = CompanyContact(
                                company_id=existing_company.company_id,
                                full_name=contact_name or 'Unknown Contact',
                                email=placeholder_email,
                                phone=record.get('contact_phone'),
                                job_title=contact_position or 'Unknown Position',
                                is_primary=True
                            )
                            db.add(contact)
                            db.flush()
                        else:
                            contact = existing_contact
                
                # Создаем вакансию если есть данные
                job_title = record.get('job_title')
                if job_title:
                    # Проверяем, существует ли такая вакансия
                    existing_job = db.query(Job).filter(
                        Job.company_id == existing_company.company_id,
                        Job.title.ilike(f"%{job_title}%")
                    ).first()
                    
                    if not existing_job:
                        # Формируем salary_range из min/max
                        salary_range = None
                        if record.get('salary_min') or record.get('salary_max'):
                            min_sal = record.get('salary_min', '')
                            max_sal = record.get('salary_max', '')
                            currency = record.get('salary_currency', 'USD')
                            if min_sal and max_sal:
                                salary_range = f"{min_sal}-{max_sal} {currency}"
                            elif min_sal:
                                salary_range = f"from {min_sal} {currency}"
                            elif max_sal:
                                salary_range = f"up to {max_sal} {currency}"
                        
                        job = Job(
                            company_id=existing_company.company_id,
                            title=job_title,
                            description=record.get('job_description') or record.get('description') or 'No description provided',
                            job_description_url=record.get('job_description_url'),
                            employment_type=record.get('employment_type'),
                            experience_level=record.get('experience_level'),
                            salary_range=salary_range,
                            currency=record.get('salary_currency'),
                            location=record.get('job_location'),
                            created_by=contact.contact_id if contact else None,
                            is_active=True
                        )
                        db.add(job)
                        saved_count += 1
                
                # Создаем вторую вакансию если есть данные
                second_job_title = record.get('second_vacancy')
                if second_job_title:
                    existing_second_job = db.query(Job).filter(
                        Job.company_id == existing_company.company_id,
                        Job.title.ilike(f"%{second_job_title}%")
                    ).first()
                    
                    if not existing_second_job:
                        second_job = Job(
                            company_id=existing_company.company_id,
                            title=second_job_title,
                            description=record.get('description') or 'No description provided',
                            job_description_url=record.get('second_vacancy_description_url') or record.get('job_description_url'),
                            employment_type=record.get('employment_type'),
                            experience_level=record.get('experience_level'),
                            salary_range=salary_range,
                            currency=record.get('salary_currency'),
                            location=record.get('job_location'),
                            created_by=contact.contact_id if contact else None,
                            is_active=True
                        )
                        db.add(second_job)
                        saved_count += 1
                
                # Создаем третью вакансию если есть данные
                third_job_title = record.get('third_vacancy')
                if third_job_title:
                    existing_third_job = db.query(Job).filter(
                        Job.company_id == existing_company.company_id,
                        Job.title.ilike(f"%{third_job_title}%")
                    ).first()
                    
                    if not existing_third_job:
                        third_job = Job(
                            company_id=existing_company.company_id,
                            title=third_job_title,
                            description=record.get('description') or 'No description provided',
                            job_description_url=record.get('third_vacancy_description_url') or record.get('job_description_url'),
                            employment_type=record.get('employment_type'),
                            experience_level=record.get('experience_level'),
                            salary_range=salary_range,
                            currency=record.get('salary_currency'),
                            location=record.get('job_location'),
                            created_by=contact.contact_id if contact else None,
                            is_active=True
                        )
                        db.add(third_job)
                        saved_count += 1
                    
            except Exception as e:
                print(f"⚠️ Ошибка при сохранении записи компании {record.get('submission_id')}: {e}")
                continue
        
        db.commit()
        return saved_count
        
    except Exception as e:
        print(f"❌ Ошибка при сохранении данных компаний: {e}")
        db.rollback()
        return 0
    finally:
        db.close()


def _create_new_candidate_submission(db: Session, record: Dict[str, Any]) -> bool:
    """Создание нового кандидата и заявки"""
    
    try:
        # Проверяем, существует ли кандидат с таким email
        email = record.get('email')
        if not email:
            return False
        
        existing_candidate = db.query(Candidate).filter(Candidate.email == email).first()
        
        if not existing_candidate:
            # Создаем нового кандидата
            candidate = Candidate(
                first_name=record.get('first_name', ''),
                last_name=record.get('last_name', ''),
                email=email,
                mobile_number=record.get('phone'),
                linkedin_url=record.get('linkedin')
            )
            db.add(candidate)
            db.flush()  # Получаем ID
        else:
            candidate = existing_candidate
        
        # Создаем заявку только если такой submission_id еще нет
        submission_id = record.get('submission_id')
        if submission_id:
            try:
                submission_uuid = uuid.UUID(submission_id)
            except (ValueError, TypeError):
                submission_uuid = uuid.uuid4()
        else:
            submission_uuid = uuid.uuid4()
        
        submission_data = {
            'submission_id': submission_uuid,
            'candidate_id': candidate.candidate_id,
            'resume_url': record.get('resume_url', ''),
            'agree_to_processing': record.get('data_processing_agreed', False),
            'agree_to_contact': record.get('future_opportunities_agreed', False),
            'status': record.get('status', 'submitted'),
            'current_step': record.get('current_step', ''),
            'submission_started': record.get('submission_started') or datetime.utcnow(),
            'last_updated': record.get('last_updated') or datetime.utcnow(),
            'legally_authorized_us': _parse_yes_no_to_bool(record.get('us_work_authorized')),
            'requires_sponsorship': _parse_yes_no_to_bool(record.get('visa_sponsorship_required')),
            'pe_license': _parse_yes_no_to_bool(record.get('pe_license')),
            'work_preference': record.get('work_preference'),
            'willingness_to_travel': _parse_int_safe(record.get('willingness_to_travel')),
            'willing_to_relocate': record.get('willing_to_relocate'),
            'work_shift_related': _parse_yes_no_to_bool(record.get('work_shift_related')),
            'available_shifts': record.get('available_shifts'),
            'source': record.get('source'),
            'utm_source': record.get('utm_source'),
            'utm_medium': record.get('utm_medium'),
            'utm_campaign': record.get('utm_campaign'),
            'errors': record.get('errors'),
            'url': record.get('url'),
            'network_id': record.get('network_id'),
            'specific_locations_preferred': record.get('specific_countries_cities')
        }
        
        submission = Submission(**submission_data)
        db.add(submission)
        
        # Добавляем зарплатные ожидания если есть
        if record.get('min_salary') or record.get('max_salary'):
            salary_expectation = SalaryExpectation(
                submission_id=submission.submission_id,
                min_salary=record.get('min_salary'),
                max_salary=record.get('max_salary'),
                currency=record.get('salary_currency', 'USD')
            )
            db.add(salary_expectation)
        
        # Добавляем адрес если есть данные
        if record.get('address') or record.get('city') or record.get('country'):
            address = Address(
                submission_id=submission.submission_id,
                address=record.get('address') or '',
                city=record.get('city') or '',
                state_province=record.get('state') or '',
                zip_postal_code=record.get('zip_code') or '',
                country=record.get('country') or ''
            )
            db.add(address)
        
        # Добавляем образование если есть данные
        if record.get('education_level') or record.get('field_of_study'):
            education = Education(
                submission_id=submission.submission_id,
                degree_level=record.get('education_level') or '',
                field_of_study=record.get('field_of_study') or '',
                other_field_of_study=record.get('other_field_of_study')
            )
            db.add(education)
        
        # Добавляем компетенции если есть
        core_competency = record.get('core_competency')
        if core_competency:
            competencies = core_competency.split(', ')
            for comp_name in competencies:
                comp_name = comp_name.strip()
                if comp_name:
                    # Ищем или создаем компетенцию
                    competency = db.query(Competency).filter(Competency.name == comp_name).first()
                    if not competency:
                        competency = Competency(name=comp_name)
                        db.add(competency)
                        db.flush()
                    # Связываем с заявкой
                    if competency not in submission.competencies:
                        submission.competencies.append(competency)
        
        # Добавляем роли если есть
        preferred_role_type = record.get('preferred_role_type')
        if preferred_role_type:
            roles = preferred_role_type.split(', ')
            for role_name in roles:
                role_name = role_name.strip()
                if role_name:
                    # Ищем или создаем роль
                    role = db.query(Role).filter(Role.name == role_name).first()
                    if not role:
                        role = Role(name=role_name)
                        db.add(role)
                        db.flush()
                    # Связываем с заявкой
                    if role not in submission.roles:
                        submission.roles.append(role)
        
        # Добавляем индустрии если есть
        preferred_industry = record.get('preferred_industry')
        if preferred_industry:
            industries = preferred_industry.split(', ')
            for industry_name in industries:
                industry_name = industry_name.strip()
                if industry_name:
                    # Ищем или создаем индустрию
                    industry = db.query(Industry).filter(Industry.name == industry_name).first()
                    if not industry:
                        industry = Industry(name=industry_name)
                        db.add(industry)
                        db.flush()
                    # Связываем с заявкой
                    if industry not in submission.industries:
                        submission.industries.append(industry)
        
        # Добавляем локации если есть
        preferred_work_locations = record.get('preferred_work_locations')
        if preferred_work_locations:
            locations = preferred_work_locations.split(', ')
            for location_name in locations:
                location_name = location_name.strip()
                if location_name:
                    # Ищем или создаем локацию
                    location = db.query(Location).filter(Location.name == location_name).first()
                    if not location:
                        location = Location(name=location_name)
                        db.add(location)
                        db.flush()
                    # Связываем с заявкой
                    if location not in submission.locations:
                        submission.locations.append(location)

        return True
        
    except Exception as e:
        print(f"❌ Ошибка при создании кандидата/заявки: {e}")
        return False


def _update_existing_submission(db: Session, submission: Submission, record: Dict[str, Any]):
    """Обновление существующей заявки"""
    
    try:
        # Обновляем основные поля
        submission.status = record.get('status', submission.status)
        submission.current_step = record.get('current_step', submission.current_step)
        setattr(submission, 'last_updated', record.get('last_updated') or datetime.utcnow())
        
        # Обновляем кандидата если нужно
        candidate = submission.candidate
        if candidate:
            candidate.mobile_number = record.get('phone') or candidate.mobile_number
            candidate.linkedin_url = record.get('linkedin') or candidate.linkedin_url
        
    except Exception as e:
        print(f"❌ Ошибка при обновлении заявки: {e}")


# Новые улучшенные задачи с проверкой дубликатов по хэшу

@celery_app.task(
    bind=True,
    name='tasks.fillout_tasks.pull_fillout_resumes',
    soft_time_limit=300,  # 5 минут
    time_limit=420,       # 7 минут
    max_retries=3
)
def pull_fillout_resumes(self) -> Dict[str, Any]:
    """
    Получение новых резюме из Fillout API с проверкой дубликатов по хэшу
    
    Returns:
        Dict с информацией о полученных резюме
    """
    logger.info("📥 Получение новых резюме из Fillout API")
    
    try:
        api_key = os.getenv('FILLOUT_API_KEY')
        cv_form_id = os.getenv('CV_FORM_ID') or os.getenv('FILLOUT_CV_FORM_ID')
        
        if not api_key or not cv_form_id:
            raise ValueError("Отсутствуют FILLOUT_API_KEY или CV_FORM_ID в переменных окружения")
        
        # Получаем данные из API
        url = f"https://api.fillout.com/v1/api/forms/{cv_form_id}/submissions"
        headers = {
            'Authorization': f'Bearer {api_key}',
            'Accept': 'application/json'
        }
        
        # Параметры для получения только последних данных
        params = {}
        
        # Получаем последнее время обновления из БД
        try:
            last_update = _get_last_resume_update()
            if last_update:
                params['after'] = last_update.isoformat()
                logger.info(f"📅 Запрос данных после: {last_update}")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось получить последнее обновление: {e}")
        
        # Выполняем запрос
        logger.info(f"🌐 Запрос к Fillout API: {url}")
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        json_data = response.json()
        submissions = json_data.get('responses', [])
        logger.info(f"📊 Получено {len(submissions)} записей резюме")
        
        if not submissions:
            return {
                'status': 'completed',
                'message': 'Нет новых резюме',
                'cv_data': {
                    'total_received': 0,
                    'new_count': 0,
                    'duplicate_count': 0,
                    'submission_ids': []
                }
            }
        
        # Обрабатываем полученные данные
        processed_result = _process_resume_submissions(submissions)
        
        logger.info(f"✅ Обработка резюме завершена: {processed_result['new_count']} новых, {processed_result['duplicate_count']} дубликатов")
        
        return {
            'status': 'completed',
            'message': f"Обработано {processed_result['new_count']} новых резюме",
            'cv_data': processed_result
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка получения резюме из Fillout: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'cv_data': {
                'total_received': 0,
                'new_count': 0,
                'duplicate_count': 0,
                'submission_ids': []
            }
        }


@celery_app.task(
    bind=True,
    name='tasks.fillout_tasks.pull_fillout_jobs',
    soft_time_limit=300,  # 5 минут
    time_limit=420,       # 7 минут
    max_retries=3
)
def pull_fillout_jobs(self) -> Dict[str, Any]:
    """
    Получение новых вакансий из Fillout API с проверкой дубликатов по хэшу
    
    Returns:
        Dict с информацией о полученных вакансиях
    """
    logger.info("📥 Получение новых вакансий из Fillout API")
    
    try:
        api_key = os.getenv('FILLOUT_API_KEY')
        company_form_id = os.getenv('COMPANY_FORM_ID') or os.getenv('FILLOUT_COMPANY_FORM_ID')
        
        if not api_key or not company_form_id:
            raise ValueError("Отсутствуют FILLOUT_API_KEY или COMPANY_FORM_ID в переменных окружения")
        
        # Получаем данные из API
        url = f"https://api.fillout.com/v1/api/forms/{company_form_id}/submissions"
        headers = {
            'Authorization': f'Bearer {api_key}',
            'Accept': 'application/json'
        }
        
        # Параметры для получения только последних данных
        params = {}
        
        # Получаем последнее время обновления из БД
        try:
            last_update = _get_last_job_update()
            if last_update:
                params['after'] = last_update.isoformat()
                logger.info(f"📅 Запрос данных после: {last_update}")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось получить последнее обновление: {e}")
        
        # Выполняем запрос
        logger.info(f"🌐 Запрос к Fillout API: {url}")
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        json_data = response.json()
        submissions = json_data.get('responses', [])
        logger.info(f"📊 Получено {len(submissions)} записей вакансий")
        
        if not submissions:
            return {
                'status': 'completed',
                'message': 'Нет новых вакансий',
                'company_data': {
                    'total_received': 0,
                    'new_count': 0,
                    'duplicate_count': 0,
                    'job_ids': []
                }
            }
        
        # Обрабатываем полученные данные
        processed_result = _process_job_submissions(submissions)
        
        logger.info(f"✅ Обработка вакансий завершена: {processed_result['new_count']} новых, {processed_result['duplicate_count']} дубликатов")
        
        return {
            'status': 'completed',
            'message': f"Обработано {processed_result['new_count']} новых вакансий",
            'company_data': processed_result
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка получения вакансий из Fillout: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'company_data': {
                'total_received': 0,
                'new_count': 0,
                'duplicate_count': 0,
                'job_ids': []
            }
        }


# Вспомогательные функции для обработки с проверкой хэшей

def _process_resume_submissions(submissions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Обработка submissions резюме с проверкой дубликатов по хэшу
    """
    logger.info(f"🔍 Обработка {len(submissions)} submissions резюме")
    
    new_submission_ids = []
    duplicate_count = 0
    existing_hashes = _get_existing_resume_hashes()
    
    try:
        db_session = database.get_session()
        
        for submission in submissions:
            try:
                # Извлекаем данные submission
                submission_id = submission.get('submissionId')
                submission_time = submission.get('submissionTime')
                questions = submission.get('questions', [])
                
                if not submission_id:
                    logger.warning("⚠️ Пропуск submission без ID")
                    continue
                
                # Создаем хэш из важных полей
                content_hash = _create_enhanced_resume_hash(submission)
                
                # Проверяем, существует ли уже такой хэш
                if content_hash['composite_hash'] in existing_hashes:
                    logger.debug(f"🔄 Дубликат резюме найден: {content_hash['composite_hash'][:8]}...")
                    duplicate_count += 1
                    continue
                
                # Парсим данные submission
                resume_data = _parse_resume_submission(submission)
                
                # Сохраняем в БД
                if _save_resume_data_with_hash(db_session, resume_data, content_hash['composite_hash']):
                    new_submission_ids.append(submission_id)
                    existing_hashes.add(content_hash['composite_hash'])
                    logger.debug(f"✅ Новое резюме сохранено: {submission_id}")
                
            except Exception as e:
                logger.error(f"❌ Ошибка обработки submission {submission_id}: {e}")
                continue
        
        db_session.close()
        
        return {
            'total_received': len(submissions),
            'new_count': len(new_submission_ids),
            'duplicate_count': duplicate_count,
            'submission_ids': new_submission_ids,
            'processed_at': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка обработки submissions резюме: {e}")
        raise


def _process_job_submissions(submissions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Обработка submissions вакансий с проверкой дубликатов по хэшу
    """
    logger.info(f"🔍 Обработка {len(submissions)} submissions вакансий")
    
    new_job_ids = []
    duplicate_count = 0
    existing_hashes = _get_existing_job_hashes()
    
    try:
        db_session = database.get_session()
        
        for submission in submissions:
            try:
                # Извлекаем данные submission
                submission_id = submission.get('submissionId')
                submission_time = submission.get('submissionTime')
                questions = submission.get('questions', [])
                
                if not submission_id:
                    logger.warning("⚠️ Пропуск submission без ID")
                    continue
                
                # Создаем хэш из важных полей
                content_hash = _create_enhanced_job_hash(submission)
                
                # Проверяем, существует ли уже такой хэш
                if content_hash['composite_hash'] in existing_hashes:
                    logger.debug(f"🔄 Дубликат вакансии найден: {content_hash['composite_hash'][:8]}...")
                    duplicate_count += 1
                    continue
                
                # Парсим данные submission
                job_data = _parse_job_submission(submission)
                
                # Сохраняем в БД
                if _save_job_data_with_hash(db_session, job_data, content_hash['composite_hash']):
                    new_job_ids.append(submission_id)
                    existing_hashes.add(content_hash['composite_hash'])
                    logger.debug(f"✅ Новая вакансия сохранена: {submission_id}")
                
            except Exception as e:
                logger.error(f"❌ Ошибка обработки submission {submission_id}: {e}")
                continue
        
        db_session.close()
        
        return {
            'total_received': len(submissions),
            'new_count': len(new_job_ids),
            'duplicate_count': duplicate_count,
            'job_ids': new_job_ids,
            'processed_at': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка обработки submissions вакансий: {e}")
        raise


def _create_enhanced_resume_hash(submission: Dict[str, Any]) -> Dict[str, str]:
    """
    Создание улучшенного хэша для резюме с множественными проверками
    
    Returns:
        Dict с различными типами хэшей:
        - content_hash: основной хэш контента
        - file_hash: хэш файла резюме (если есть)
        - metadata_hash: хэш метаданных
        - composite_hash: композитный хэш всех данных
    """
    questions = submission.get('questions', [])
    
    # Основные поля для content_hash
    content_fields = []
    file_url = None
    metadata_fields = []
    
    field_mapping = {
        'Your Email': 'email',
        'First Name': 'first_name',
        'Last Name': 'last_name',
        'Upload your resume': 'resume_url',
        'Technical Skills': 'skills',
        'What position are you applying for?': 'position',
        'Phone Number': 'phone',
        'Years of Experience': 'experience',
        'Desired Salary': 'salary'
    }
    
    for question in questions:
        question_name = question.get('name', '')
        question_value = question.get('value', '')
        
        if question_name in field_mapping and question_value:
            content_fields.append(f"{field_mapping[question_name]}:{question_value}")
            
            # Сохраняем URL файла отдельно
            if question_name == 'Upload your resume':
                file_url = question_value
    
    # Метаданные
    metadata_fields = [
        f"submission_id:{submission.get('submissionId', '')}",
        f"submitted_at:{submission.get('submissionTime', '')}",
        f"last_updated:{submission.get('lastUpdatedAt', '')}"
    ]
    
    # Создаем различные хэши
    content_hash = hashlib.md5('|'.join(sorted(content_fields)).encode('utf-8')).hexdigest()
    metadata_hash = hashlib.md5('|'.join(sorted(metadata_fields)).encode('utf-8')).hexdigest()
    
    # Хэш файла (если есть URL)
    file_hash = None
    if file_url:
        try:
            file_hash = _get_file_content_hash(file_url)
        except Exception as e:
            logger.warning(f"⚠️ Не удалось получить хэш файла {file_url}: {e}")
    
    # Композитный хэш всех данных
    all_fields = content_fields + metadata_fields
    if file_hash:
        all_fields.append(f"file_hash:{file_hash}")
    
    composite_hash = hashlib.md5('|'.join(sorted(all_fields)).encode('utf-8')).hexdigest()
    
    return {
        'content_hash': content_hash,
        'file_hash': file_hash,
        'metadata_hash': metadata_hash,
        'composite_hash': composite_hash
    }


def _create_enhanced_job_hash(submission: Dict[str, Any]) -> Dict[str, str]:
    """
    Создание улучшенного хэша для вакансии с множественными проверками
    """
    questions = submission.get('questions', [])
    
    # Основные поля для content_hash
    content_fields = []
    file_url = None
    metadata_fields = []
    
    field_mapping = {
        'Company Name': 'company_name',
        'Contact Email': 'email',
        'Job Title': 'job_title',
        'Job Description': 'job_description',
        'Requirements': 'requirements',
        'Salary Range': 'salary_range',
        'Location': 'location',
        'Employment Type': 'employment_type',
        'Industry': 'industry'
    }
    
    for question in questions:
        question_name = question.get('name', '')
        question_value = question.get('value', '')
        
        if question_name in field_mapping and question_value:
            content_fields.append(f"{field_mapping[question_name]}:{question_value}")
            
            # Сохраняем URL файла отдельно
            if question_name == 'Upload Job Description':
                file_url = question_value
    
    # Метаданные
    metadata_fields = [
        f"submission_id:{submission.get('submissionId', '')}",
        f"submitted_at:{submission.get('submissionTime', '')}",
        f"last_updated:{submission.get('lastUpdatedAt', '')}"
    ]
    
    # Создаем различные хэши
    content_hash = hashlib.md5('|'.join(sorted(content_fields)).encode('utf-8')).hexdigest()
    metadata_hash = hashlib.md5('|'.join(sorted(metadata_fields)).encode('utf-8')).hexdigest()
    
    # Хэш файла (если есть URL)
    file_hash = None
    if file_url:
        try:
            file_hash = _get_file_content_hash(file_url)
        except Exception as e:
            logger.warning(f"⚠️ Не удалось получить хэш файла {file_url}: {e}")
    
    # Композитный хэш всех данных
    all_fields = content_fields + metadata_fields
    if file_hash:
        all_fields.append(f"file_hash:{file_hash}")
    
    composite_hash = hashlib.md5('|'.join(sorted(all_fields)).encode('utf-8')).hexdigest()
    
    return {
        'content_hash': content_hash,
        'file_hash': file_hash,
        'metadata_hash': metadata_hash,
        'composite_hash': composite_hash
    }


def _get_file_content_hash(file_url: str) -> str:
    """
    Получение хэша содержимого файла по URL
    
    Args:
        file_url: URL файла
        
    Returns:
        MD5 хэш содержимого файла
    """
    try:
        # Скачиваем файл
        response = requests.get(file_url, timeout=30)
        response.raise_for_status()
        
        # Создаем хэш содержимого
        file_content_hash = hashlib.md5(response.content).hexdigest()
        
        logger.debug(f"📄 Хэш файла {_mask_url(file_url)}: {file_content_hash[:8]}...")
        return file_content_hash
    
    except Exception as e:
        logger.error(f"❌ Ошибка получения хэша файла {file_url}: {e}")
        raise


def _check_duplicate_with_enhanced_hash(hashes: Dict[str, str], existing_records: List[Dict[str, str]]) -> Dict[str, Any]:
    """
    Проверка дубликатов с использованием множественных хэшей
    
    Args:
        hashes: Словарь с различными типами хэшей
        existing_records: Список существующих записей с хэшами
        
    Returns:
        Dict с результатами проверки
    """
    duplicate_found = False
    duplicate_type = None
    duplicate_record = None
    confidence = 0.0
    
    for record in existing_records:
        # Проверяем композитный хэш (наивысший приоритет)
        if hashes.get('composite_hash') == record.get('composite_hash'):
            duplicate_found = True
            duplicate_type = 'composite'
            duplicate_record = record
            confidence = 1.0
            break
        
        # Проверяем хэш файла (высокий приоритет)
        if hashes.get('file_hash') and hashes.get('file_hash') == record.get('file_hash'):
            duplicate_found = True
            duplicate_type = 'file_content'
            duplicate_record = record
            confidence = 0.9
            break
        
        # Проверяем основной контент (средний приоритет)
        if hashes.get('content_hash') == record.get('content_hash'):
            duplicate_found = True
            duplicate_type = 'content'
            duplicate_record = record
            confidence = 0.7
            break
        
        # Проверяем метаданные (низкий приоритет)
        if hashes.get('metadata_hash') == record.get('metadata_hash'):
            duplicate_found = True
            duplicate_type = 'metadata'
            duplicate_record = record
            confidence = 0.3
            break
    
    return {
        'is_duplicate': duplicate_found,
        'duplicate_type': duplicate_type,
        'duplicate_record': duplicate_record,
        'confidence': confidence,
        'hashes': hashes
    }


def _get_existing_resume_records_with_hashes() -> List[Dict[str, str]]:
    """Получение существующих записей резюме с хэшами из БД"""
    try:
        db_session = database.get_session()
        result = db_session.query(
            Submission.id,
            Submission.submission_id,
            Submission.content_hash,
            Submission.file_hash,
            Submission.metadata_hash,
            Submission.composite_hash
        ).filter(
            Submission.content_hash.isnot(None)
        ).all()
        db_session.close()
        
        return [
            {
                'id': row[0],
                'submission_id': row[1],
                'content_hash': row[2],
                'file_hash': row[3],
                'metadata_hash': row[4],
                'composite_hash': row[5]
            }
            for row in result
        ]
    except Exception as e:
        logger.warning(f"⚠️ Не удалось получить существующие записи резюме: {e}")
        return []


def _get_existing_job_records_with_hashes() -> List[Dict[str, str]]:
    """Получение существующих записей вакансий с хэшами из БД"""
    try:
        db_session = database.get_session()
        result = db_session.query(
            Job.id,
            Job.job_id,
            Job.content_hash,
            Job.file_hash,
            Job.metadata_hash,
            Job.composite_hash
        ).filter(
            Job.content_hash.isnot(None)
        ).all()
        db_session.close()
        
        return [
            {
                'id': row[0],
                'job_id': row[1],
                'content_hash': row[2],
                'file_hash': row[3],
                'metadata_hash': row[4],
                'composite_hash': row[5]
            }
            for row in result
        ]
    except Exception as e:
        logger.warning(f"⚠️ Не удалось получить существующие записи вакансий: {e}")
        return []


def _mask_url(url: str) -> str:
    """Маскирование URL для логов"""
    if len(url) > 50:
        return url[:25] + "..." + url[-20:]
    return url