"""
Document parsing tasks for HR analysis system
"""

import os
import hashlib
import tempfile
import requests
from typing import Dict, List, Any, Optional
from celery import Celery
from celery.utils.log import get_task_logger
from datetime import datetime

# PDF and document processing libraries
try:
    from PyPDF2 import PdfReader
    from docx import Document
    import docx2txt
except ImportError:
    logger = get_task_logger(__name__)
    logger.warning("⚠️ Document processing libraries not available")

# Создаем экземпляр Celery
# Безопасное подключение к Redis через Secret Manager
from deployment.common.utils.secret_manager import get_redis_url_with_auth
redis_url = get_redis_url_with_auth()
app = Celery('hr_analysis', broker=redis_url, backend=redis_url)
logger = get_task_logger(__name__)


@app.task(
    bind=True,
    name='tasks.parse_tasks.parse_documents',
    soft_time_limit=600,  # 10 минут
    time_limit=720,       # 12 минут
    max_retries=3
)
def parse_documents(self, documents_data: List[Dict[str, Any]], document_type: str = 'resume') -> Dict[str, Any]:
    """
    Парсинг документов (резюме или описаний вакансий)
    
    Args:
        documents_data: Список данных документов с URL и метаданными
        document_type: Тип документов ('resume' или 'job_description')
        
    Returns:
        Dict с результатами парсинга
    """
    logger.info(f"📄 Запуск парсинга {len(documents_data)} документов типа '{document_type}'")
    
    try:
        parsed_documents = []
        failed_documents = []
        
        for i, doc_data in enumerate(documents_data):
            try:
                # Обновляем прогресс
                progress = (i / len(documents_data)) * 100
                self.update_state(
                    state='PROGRESS',
                    meta={
                        'progress': int(progress),
                        'status': f'Парсинг документа {i+1}/{len(documents_data)}',
                        'document_type': document_type
                    }
                )
                
                # Парсим отдельный документ
                parsed_doc = _parse_single_document(doc_data, document_type)
                if parsed_doc:
                    parsed_documents.append(parsed_doc)
                else:
                    failed_documents.append({
                        'document_id': doc_data.get('id', f'doc_{i}'),
                        'error': 'Failed to parse document'
                    })
                    
            except Exception as e:
                logger.error(f"❌ Ошибка парсинга документа {i+1}: {e}")
                failed_documents.append({
                    'document_id': doc_data.get('id', f'doc_{i}'),
                    'error': str(e)
                })
                continue
        
        # Финальное обновление прогресса
        self.update_state(
            state='SUCCESS',
            meta={
                'progress': 100,
                'status': 'Парсинг завершен',
                'parsed_count': len(parsed_documents),
                'failed_count': len(failed_documents)
            }
        )
        
        result = {
            'status': 'completed',
            'document_type': document_type,
            'parsed_documents': parsed_documents,
            'failed_documents': failed_documents,
            'stats': {
                'total_count': len(documents_data),
                'parsed_count': len(parsed_documents),
                'failed_count': len(failed_documents),
                'success_rate': len(parsed_documents) / len(documents_data) * 100 if documents_data else 0
            },
            'processed_at': datetime.utcnow().isoformat()
        }
        
        logger.info(f"✅ Парсинг завершен: {len(parsed_documents)} успешно, {len(failed_documents)} ошибок")
        return result
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка парсинга: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'document_type': document_type,
            'processed_at': datetime.utcnow().isoformat()
        }


def _parse_single_document(doc_data: Dict[str, Any], document_type: str) -> Optional[Dict[str, Any]]:
    """
    Парсинг одного документа
    
    Args:
        doc_data: Данные документа (включая URL, ID, метаданные)
        document_type: Тип документа
        
    Returns:
        Распарсенные данные документа или None при ошибке
    """
    try:
        doc_id = doc_data.get('id') or doc_data.get('submission_id') or doc_data.get('job_id')
        doc_url = doc_data.get('url') or doc_data.get('file_url') or doc_data.get('resume_file_url')
        
        if not doc_url:
            logger.warning(f"⚠️ URL документа не найден для {doc_id}")
            return None
        
        logger.info(f"📄 Парсинг документа {doc_id}: {_mask_url(doc_url)}")
        
        # Скачиваем документ
        response = requests.get(doc_url, timeout=30)
        response.raise_for_status()
        
        # Определяем тип файла
        content_type = response.headers.get('content-type', '').lower()
        file_extension = _get_file_extension(doc_url, content_type)
        
        # Создаем временный файл
        with tempfile.NamedTemporaryFile(suffix=file_extension, delete=False) as temp_file:
            temp_file.write(response.content)
            temp_file_path = temp_file.name
        
        try:
            # Парсим содержимое в зависимости от типа файла
            if file_extension == '.pdf':
                text_content = _parse_pdf(temp_file_path)
            elif file_extension in ['.doc', '.docx']:
                text_content = _parse_docx(temp_file_path)
            else:
                # Попытка парсинга как текст
                with open(temp_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    text_content = f.read()
            
            # Генерируем хэш содержимого для проверки дубликатов
            content_hash = hashlib.md5(text_content.encode('utf-8')).hexdigest()
            
            # Извлекаем структурированную информацию
            if document_type == 'resume':
                structured_data = _extract_resume_info(text_content, doc_data)
            elif document_type == 'job_description':
                structured_data = _extract_job_info(text_content, doc_data)
            else:
                structured_data = {}
            
            parsed_doc = {
                'id': doc_id,
                'type': document_type,
                'url': doc_url,
                'content_hash': content_hash,
                'text_content': text_content,
                'structured_data': structured_data,
                'metadata': {
                    'file_size': len(response.content),
                    'content_type': content_type,
                    'file_extension': file_extension,
                    'parsed_at': datetime.utcnow().isoformat()
                },
                'original_data': doc_data
            }
            
            return parsed_doc
            
        finally:
            # Удаляем временный файл
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
    
    except Exception as e:
        logger.error(f"❌ Ошибка парсинга документа {doc_id}: {e}")
        return None


def _parse_pdf(file_path: str) -> str:
    """Парсинг PDF файла"""
    try:
        reader = PdfReader(file_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text() + "\n"
        return text.strip()
    except Exception as e:
        logger.error(f"❌ Ошибка парсинга PDF: {e}")
        raise


def _parse_docx(file_path: str) -> str:
    """Парсинг DOCX файла"""
    try:
        # Пробуем сначала docx2txt (быстрее)
        text = docx2txt.process(file_path)
        if text.strip():
            return text.strip()
        
        # Если не получилось, используем python-docx
        doc = Document(file_path)
        text = ""
        for paragraph in doc.paragraphs:
            text += paragraph.text + "\n"
        return text.strip()
    except Exception as e:
        logger.error(f"❌ Ошибка парсинга DOCX: {e}")
        raise


def _extract_resume_info(text_content: str, original_data: Dict[str, Any]) -> Dict[str, Any]:
    """Извлечение структурированной информации из резюме"""
    # Базовая реализация - можно улучшить с помощью NLP
    structured_info = {
        'skills': _extract_skills(text_content),
        'experience': _extract_experience(text_content),
        'education': _extract_education(text_content),
        'contact_info': _extract_contact_info(text_content),
        'summary': _extract_summary(text_content)
    }
    
    # Дополняем данными из оригинальной формы
    if original_data:
        structured_info.update({
            'form_data': {
                'first_name': original_data.get('first_name'),
                'last_name': original_data.get('last_name'),
                'email': original_data.get('email'),
                'phone': original_data.get('phone'),
                'skills': original_data.get('skills'),
                'experience_years': original_data.get('experience_years'),
                'desired_position': original_data.get('desired_position')
            }
        })
    
    return structured_info


def _extract_job_info(text_content: str, original_data: Dict[str, Any]) -> Dict[str, Any]:
    """Извлечение структурированной информации из описания вакансии"""
    structured_info = {
        'requirements': _extract_requirements(text_content),
        'responsibilities': _extract_responsibilities(text_content),
        'benefits': _extract_benefits(text_content),
        'qualifications': _extract_qualifications(text_content),
        'company_info': _extract_company_info(text_content)
    }
    
    # Дополняем данными из оригинальной формы
    if original_data:
        structured_info.update({
            'form_data': {
                'job_title': original_data.get('job_title'),
                'company_name': original_data.get('company_name'),
                'location': original_data.get('location'),
                'salary_range': original_data.get('salary_range'),
                'employment_type': original_data.get('employment_type'),
                'is_remote': original_data.get('is_remote')
            }
        })
    
    return structured_info


# Вспомогательные функции для извлечения информации
def _extract_skills(text: str) -> List[str]:
    """Извлечение навыков из текста"""
    # Простая реализация - поиск ключевых слов
    skills_keywords = ['python', 'javascript', 'java', 'c++', 'sql', 'html', 'css', 'react', 'angular', 'vue', 'docker', 'kubernetes']
    found_skills = []
    text_lower = text.lower()
    for skill in skills_keywords:
        if skill in text_lower:
            found_skills.append(skill)
    return found_skills


def _extract_experience(text: str) -> List[str]:
    """Извлечение опыта работы"""
    # Поиск секций с опытом работы
    experience_sections = []
    lines = text.split('\n')
    current_section = []
    
    for line in lines:
        line = line.strip()
        if any(keyword in line.lower() for keyword in ['experience', 'work', 'employment', 'position']):
            if current_section:
                experience_sections.append('\n'.join(current_section))
                current_section = []
        current_section.append(line)
    
    if current_section:
        experience_sections.append('\n'.join(current_section))
    
    return experience_sections


def _extract_education(text: str) -> List[str]:
    """Извлечение образования"""
    education_keywords = ['university', 'college', 'degree', 'bachelor', 'master', 'phd', 'education']
    education_info = []
    lines = text.split('\n')
    
    for line in lines:
        if any(keyword in line.lower() for keyword in education_keywords):
            education_info.append(line.strip())
    
    return education_info


def _extract_contact_info(text: str) -> Dict[str, Any]:
    """Извлечение контактной информации"""
    import re
    contact_info = {}
    
    # Email
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    emails = re.findall(email_pattern, text)
    if emails:
        contact_info['emails'] = emails
    
    # Phone
    phone_pattern = r'[\+]?[1-9]?[0-9]{7,15}'
    phones = re.findall(phone_pattern, text)
    if phones:
        contact_info['phones'] = phones
    
    return contact_info


def _extract_summary(text: str) -> str:
    """Извлечение резюме/описания"""
    # Берем первые несколько строк как резюме
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    if lines:
        return ' '.join(lines[:3])  # Первые 3 непустые строки
    return ""


def _extract_requirements(text: str) -> List[str]:
    """Извлечение требований из вакансии"""
    requirements = []
    lines = text.split('\n')
    in_requirements = False
    
    for line in lines:
        line = line.strip()
        if any(keyword in line.lower() for keyword in ['requirements', 'qualifications', 'must have']):
            in_requirements = True
            continue
        if in_requirements and line and not line.startswith('•') and not line.startswith('-'):
            if any(keyword in line.lower() for keyword in ['responsibilities', 'benefits', 'we offer']):
                break
        if in_requirements and line:
            requirements.append(line)
    
    return requirements


def _extract_responsibilities(text: str) -> List[str]:
    """Извлечение обязанностей"""
    responsibilities = []
    lines = text.split('\n')
    in_responsibilities = False
    
    for line in lines:
        line = line.strip()
        if any(keyword in line.lower() for keyword in ['responsibilities', 'duties', 'you will']):
            in_responsibilities = True
            continue
        if in_responsibilities and line and not line.startswith('•') and not line.startswith('-'):
            if any(keyword in line.lower() for keyword in ['requirements', 'benefits', 'qualifications']):
                break
        if in_responsibilities and line:
            responsibilities.append(line)
    
    return responsibilities


def _extract_benefits(text: str) -> List[str]:
    """Извлечение льгот и преимуществ"""
    benefits = []
    lines = text.split('\n')
    in_benefits = False
    
    for line in lines:
        line = line.strip()
        if any(keyword in line.lower() for keyword in ['benefits', 'we offer', 'perks', 'compensation']):
            in_benefits = True
            continue
        if in_benefits and line and not line.startswith('•') and not line.startswith('-'):
            if any(keyword in line.lower() for keyword in ['requirements', 'responsibilities']):
                break
        if in_benefits and line:
            benefits.append(line)
    
    return benefits


def _extract_qualifications(text: str) -> List[str]:
    """Извлечение квалификаций"""
    return _extract_requirements(text)  # Используем ту же логику


def _extract_company_info(text: str) -> Dict[str, Any]:
    """Извлечение информации о компании"""
    # Простая реализация
    return {
        'description': _extract_summary(text)
    }


def _get_file_extension(url: str, content_type: str) -> str:
    """Определение расширения файла"""
    if 'pdf' in content_type:
        return '.pdf'
    elif 'word' in content_type or 'document' in content_type:
        return '.docx'
    elif url.endswith('.pdf'):
        return '.pdf'
    elif url.endswith(('.doc', '.docx')):
        return '.docx'
    else:
        return '.txt'


def _mask_url(url: str) -> str:
    """Маскирование URL для логов"""
    if len(url) > 50:
        return url[:25] + "..." + url[-20:]
    return url
