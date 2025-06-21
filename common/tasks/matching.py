"""
Консолидированные Celery задачи для сопоставления резюме и вакансий

Основная функциональность:
- Поиск резюме для вакансии (find_matching_resumes_for_job)
- Поиск вакансий для резюме (find_matching_jobs_for_resume)
- Пакетная обработка (batch_find_matches_for_jobs, batch_find_matches_for_resumes)
- Упрощенные версии без обновления состояния (_simple варианты)

Использует унифицированные декораторы из tasks.base для retry, сериализации и мониторинга.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any
from celery.utils.log import get_task_logger
from database.operations.candidate_operations import SubmissionCRUD
from database.operations.company_operations import JobCRUD
from utils.chroma_config import chroma_client, ChromaConfig
from tasks.task_utils import safe_uuid_convert, serialize_for_json, mask_sensitive_data
from tasks.base import get_db_session, safe_task, monitored_task, standard_retry_policy, celery_friendly_delay

# Импортируем Celery app
from celery_app.celery_app import get_celery_app

app = get_celery_app()

logger = get_task_logger(__name__)


@app.task(
    bind=True, 
    name='tasks.matching.find_matching_resumes_for_job',
    soft_time_limit=300,  # 5 минут
    time_limit=360        # 6 минут
)
@safe_task
@monitored_task
@standard_retry_policy(max_retries=3, countdown=30)
def find_matching_resumes_for_job(self, job_id: int, top_k: int = 20, min_similarity: float = 0.4) -> Dict[str, Any]:
    """
    ОСНОВНАЯ ЗАДАЧА: Поиск наиболее подходящих резюме для вакансии
    
    Args:
        job_id: ID вакансии
        top_k: Количество топ-резюме для возврата (по умолчанию 20)
        min_similarity: Минимальный порог сходства (по умолчанию 0.4)
        
    Returns:
        Словарь с результатами сопоставления и рейтингом кандидатов
    """
    logger.info(f"🎯 Поиск кандидатов для вакансии {job_id}")
    
    with get_db_session() as db:
        # Проверяем доступность ChromaDB
        if not chroma_client.health_check():
            logger.error("❌ ChromaDB недоступен")
            raise Exception("ChromaDB недоступен")
        
        # Валидируем job_id
        if not isinstance(job_id, int) or job_id <= 0:
            logger.error(f"❌ Некорректный job_id: {job_id}")
            raise ValueError(f"Некорректный job_id: {job_id}")
        
        # Получаем вакансию
        job = JobCRUD.get_by_id(db, job_id)
        if not job:
            logger.error(f"❌ Вакансия не найдена: {job_id}")
            raise ValueError(f"Вакансия не найдена: {job_id}")
        
        if not getattr(job, 'job_description_raw_text', None):
            logger.warning(f"⚠️ Отсутствует текст описания для вакансии {job_id}")
            return {
                'job_id': job_id,
                'job_title': job.title,
                'matches': [],
                'total_found': 0,
                'message': 'Отсутствует текст описания вакансии'
            }
        
        # Получаем коллекции ChromaDB
        resume_collection = chroma_client.get_or_create_collection(ChromaConfig.RESUME_COLLECTION)
        job_collection = chroma_client.get_or_create_collection(ChromaConfig.JOB_COLLECTION)
        
        # Ищем эмбеддинг для данной вакансии
        job_prefix = f"job_{job_id}"
        all_job_results = job_collection.get(include=['embeddings', 'metadatas'])
        
        matching_job_id = None
        job_embedding = None
        for idx, job_id_str in enumerate(all_job_results['ids']):
            if job_id_str.startswith(job_prefix):
                matching_job_id = job_id_str
                job_embedding = all_job_results['embeddings'][idx]
                break
        
        if not matching_job_id:
            logger.warning(f"⚠️ Эмбеддинг для вакансии {job_id} не найден в ChromaDB")
            return {
                'job_id': job_id,
                'job_title': job.title,
                'matches': [],
                'total_found': 0,
                'message': 'Эмбеддинг вакансии не найден. Необходимо сгенерировать эмбеддинг.'
            }
        
        # Ищем наиболее похожие резюме
        logger.info(f"🔍 Поиск похожих резюме в ChromaDB (top_{top_k})")
        resume_matches = resume_collection.query(
            query_embeddings=[job_embedding],
            n_results=min(top_k * 2, 100),  # Берем больше для фильтрации
            include=['distances', 'metadatas', 'documents']
        )
        
        # Обрабатываем результаты
        matches = []
        if resume_matches['ids'] and resume_matches['ids'][0]:
            submission_ids = resume_matches['ids'][0]
            distances = resume_matches['distances'][0]
            metadatas = resume_matches['metadatas'][0]
            documents = resume_matches['documents'][0]
            
            logger.info(f"📊 Обрабатываем {len(submission_ids)} результатов из ChromaDB")
            
            for i, submission_id_str in enumerate(submission_ids):
                # Преобразуем расстояние в сходство (cosine similarity)
                similarity = 1.0 - distances[i]
                
                # Фильтруем по минимальному порогу сходства
                if similarity >= min_similarity:
                    # Извлекаем UUID из строки формата 'resume_{UUID}_{hash}'
                    submission_uuid = None
                    if submission_id_str.startswith('resume_'):
                        parts = submission_id_str.split('_')
                        if len(parts) >= 2:
                            submission_uuid = safe_uuid_convert(parts[1])
                    
                    if not submission_uuid:
                        continue
                    
                    match_data = {
                        'submission_id': str(submission_uuid),
                        'similarity': round(similarity, 4),
                        'distance': round(distances[i], 4),
                        'chroma_id': submission_id_str,
                        'metadata': metadatas[i] if metadatas[i] else {},
                        'snippet': documents[i][:300] + '...' if documents[i] and len(documents[i]) > 300 else documents[i]
                    }
                    matches.append(match_data)
        
        # Сортируем по убыванию сходства
        matches.sort(key=lambda x: x['similarity'], reverse=True)
        logger.info(f"📈 Найдено {len(matches)} совпадений выше порога {min_similarity}")
        
        # Валидируем существование в БД и обогащаем данными
        validated_matches = []
        for match in matches[:top_k]:  # Ограничиваем до top_k
            submission_uuid = safe_uuid_convert(match['submission_id'])
            if submission_uuid:
                submission = SubmissionCRUD.get_by_id(db, submission_uuid)
                if submission:
                    # Добавляем информацию о кандидате
                    match['candidate_id'] = submission.candidate_id
                    match['submission_status'] = getattr(submission, 'status', 'unknown')
                    
                    # Информация о кандидате (если связь есть)
                    if hasattr(submission, 'candidate') and submission.candidate:
                        candidate = submission.candidate
                        match['candidate_name'] = f"{candidate.first_name} {candidate.last_name}"
                        match['candidate_email'] = mask_sensitive_data(getattr(candidate, 'email', ''))
                    else:
                        match['candidate_name'] = 'Unknown'
                        match['candidate_email'] = 'Unknown'
                    
                    # Дополнительная информация о резюме
                    if hasattr(submission, 'resume_url'):
                        match['resume_url'] = getattr(submission, 'resume_url', None)
                    
                    validated_matches.append(match)
        
        logger.info(f"✅ Валидировано {len(validated_matches)} кандидатов для вакансии '{job.title}'")
        
        # Формируем финальный результат
        result = {
            'job_id': job_id,
            'job_title': job.title,
            'company_id': job.company_id,
            'matches': validated_matches,
            'total_found': len(validated_matches),
            'search_params': {
                'top_k': top_k,
                'min_similarity': min_similarity,
                'requested_top_k': top_k
            },
            'statistics': {
                'chroma_results': len(submission_ids) if resume_matches['ids'] and resume_matches['ids'][0] else 0,
                'above_threshold': len(matches),
                'validated_in_db': len(validated_matches)
            },
            'processed_at': datetime.utcnow().isoformat(),
            'message': f'Найдено {len(validated_matches)} подходящих кандидатов'
        }
        
        return result


@app.task(
    bind=True, 
    name='tasks.matching.find_matching_jobs_for_resume',
    soft_time_limit=300,  # 5 минут
    time_limit=360        # 6 минут
)
@safe_task
@monitored_task
@standard_retry_policy(max_retries=3, countdown=30)
def find_matching_jobs_for_resume(self, submission_id: str, top_k: int = 10, min_similarity: float = 0.3) -> Dict[str, Any]:
    """
    ДОПОЛНИТЕЛЬНАЯ ЗАДАЧА: Поиск подходящих вакансий для резюме
    
    Args:
        submission_id: ID заявки кандидата
        top_k: Количество топ-вакансий (по умолчанию 10)
        min_similarity: Минимальный порог сходства (по умолчанию 0.3)
        
    Returns:
        Словарь с результатами сопоставления
    """
    logger.info(f"🔍 Поиск вакансий для резюме {mask_sensitive_data(submission_id)}")
    
    with get_db_session() as db:
        # Валидируем submission_id
        submission_uuid = safe_uuid_convert(submission_id)
        if not submission_uuid:
            raise ValueError(f"Некорректный submission_id: {submission_id}")
        
        # Получаем заявку кандидата
        submission = SubmissionCRUD.get_by_id(db, submission_uuid)
        if not submission:
            raise ValueError(f"Заявка не найдена: {submission_id}")
        
        # Проверяем наличие эмбеддинга
        resume_collection = chroma_client.get_or_create_collection(ChromaConfig.RESUME_COLLECTION)
        job_collection = chroma_client.get_or_create_collection(ChromaConfig.JOB_COLLECTION)
        
        resume_prefix = f"resume_{submission_uuid}"
        all_resume_results = resume_collection.get(include=['embeddings'])
        
        resume_embedding = None
        for idx, resume_id in enumerate(all_resume_results['ids']):
            if resume_id.startswith(resume_prefix):
                resume_embedding = all_resume_results['embeddings'][idx]
                break
        
        if resume_embedding is None or len(resume_embedding) == 0:
            return {
                'submission_id': submission_id,
                'matches': [],
                'total_found': 0,
                'message': 'Эмбеддинг резюме не найден'
            }
        
        # Поиск похожих вакансий
        job_matches = job_collection.query(
            query_embeddings=[resume_embedding],
            n_results=top_k,
            include=['distances', 'metadatas', 'documents']
        )
        
        matches = []
        if job_matches['ids'] and job_matches['ids'][0]:
            for i, job_id_str in enumerate(job_matches['ids'][0]):
                similarity = 1.0 - job_matches['distances'][0][i]
                
                if similarity >= min_similarity:
                    # Извлекаем job_id
                    try:
                        if job_id_str.startswith('job_'):
                            parts = job_id_str.split('_')
                            job_id_num = int(parts[1]) if len(parts) >= 2 else None
                        else:
                            job_id_num = int(job_id_str)
                    except (ValueError, IndexError):
                        continue
                    
                    if job_id_num:
                        job = JobCRUD.get_by_id(db, job_id_num)
                        if job and getattr(job, 'is_active', True):
                            matches.append({
                                'job_id': job_id_num,
                                'job_title': job.title,
                                'company_id': job.company_id,
                                'similarity': round(similarity, 4),
                                'distance': round(job_matches['distances'][0][i], 4)
                            })
        
        return {
            'submission_id': submission_id,
            'candidate_id': submission.candidate_id,
            'matches': sorted(matches, key=lambda x: x['similarity'], reverse=True),
            'total_found': len(matches),
            'processed_at': datetime.utcnow().isoformat()
        }


# ================== BATCH OPERATIONS ==================

@app.task(
    bind=True, 
    name='tasks.matching.batch_find_matches_for_resumes',
    soft_time_limit=600,  # 10 минут для пакетной обработки
    time_limit=720        # 12 минут
)
@safe_task
@monitored_task
@standard_retry_policy(max_retries=3, countdown=120)
def batch_find_matches_for_resumes(self, submission_ids: List[str], top_k: int = 50, min_similarity: float = 0.5) -> Dict[str, Any]:
    """
    Пакетный поиск вакансий для множества резюме
    
    Args:
        submission_ids: Список ID заявок кандидатов
        top_k: Количество топ-вакансий для каждого резюме
        min_similarity: Минимальный порог сходства
        
    Returns:
        Словарь с результатами пакетного сопоставления
    """
    logger.info(f"🔍 Начинаем пакетный поиск вакансий для {len(submission_ids)} резюме")
    
    total_submissions = len(submission_ids)
    processed_count = 0
    successful_matches = 0
    failed_matches = 0
    results = {}
    
    for i, submission_id in enumerate(submission_ids):
        try:
            # Обновляем прогресс
            progress = int((i / total_submissions) * 100)
            self.update_state(
                state='PROGRESS',
                meta={
                    'progress': progress,
                    'status': f'Обработка резюме {i+1} из {total_submissions}',
                    'processed': processed_count,
                    'successful': successful_matches,
                    'failed': failed_matches
                }
            )
            
            # Небольшая задержка между запросами для избежания перегрузки
            if i > 0:
                import time
                time.sleep(0.1)
            
            # Вызываем задачу поиска для конкретного резюме
            result = find_matching_jobs_for_resume.apply(
                args=[submission_id, top_k, min_similarity]
            ).get()
            
            results[submission_id] = result
            successful_matches += 1
            
            logger.info(f"✅ Обработано резюме {submission_id}: найдено {result.get('total_found', 0)} вакансий")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке резюме {submission_id}: {str(e)}")
            results[submission_id] = {
                'submission_id': submission_id,
                'error': str(e),
                'matches': [],
                'total_found': 0
            }
            failed_matches += 1
        
        processed_count += 1
    
    logger.info(f"✅ Пакетная обработка завершена: {successful_matches} успешных, {failed_matches} с ошибками")
    
    summary = {
        'total_processed': processed_count,
        'successful_matches': successful_matches,
        'failed_matches': failed_matches,
        'results': results,
        'search_params': {
            'top_k': top_k,
            'min_similarity': min_similarity
        },
        'processed_at': datetime.utcnow().isoformat()
    }
    
    return summary


@app.task(
    bind=True, 
    name='tasks.matching.batch_find_matches_for_jobs',
    soft_time_limit=600,  # 10 минут для пакетной обработки
    time_limit=720        # 12 минут
)
@safe_task
@monitored_task
@standard_retry_policy(max_retries=3, countdown=120)
def batch_find_matches_for_jobs(self, job_ids: List[int], top_k: int = 50, min_similarity: float = 0.5) -> Dict[str, Any]:
    """
    Пакетный поиск резюме для множества вакансий
    
    Args:
        job_ids: Список ID вакансий
        top_k: Количество топ-резюме для каждой вакансии
        min_similarity: Минимальный порог сходства
        
    Returns:
        Словарь с результатами пакетного сопоставления
    """
    logger.info(f"🔍 Начинаем пакетный поиск резюме для {len(job_ids)} вакансий")
    
    total_jobs = len(job_ids)
    processed_count = 0
    successful_matches = 0
    failed_matches = 0
    results = {}
    
    for i, job_id in enumerate(job_ids):
        try:
            # Обновляем прогресс
            progress = int((i / total_jobs) * 100)
            self.update_state(
                state='PROGRESS',
                meta={
                    'progress': progress,
                    'status': f'Обработка вакансии {i+1} из {total_jobs}',
                    'processed': processed_count,
                    'successful': successful_matches,
                    'failed': failed_matches
                }
            )
            
            # Небольшая задержка между запросами для избежания перегрузки
            if i > 0:
                import time
                time.sleep(0.1)
            
            # Вызываем задачу поиска для конкретной вакансии
            result = find_matching_resumes_for_job.apply(
                args=[job_id, top_k, min_similarity]
            ).get()
            
            results[str(job_id)] = result
            successful_matches += 1
            
            logger.info(f"✅ Обработана вакансия {job_id}: найдено {result.get('total_found', 0)} резюме")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке вакансии {job_id}: {str(e)}")
            results[str(job_id)] = {
                'job_id': job_id,
                'error': str(e),
                'matches': [],
                'total_found': 0
            }
            failed_matches += 1
        
        processed_count += 1
    
    logger.info(f"✅ Пакетная обработка завершена: {successful_matches} успешных, {failed_matches} с ошибками")
    
    summary = {
        'total_processed': processed_count,
        'successful_matches': successful_matches,
        'failed_matches': failed_matches,
        'results': results,
        'search_params': {
            'top_k': top_k,
            'min_similarity': min_similarity
        },
        'processed_at': datetime.utcnow().isoformat()
    }
    
    return summary


# ================== SIMPLE VERSIONS (без обновления состояния) ==================

@app.task(
    bind=True, 
    name='tasks.matching.find_matching_jobs_for_resume_simple',
    soft_time_limit=180,  # 3 минуты для простых задач
    time_limit=240        # 4 минуты
)
@safe_task
@monitored_task
@standard_retry_policy(max_retries=2, countdown=20)
def find_matching_jobs_for_resume_simple(self, submission_id: str, top_k: int = 100, min_similarity: float = 0.5) -> Dict[str, Any]:
    """
    Упрощенный поиск наиболее подходящих вакансий для резюме (без обновлений состояния)
    
    Быстрая версия без детального логирования прогресса.
    """
    logger.info(f"🔍 Быстрый поиск вакансий для резюме {mask_sensitive_data(submission_id)}")
    
    with get_db_session() as db:
        # Проверяем доступность ChromaDB
        if not chroma_client.health_check():
            logger.error("❌ ChromaDB недоступен")
            raise Exception("ChromaDB недоступен")
        
        # Валидируем submission_id
        submission_uuid = safe_uuid_convert(submission_id)
        if not submission_uuid:
            logger.error(f"❌ Некорректный submission_id: {submission_id}")
            raise ValueError(f"Некорректный submission_id: {submission_id}")
        
        # Получаем заявку кандидата
        submission = SubmissionCRUD.get_by_id(db, submission_uuid)
        
        if not submission or not getattr(submission, 'resume_raw_text', None):
            logger.warning(f"⚠️ Отсутствует текст резюме для заявки {submission_id}")
            return {
                'submission_id': submission_id,
                'matches': [],
                'total_found': 0,
                'message': 'Отсутствует текст резюме'
            }
        
        # Быстрый поиск в ChromaDB
        resume_collection = chroma_client.get_or_create_collection(ChromaConfig.RESUME_COLLECTION)
        job_collection = chroma_client.get_or_create_collection(ChromaConfig.JOB_COLLECTION)
        
        resume_prefix = f"resume_{submission_uuid}"
        all_resume_results = resume_collection.get(include=['embeddings'])
        
        resume_embedding = None
        for idx, resume_id in enumerate(all_resume_results['ids']):
            if resume_id.startswith(resume_prefix):
                resume_embedding = all_resume_results['embeddings'][idx]
                break
        
        if not resume_embedding:
            return {
                'submission_id': submission_id,
                'matches': [],
                'total_found': 0,
                'message': 'Эмбеддинг резюме не найден'
            }
        
        # Поиск вакансий
        job_matches = job_collection.query(
            query_embeddings=[resume_embedding],
            n_results=top_k,
            include=['distances', 'metadatas']
        )
        
        matches = []
        if job_matches['ids'] and job_matches['ids'][0]:
            for i, job_id_str in enumerate(job_matches['ids'][0]):
                similarity = 1.0 - job_matches['distances'][0][i]
                
                if similarity >= min_similarity:
                    try:
                        if job_id_str.startswith('job_'):
                            job_id_num = int(job_id_str.split('_')[1])
                        else:
                            job_id_num = int(job_id_str)
                        
                        matches.append({
                            'job_id': job_id_num,
                            'similarity': round(similarity, 4),
                            'distance': round(job_matches['distances'][0][i], 4)
                        })
                    except (ValueError, IndexError):
                        continue
        
        return {
            'submission_id': submission_id,
            'matches': sorted(matches, key=lambda x: x['similarity'], reverse=True),
            'total_found': len(matches),
            'processed_at': datetime.utcnow().isoformat()
        }


@app.task(
    bind=True, 
    name='tasks.matching.find_matching_resumes_for_job_simple',
    soft_time_limit=180,  # 3 минуты для простых задач
    time_limit=240        # 4 минуты
)
@safe_task
@monitored_task
@standard_retry_policy(max_retries=2, countdown=20)
def find_matching_resumes_for_job_simple(self, job_id: int, top_k: int = 50, min_similarity: float = 0.4) -> Dict[str, Any]:
    """
    Упрощенный поиск наиболее подходящих резюме для вакансии (без обновлений состояния)
    
    Быстрая версия без детального логирования прогресса.
    """
    logger.info(f"🎯 Быстрый поиск резюме для вакансии {job_id}")
    
    with get_db_session() as db:
        # Проверяем доступность ChromaDB
        if not chroma_client.health_check():
            raise Exception("ChromaDB недоступен")
        
        # Валидируем job_id
        if not isinstance(job_id, int) or job_id <= 0:
            raise ValueError(f"Некорректный job_id: {job_id}")
        
        # Получаем вакансию
        job = JobCRUD.get_by_id(db, job_id)
        if not job:
            raise ValueError(f"Вакансия не найдена: {job_id}")
        
        # Быстрый поиск в ChromaDB
        resume_collection = chroma_client.get_or_create_collection(ChromaConfig.RESUME_COLLECTION)
        job_collection = chroma_client.get_or_create_collection(ChromaConfig.JOB_COLLECTION)
        
        job_prefix = f"job_{job_id}"
        all_job_results = job_collection.get(include=['embeddings'])
        
        job_embedding = None
        for idx, job_id_str in enumerate(all_job_results['ids']):
            if job_id_str.startswith(job_prefix):
                job_embedding = all_job_results['embeddings'][idx]
                break
        
        if not job_embedding:
            return {
                'job_id': job_id,
                'matches': [],
                'total_found': 0,
                'message': 'Эмбеддинг вакансии не найден'
            }
        
        # Поиск резюме
        resume_matches = resume_collection.query(
            query_embeddings=[job_embedding],
            n_results=top_k,
            include=['distances', 'metadatas']
        )
        
        matches = []
        if resume_matches['ids'] and resume_matches['ids'][0]:
            for i, submission_id_str in enumerate(resume_matches['ids'][0]):
                similarity = 1.0 - resume_matches['distances'][0][i]
                
                if similarity >= min_similarity:
                    if submission_id_str.startswith('resume_'):
                        parts = submission_id_str.split('_')
                        if len(parts) >= 2:
                            submission_uuid = safe_uuid_convert(parts[1])
                            if submission_uuid:
                                matches.append({
                                    'submission_id': str(submission_uuid),
                                    'similarity': round(similarity, 4),
                                    'distance': round(resume_matches['distances'][0][i], 4)
                                })
        
        return {
            'job_id': job_id,
            'job_title': job.title,
            'matches': sorted(matches, key=lambda x: x['similarity'], reverse=True),
            'total_found': len(matches),
            'processed_at': datetime.utcnow().isoformat()
        }


# ================== ALIASES FOR BACKWARD COMPATIBILITY ==================

# Алиасы для обратной совместимости с existing imports
batch_find_matches_for_resumes = batch_find_matches_for_resumes
batch_find_matches_for_jobs = batch_find_matches_for_jobs
find_matching_jobs_for_resume_simple = find_matching_jobs_for_resume_simple
find_matching_resumes_for_job_simple = find_matching_resumes_for_job_simple
