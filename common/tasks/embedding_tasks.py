"""
Celery задачи для работы с эмбеддингами (очищенная версия)
"""

import os
import sys
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any
import traceback

from celery import current_task, shared_task
from celery.utils.log import get_task_logger

# Абсолютные импорты для избежания проблем с путями
from common.database.config import database
from common.database.operations.embedding_operations import embedding_crud
from common.database.operations.candidate_operations import SubmissionCRUD
from common.database.operations.company_operations import JobCRUD
from common.models.candidates import Submission
from common.models.companies import Job
from common.models.embeddings import EmbeddingMetadata
from common.utils.chroma_config import chroma_client, ChromaConfig
from common.utils.text_preprocessing import preprocess_resume_text, preprocess_job_description_text, preprocess_text_with_stats

# Импортируем Celery app напрямую
from common.celery_app.celery_app import celery_app

logger = get_task_logger(__name__)


@celery_app.task(bind=True, name='common.tasks.embedding_tasks.generate_resume_embeddings')
def generate_resume_embeddings(self, submission_ids: Optional[List[str]] = None):
    """
    Генерация эмбеддингов для резюме кандидатов
    
    Args:
        submission_ids: Список ID заявок для обработки. Если None, обрабатываются все заявки с сырым текстом
    """
    logger.info("🔄 Начинаем генерацию эмбеддингов для резюме")
    
    # Исправление: фильтруем входные данные, если они не список строк (UUID)
    if not (isinstance(submission_ids, list) and all(isinstance(x, str) for x in submission_ids)):
        submission_ids = None
    
    db = database.get_session()
    try:
        # Обновляем прогресс
        self.update_state(state='PROGRESS', meta={'progress': 5, 'status': 'Инициализация'})
        
        # Проверяем доступность ChromaDB
        if not chroma_client.health_check():
            logger.error("❌ ChromaDB недоступен")
            raise Exception("ChromaDB недоступен. Убедитесь, что сервер запущен.")
        
        # Получаем коллекцию для резюме
        collection = chroma_client.get_resume_collection()
        
        # Обновляем прогресс
        self.update_state(state='PROGRESS', meta={'progress': 10, 'status': 'Загрузка данных'})
        
        # Получаем заявки для обработки
        if submission_ids:
            submissions = []
            for submission_id in submission_ids:
                try:
                    submission_uuid = uuid.UUID(submission_id)
                    submission = SubmissionCRUD().get_by_id(db, submission_uuid)
                    if submission and getattr(submission, 'resume_raw_text', None):
                        submissions.append(submission)
                except ValueError:
                    logger.warning(f"Неверный формат UUID для заявки: {submission_id}")
                    continue
        else:
            # Получаем все заявки с сырым текстом, которые еще не обработаны
            all_submissions = db.query(Submission).filter(
                Submission.resume_raw_text.isnot(None),
                Submission.resume_raw_text != ''
            ).all()
            
            # Фильтруем те, для которых еще нет эмбеддингов
            submission_string_ids = [str(sub.submission_id) for sub in all_submissions]
            unprocessed_ids = embedding_crud.get_sources_without_embeddings(
                db, 'resume', submission_string_ids
            )
            submissions = [sub for sub in all_submissions if str(sub.submission_id) in unprocessed_ids]
        
        if not submissions:
            logger.info("✅ Нет новых резюме для обработки")
            return {
                'status': 'completed',
                'processed_count': 0,
                'message': 'Нет новых резюме для обработки'
            }
        
        logger.info(f"📊 Найдено {len(submissions)} резюме для обработки")
        
        processed_count = 0
        failed_count = 0
        
        for i, submission in enumerate(submissions):
            try:
                # Обновляем прогресс
                progress = 10 + (i / len(submissions)) * 80
                self.update_state(
                    state='PROGRESS', 
                    meta={
                        'progress': int(progress),
                        'status': f'Обработка резюме {i+1}/{len(submissions)}',
                        'current_submission_id': str(submission.submission_id)
                    }
                )
                
                # Генерируем уникальный ID для документа в ChromaDB
                chroma_doc_id = f"resume_{submission.submission_id}_{uuid.uuid4().hex[:8]}"
                
                # Получаем и предобрабатываем текст резюме
                raw_text = getattr(submission, 'resume_raw_text', '')
                processed_text, preprocessing_stats = preprocess_text_with_stats(
                    raw_text, 
                    config=None  # Используем конфигурацию по умолчанию для резюме
                )
                
                # Логируем статистику предобработки
                logger.info(f"📝 Предобработка резюме {submission.submission_id}: "
                          f"было {preprocessing_stats['original_length']} символов, "
                          f"стало {preprocessing_stats['processed_length']} символов "
                          f"(сжатие: {preprocessing_stats['compression_ratio']:.2%})")
                
                # Подготавливаем метаданные
                metadata = {
                    'submission_id': str(submission.submission_id),
                    'candidate_id': submission.candidate_id,
                    'source_type': 'resume',
                    'created_at': datetime.now().isoformat(),
                    'model': ChromaConfig.EMBEDDING_MODEL,
                    'original_length': preprocessing_stats['original_length'],
                    'processed_length': preprocessing_stats['processed_length'],
                    'compression_ratio': round(preprocessing_stats['compression_ratio'], 4)
                }
                
                # Добавляем дополнительную информацию если есть
                if submission.candidate:
                    metadata.update({
                        'candidate_name': f"{submission.candidate.first_name} {submission.candidate.last_name}",
                        'candidate_email': submission.candidate.email
                    })
                
                # Добавляем документ в ChromaDB (используем обработанный текст)
                collection.add(
                    documents=[processed_text],
                    metadatas=[metadata],
                    ids=[chroma_doc_id]
                )
                
                # Сохраняем метаданные в PostgreSQL (используем обработанный текст)
                embedding_crud.create_embedding_metadata(
                    db=db,
                    source_type='resume',
                    source_id=str(submission.submission_id),
                    chroma_document_id=chroma_doc_id,
                    collection_name=ChromaConfig.RESUME_COLLECTION,
                    text_content=processed_text,  # Сохраняем обработанный текст
                    model_name=ChromaConfig.EMBEDDING_MODEL,
                    additional_metadata=metadata
                )
                
                processed_count += 1
                logger.info(f"✅ Обработано резюме {submission.submission_id}")
                
            except Exception as e:
                failed_count += 1
                logger.error(f"❌ Ошибка обработки резюме {submission.submission_id}: {str(e)}")
                continue
        
        # Финальное обновление прогресса
        self.update_state(
            state='SUCCESS',
            meta={
                'progress': 100,
                'status': 'Завершено',
                'processed_count': processed_count,
                'failed_count': failed_count,
                'total_count': len(submissions)
            }
        )
        
        logger.info(f"🎉 Генерация эмбеддингов завершена. Обработано: {processed_count}, ошибок: {failed_count}")
        
        return {
            'status': 'completed',
            'processed_count': processed_count,
            'failed_count': failed_count,
            'total_count': len(submissions)
        }
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при генерации эмбеддингов резюме: {str(e)}")
        tb = traceback.format_exc()
        self.update_state(
            state='FAILURE',
            meta={
                'progress': 0,
                'status': f'Ошибка: {str(e)}',
                'error_type': type(e).__name__,
                'error_message': str(e),
                'traceback': tb
            }
        )
        return {
            'status': 'failed',
            'error_type': type(e).__name__,
            'error_message': str(e),
            'traceback': tb
        }
    finally:
        db.close()


@celery_app.task(bind=True, name='common.tasks.embedding_tasks.generate_job_embeddings')
def generate_job_embeddings(self, job_ids: Optional[List[int]] = None):
    """
    Генерация эмбеддингов для описаний вакансий
    
    Args:
        job_ids: Список ID вакансий для обработки. Если None, обрабатываются все вакансии с сырым текстом
    """
    logger.info("🔄 Начинаем генерацию эмбеддингов для вакансий")
    
    # Исправление: фильтруем входные данные, если они не список int
    if not (isinstance(job_ids, list) and all(isinstance(x, int) for x in job_ids)):
        job_ids = None
    
    db = database.get_session()
    try:
        # Обновляем прогресс
        self.update_state(state='PROGRESS', meta={'progress': 5, 'status': 'Инициализация'})
        
        # Проверяем доступность ChromaDB
        if not chroma_client.health_check():
            logger.error("❌ ChromaDB недоступен")
            raise Exception("ChromaDB недоступен. Убедитесь, что сервер запущен.")
        
        # Получаем коллекцию для вакансий
        collection = chroma_client.get_job_collection()
        
        # Обновляем прогресс
        self.update_state(state='PROGRESS', meta={'progress': 10, 'status': 'Загрузка данных'})
        
        # Получаем вакансии для обработки
        if job_ids:
            jobs = []
            for job_id in job_ids:
                job = JobCRUD().get_by_id(db, job_id)
                if job and getattr(job, 'job_description_raw_text', None):
                    jobs.append(job)
        else:
            # Получаем все вакансии с сырым текстом, которые еще не обработаны
            all_jobs = db.query(Job).filter(
                Job.job_description_raw_text.isnot(None),
                Job.job_description_raw_text != ''
            ).all()
            
            # Фильтруем те, для которых еще нет эмбеддингов
            job_string_ids = [str(job.job_id) for job in all_jobs]
            unprocessed_ids = embedding_crud.get_sources_without_embeddings(
                db, 'job_description', job_string_ids
            )
            jobs = [job for job in all_jobs if str(job.job_id) in unprocessed_ids]
        
        if not jobs:
            logger.info("✅ Нет новых вакансий для обработки")
            return {
                'status': 'completed',
                'processed_count': 0,
                'message': 'Нет новых вакансий для обработки'
            }
        
        logger.info(f"📊 Найдено {len(jobs)} вакансий для обработки")
        
        processed_count = 0
        failed_count = 0
        
        for i, job in enumerate(jobs):
            try:
                # Обновляем прогресс
                progress = 10 + (i / len(jobs)) * 80
                self.update_state(
                    state='PROGRESS', 
                    meta={
                        'progress': int(progress),
                        'status': f'Обработка вакансии {i+1}/{len(jobs)}',
                        'current_job_id': job.job_id
                    }
                )
                
                # Генерируем уникальный ID для документа в ChromaDB
                chroma_doc_id = f"job_{job.job_id}_{uuid.uuid4().hex[:8]}"
                
                # Получаем и предобрабатываем текст описания вакансии
                raw_text = getattr(job, 'job_description_raw_text', '')
                processed_text, preprocessing_stats = preprocess_text_with_stats(
                    raw_text,
                    config={
                        'remove_extra_whitespace': True,
                        'normalize_line_breaks': True,
                        'remove_duplicates': True,
                        'min_sentence_length': 10,
                        'normalize_unicode': True,
                        'preserve_structure': True,
                        'remove_empty_lines': True,
                        'max_consecutive_newlines': 2
                    }
                )
                
                # Логируем статистику предобработки
                logger.info(f"📝 Предобработка вакансии {job.job_id}: "
                          f"было {preprocessing_stats['original_length']} символов, "
                          f"стало {preprocessing_stats['processed_length']} символов "
                          f"(сжатие: {preprocessing_stats['compression_ratio']:.2%})")
                
                # Подготавливаем метаданные
                metadata = {
                    'job_id': job.job_id,
                    'company_id': job.company_id,
                    'source_type': 'job_description',
                    'created_at': datetime.now().isoformat(),
                    'model': ChromaConfig.EMBEDDING_MODEL,
                    'job_title': job.title or '',
                    'employment_type': job.employment_type or '',
                    'experience_level': job.experience_level or '',
                    'location': job.location or '',
                    'is_active': job.is_active if job.is_active is not None else True,
                    'original_length': preprocessing_stats['original_length'],
                    'processed_length': preprocessing_stats['processed_length'],
                    'compression_ratio': round(preprocessing_stats['compression_ratio'], 4)
                }
                
                # Добавляем информацию о компании если есть
                if job.company:
                    metadata.update({
                        'company_name': job.company.name or '',
                        'company_website': job.company.website or ''
                    })
                
                # Добавляем документ в ChromaDB (используем обработанный текст)
                collection.add(
                    documents=[processed_text],
                    metadatas=[metadata],
                    ids=[chroma_doc_id]
                )
                
                # Сохраняем метаданные в PostgreSQL (используем обработанный текст)
                embedding_crud.create_embedding_metadata(
                    db=db,
                    source_type='job_description',
                    source_id=str(job.job_id),
                    chroma_document_id=chroma_doc_id,
                    collection_name=ChromaConfig.JOB_COLLECTION,
                    text_content=processed_text,  # Сохраняем обработанный текст
                    model_name=ChromaConfig.EMBEDDING_MODEL,
                    additional_metadata=metadata
                )
                
                processed_count += 1
                logger.info(f"✅ Обработана вакансия {job.job_id}")
                
            except Exception as e:
                failed_count += 1
                logger.error(f"❌ Ошибка обработки вакансии {job.job_id}: {str(e)}")
                continue
        
        # Финальное обновление прогресса
        self.update_state(
            state='SUCCESS',
            meta={
                'progress': 100,
                'status': 'Завершено',
                'processed_count': processed_count,
                'failed_count': failed_count,
                'total_count': len(jobs)
            }
        )
        
        logger.info(f"🎉 Генерация эмбеддингов завершена. Обработано: {processed_count}, ошибок: {failed_count}")
        
        return {
            'status': 'completed',
            'processed_count': processed_count,
            'failed_count': failed_count,
            'total_count': len(jobs)
        }
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при генерации эмбеддингов вакансий: {str(e)}")
        tb = traceback.format_exc()
        self.update_state(
            state='FAILURE',
            meta={
                'progress': 0,
                'status': f'Ошибка: {str(e)}',
                'error_type': type(e).__name__,
                'error_message': str(e),
                'traceback': tb
            }
        )
        return {
            'status': 'failed',
            'error_type': type(e).__name__,
            'error_message': str(e),
            'traceback': tb
        }
    finally:
        db.close()


@celery_app.task(bind=True, name='common.tasks.embedding_tasks.search_similar_resumes')
def search_similar_resumes(self, query_text: str, limit: int = 10, min_similarity: float = 0.7):
    """
    Поиск похожих резюме по текстовому запросу
    
    Args:
        query_text: Текст запроса для поиска
        limit: Максимальное количество результатов
        min_similarity: Минимальный порог схожести (0.0 - 1.0)
    """
    logger.info(f"🔍 Поиск похожих резюме для запроса: {query_text[:100]}...")
    
    db = database.get_session()
    try:
        # Проверяем доступность ChromaDB
        if not chroma_client.health_check():
            raise Exception("ChromaDB недоступен")
        
        # Получаем коллекцию резюме
        collection = chroma_client.get_resume_collection()
        
        # Выполняем поиск
        results = collection.query(
            query_texts=[query_text],
            n_results=limit,
            include=['documents', 'metadatas', 'distances']
        )
        
        # Обрабатываем результаты
        similar_resumes = []
        if results['documents'] and results['documents'][0]:
            for i, (doc, metadata, distance) in enumerate(zip(
                results['documents'][0],
                results['metadatas'][0],
                results['distances'][0]
            )):
                # Конвертируем distance в similarity (ChromaDB возвращает расстояние, а не схожесть)
                similarity = 1 - distance
                
                if similarity >= min_similarity:
                    similar_resumes.append({
                        'submission_id': metadata.get('submission_id'),
                        'candidate_id': metadata.get('candidate_id'),
                        'candidate_name': metadata.get('candidate_name'),
                        'candidate_email': metadata.get('candidate_email'),
                        'similarity': round(similarity, 3),
                        'text_preview': doc[:200] + '...' if len(doc) > 200 else doc
                    })
        
        logger.info(f"✅ Найдено {len(similar_resumes)} похожих резюме")
        
        return {
            'status': 'completed',
            'query': query_text,
            'results_count': len(similar_resumes),
            'similar_resumes': similar_resumes
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка поиска похожих резюме: {str(e)}")
        raise
    finally:
        db.close()


@celery_app.task(bind=True, name='common.tasks.embedding_tasks.search_similar_jobs')
def search_similar_jobs(self, query_text: str, limit: int = 10, min_similarity: float = 0.7):
    """
    Поиск похожих вакансий по текстовому запросу
    
    Args:
        query_text: Текст запроса для поиска
        limit: Максимальное количество результатов
        min_similarity: Минимальный порог схожести (0.0 - 1.0)
    """
    logger.info(f"🔍 Поиск похожих вакансий для запроса: {query_text[:100]}...")
    
    db = database.get_session()
    try:
        # Проверяем доступность ChromaDB
        if not chroma_client.health_check():
            raise Exception("ChromaDB недоступен")
        
        # Получаем коллекцию вакансий
        collection = chroma_client.get_job_collection()
        
        # Выполняем поиск
        results = collection.query(
            query_texts=[query_text],
            n_results=limit,
            include=['documents', 'metadatas', 'distances']
        )
        
        # Обрабатываем результаты
        similar_jobs = []
        if results['documents'] and results['documents'][0]:
            for i, (doc, metadata, distance) in enumerate(zip(
                results['documents'][0],
                results['metadatas'][0],
                results['distances'][0]
            )):
                # Конвертируем distance в similarity
                similarity = 1 - distance
                
                if similarity >= min_similarity:
                    similar_jobs.append({
                        'job_id': metadata.get('job_id'),
                        'company_id': metadata.get('company_id'),
                        'company_name': metadata.get('company_name'),
                        'job_title': metadata.get('job_title'),
                        'employment_type': metadata.get('employment_type'),
                        'experience_level': metadata.get('experience_level'),
                        'location': metadata.get('location'),
                        'is_active': metadata.get('is_active'),
                        'similarity': round(similarity, 3),
                        'text_preview': doc[:200] + '...' if len(doc) > 200 else doc
                    })
        
        logger.info(f"✅ Найдено {len(similar_jobs)} похожих вакансий")
        
        return {
            'status': 'completed',
            'query': query_text,
            'results_count': len(similar_jobs),
            'similar_jobs': similar_jobs
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка поиска похожих вакансий: {str(e)}")
        raise
    finally:
        db.close()


@celery_app.task(bind=True, name='common.tasks.embedding_tasks.generate_all_embeddings')
def generate_all_embeddings(self, previous_results=None) -> Dict[str, Any]:
    """
    Генерация всех эмбеддингов: резюме и вакансий
    (Оркестрация теперь на уровне workflow, задача только для совместимости)
    """
    logger.info("🔄 Вызвана задача generate_all_embeddings (логическая точка в pipeline, без запуска вложенных задач)")
    return {
        'status': 'skipped',
        'message': 'Генерация всех эмбеддингов теперь orchestrated на уровне workflow',
        'timestamp': datetime.now().isoformat(),
        'previous_results': previous_results
    }
