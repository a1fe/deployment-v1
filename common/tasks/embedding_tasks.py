"""
Celery задачи для работы с эмбеддингами
"""

import os
import sys
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any

# Добавляем корневую папку в путь
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from celery import current_task, shared_task
from celery.utils.log import get_task_logger
from database.config import database
from database.operations.embedding_operations import embedding_crud
from database.operations.candidate_operations import SubmissionCRUD
from database.operations.company_operations import JobCRUD
from models.candidates import Submission
from models.companies import Job
from models.embeddings import EmbeddingMetadata
from utils.chroma_config import chroma_client, ChromaConfig
from utils.text_preprocessing import preprocess_resume_text, preprocess_job_description_text, preprocess_text_with_stats

# Импортируем Celery app только когда он будет создан
def get_celery_app():
    from celery_app.celery_config import celery_app
    return celery_app

logger = get_task_logger(__name__)


@get_celery_app().task(bind=True, name='tasks.embedding_tasks.generate_resume_embeddings')
def generate_resume_embeddings(self, submission_ids: Optional[List[str]] = None):
    """
    Генерация эмбеддингов для резюме кандидатов
    
    Args:
        submission_ids: Список ID заявок для обработки. Если None, обрабатываются все заявки с сырым текстом
    """
    logger.info("🔄 Начинаем генерацию эмбеддингов для резюме")
    
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
                    'preprocessing_stats': preprocessing_stats,
                    'original_length': preprocessing_stats['original_length'],
                    'processed_length': preprocessing_stats['processed_length']
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
        self.update_state(
            state='FAILURE',
            meta={
                'progress': 0,
                'status': f'Ошибка: {str(e)}',
                'error': str(e)
            }
        )
        raise
    finally:
        db.close()


@get_celery_app().task(bind=True, name='tasks.embedding_tasks.generate_job_embeddings')
def generate_job_embeddings(self, job_ids: Optional[List[int]] = None):
    """
    Генерация эмбеддингов для описаний вакансий
    
    Args:
        job_ids: Список ID вакансий для обработки. Если None, обрабатываются все вакансии с сырым текстом
    """
    logger.info("🔄 Начинаем генерацию эмбеддингов для вакансий")
    
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
                    'preprocessing_stats': preprocessing_stats,
                    'original_length': preprocessing_stats['original_length'],
                    'processed_length': preprocessing_stats['processed_length']
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
        self.update_state(
            state='FAILURE',
            meta={
                'progress': 0,
                'status': f'Ошибка: {str(e)}',
                'error': str(e)
            }
        )
        raise
    finally:
        db.close()


@get_celery_app().task(bind=True, name='tasks.embedding_tasks.search_similar_resumes')
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


@get_celery_app().task(bind=True, name='tasks.embedding_tasks.search_similar_jobs')
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


@get_celery_app().task(bind=True, name='tasks.embedding_tasks.cleanup_embeddings')
def cleanup_embeddings(self):
    """
    Очистка устаревших эмбеддингов
    Удаляет эмбеддинги для несуществующих записей
    """
    logger.info("🧹 Начинаем очистку устаревших эмбеддингов")
    
    db = database.get_session()
    try:
        deleted_count = 0
        
        # Очистка эмбеддингов резюме
        resume_embeddings = embedding_crud.get_by_collection(db, ChromaConfig.RESUME_COLLECTION)
        resume_collection = chroma_client.get_resume_collection()
        
        for embedding in resume_embeddings:
            # Проверяем, существует ли заявка
            try:
                submission_uuid = uuid.UUID(getattr(embedding, 'source_id'))
                submission = SubmissionCRUD().get_by_id(db, submission_uuid)
                if not submission or not getattr(submission, 'resume_raw_text', None):
                    # Удаляем из ChromaDB
                    try:
                        resume_collection.delete(ids=[getattr(embedding, 'chroma_document_id')])
                    except:
                        pass  # Документ может уже не существовать в ChromaDB
                    
                    # Удаляем из PostgreSQL
                    db.delete(embedding)
                    deleted_count += 1
                    logger.info(f"🗑️ Удален эмбеддинг для несуществующего резюме {getattr(embedding, 'source_id')}")
            except ValueError:
                # Неверный UUID, удаляем эмбеддинг
                try:
                    resume_collection.delete(ids=[getattr(embedding, 'chroma_document_id')])
                except:
                    pass
                db.delete(embedding)
                deleted_count += 1
        
        # Очистка эмбеддингов вакансий
        job_embeddings = embedding_crud.get_by_collection(db, ChromaConfig.JOB_COLLECTION)
        job_collection = chroma_client.get_job_collection()
        
        for embedding in job_embeddings:
            # Проверяем, существует ли вакансия
            try:
                job_id = int(getattr(embedding, 'source_id'))
                job = JobCRUD().get_by_id(db, job_id)
                if not job or not getattr(job, 'job_description_raw_text', None):
                    # Удаляем из ChromaDB
                    try:
                        job_collection.delete(ids=[getattr(embedding, 'chroma_document_id')])
                    except:
                        pass  # Документ может уже не существовать в ChromaDB
                    
                    # Удаляем из PostgreSQL
                    db.delete(embedding)
                    deleted_count += 1
                    logger.info(f"🗑️ Удален эмбеддинг для несуществующей вакансии {getattr(embedding, 'source_id')}")
            except (ValueError, TypeError):
                # Неверный ID, удаляем эмбеддинг
                try:
                    job_collection.delete(ids=[getattr(embedding, 'chroma_document_id')])
                except:
                    pass
                db.delete(embedding)
                deleted_count += 1
        
        db.commit()
        
        logger.info(f"✅ Очистка завершена. Удалено {deleted_count} устаревших эмбеддингов")
        
        return {
            'status': 'completed',
            'deleted_count': deleted_count
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка очистки эмбеддингов: {str(e)}")
        raise
    finally:
        db.close()


@get_celery_app().task(bind=True, name='tasks.embedding_tasks.recreate_all_embeddings_with_preprocessing')
def recreate_all_embeddings_with_preprocessing(self, force_recreate: bool = False):
    """
    Пересоздание всех эмбеддингов с применением предобработки текста
    
    Args:
        force_recreate: Если True, пересоздаёт все эмбеддинги принудительно.
                       Если False, пересоздаёт только те, которые были созданы без предобработки.
    """
    logger.info("🔄 Начинаем пересоздание всех эмбеддингов с предобработкой")
    
    db = database.get_session()
    try:
        # Обновляем прогресс
        self.update_state(state='PROGRESS', meta={'progress': 5, 'status': 'Инициализация и проверка систем'})
        
        # Проверяем доступность ChromaDB
        if not chroma_client.health_check():
            logger.error("❌ ChromaDB недоступен")
            raise Exception("ChromaDB недоступен")
        
        # Получаем коллекции
        config = ChromaConfig()
        resume_collection = chroma_client.get_collection(config.resume_collection_name)
        job_collection = chroma_client.get_collection(config.job_collection_name)
        
        # Обновляем прогресс
        self.update_state(state='PROGRESS', meta={'progress': 10, 'status': 'Подсчёт данных для обработки'})
        
        # Подсчитываем количество заявок и вакансий
        submission_crud = SubmissionCRUD()
        job_crud = JobCRUD()
        
        # Получаем все заявки с сырым текстом
        all_submissions = db.query(Submission).filter(
            Submission.resume_raw_text.isnot(None),
            Submission.resume_raw_text != ''
        ).all()
        
        # Получаем все вакансии с описанием
        all_jobs = db.query(Job).filter(
            Job.description.isnot(None),
            Job.description != ''
        ).all()
        
        total_submissions = len(all_submissions)
        total_jobs = len(all_jobs)
        total_items = total_submissions + total_jobs
        
        logger.info(f"📊 Найдено для обработки: {total_submissions} резюме, {total_jobs} вакансий")
        
        if total_items == 0:
            logger.info("ℹ️ Нет данных для обработки")
            return {
                'status': 'completed',
                'processed_resumes': 0,
                'processed_jobs': 0,
                'errors': 0
            }
        
        # Статистика обработки
        processed_resumes = 0
        processed_jobs = 0
        errors = 0
        
        # Обрабатываем резюме
        logger.info("🔄 Начинаем пересоздание эмбеддингов для резюме")
        
        for i, submission in enumerate(all_submissions):
            try:
                progress = 10 + (i / total_items) * 80
                self.update_state(
                    state='PROGRESS',
                    meta={
                        'progress': int(progress),
                        'status': f'Обработка резюме {i + 1}/{total_submissions}',
                        'current_item': f"{submission.candidate.first_name} {submission.candidate.last_name}",
                        'processed_resumes': processed_resumes,
                        'processed_jobs': processed_jobs,
                        'errors': errors
                    }
                )
                
                # Проверяем, нужно ли пересоздавать эмбеддинг
                should_recreate = force_recreate
                
                if not force_recreate:
                    # Проверяем, есть ли уже эмбеддинг и был ли он создан с предобработкой
                    existing_embedding = embedding_crud.get_by_source_id_and_type(
                        db, str(submission.submission_id), 'resume'
                    )
                    
                    if existing_embedding:
                        # Проверяем метаданные - если нет информации о предобработке, пересоздаём
                        try:
                            results = resume_collection.get(
                                ids=[existing_embedding.chroma_document_id],
                                include=['metadatas']
                            )
                            
                            if results['metadatas'] and len(results['metadatas']) > 0:
                                metadata = results['metadatas'][0]
                                # Если нет информации о предобработке, пересоздаём
                                if 'preprocessing_stats' not in metadata:
                                    should_recreate = True
                                    logger.info(f"🔄 Эмбеддинг для резюме {submission.submission_id} будет пересоздан (без предобработки)")
                                else:
                                    logger.info(f"✅ Эмбеддинг для резюме {submission.submission_id} уже создан с предобработкой")
                            else:
                                should_recreate = True
                        except Exception as e:
                            logger.warning(f"⚠️ Ошибка при проверке метаданных для резюме {submission.submission_id}: {e}")
                            should_recreate = True
                    else:
                        should_recreate = True
                
                if should_recreate:
                    # Удаляем существующий эмбеддинг если есть
                    existing_embedding = embedding_crud.get_by_source_id_and_type(
                        db, str(submission.submission_id), 'resume'
                    )
                    
                    if existing_embedding:
                        try:
                            resume_collection.delete(ids=[existing_embedding.chroma_document_id])
                        except Exception as e:
                            logger.warning(f"⚠️ Не удалось удалить эмбеддинг из ChromaDB: {e}")
                        
                        db.delete(existing_embedding)
                    
                    # Предобрабатываем текст
                    processed_text, preprocessing_stats = preprocess_text_with_stats(
                        submission.resume_raw_text,
                        config=None  # Используем стандартную конфигурацию для резюме
                    )
                    
                    logger.info(f"📄 Предобработка резюме для {submission.candidate.first_name} {submission.candidate.last_name}: "
                              f"было {preprocessing_stats['original_length']} символов, "
                              f"стало {preprocessing_stats['processed_length']} символов "
                              f"(сжатие: {preprocessing_stats['compression_ratio']:.2%})")
                    
                    # Создаём эмбеддинг
                    embedding_response = chroma_client.create_embedding(processed_text)
                    
                    if embedding_response and 'embedding' in embedding_response:
                        chroma_doc_id = str(uuid.uuid4())
                        
                        # Метаданные с информацией о предобработке
                        metadata = {
                            'submission_id': str(submission.submission_id),
                            'candidate_name': f"{submission.candidate.first_name} {submission.candidate.last_name}",
                            'candidate_email': submission.candidate.email or '',
                            'preprocessing_stats': preprocessing_stats,
                            'original_length': preprocessing_stats['original_length'],
                            'processed_length': preprocessing_stats['processed_length'],
                            'compression_ratio': preprocessing_stats['compression_ratio'],
                            'created_with_preprocessing': True,
                            'preprocessing_version': '1.0',
                            'created_at': datetime.now().isoformat()
                        }
                        
                        # Добавляем в ChromaDB
                        resume_collection.add(
                            documents=[processed_text],
                            embeddings=[embedding_response['embedding']],
                            metadatas=[metadata],
                            ids=[chroma_doc_id]
                        )
                        
                        # Сохраняем в PostgreSQL
                        embedding_crud.create(db, {
                            'source_id': str(submission.submission_id),
                            'source_type': 'resume',
                            'chroma_document_id': chroma_doc_id,
                            'text_content': processed_text,
                            'metadata': metadata
                        })
                        
                        processed_resumes += 1
                        logger.info(f"✅ Создан эмбеддинг для резюме: {submission.candidate.first_name} {submission.candidate.last_name}")
                    else:
                        errors += 1
                        logger.error(f"❌ Не удалось создать эмбеддинг для резюме {submission.submission_id}")
                
            except Exception as e:
                errors += 1
                logger.error(f"❌ Ошибка при обработке резюме {submission.submission_id}: {str(e)}")
                continue
        
        # Обрабатываем вакансии
        logger.info("🔄 Начинаем пересоздание эмбеддингов для вакансий")
        
        for i, job in enumerate(all_jobs):
            try:
                progress = 10 + ((total_submissions + i) / total_items) * 80
                self.update_state(
                    state='PROGRESS',
                    meta={
                        'progress': int(progress),
                        'status': f'Обработка вакансии {i + 1}/{total_jobs}',
                        'current_item': job.title,
                        'processed_resumes': processed_resumes,
                        'processed_jobs': processed_jobs,
                        'errors': errors
                    }
                )
                
                # Проверяем, нужно ли пересоздавать эмбеддинг
                should_recreate = force_recreate
                
                if not force_recreate:
                    # Проверяем, есть ли уже эмбеддинг и был ли он создан с предобработкой
                    existing_embedding = embedding_crud.get_by_source_id_and_type(
                        db, str(job.job_id), 'job'
                    )
                    
                    if existing_embedding:
                        # Проверяем метаданные
                        try:
                            results = job_collection.get(
                                ids=[existing_embedding.chroma_document_id],
                                include=['metadatas']
                            )
                            
                            if results['metadatas'] and len(results['metadatas']) > 0:
                                metadata = results['metadatas'][0]
                                if 'preprocessing_stats' not in metadata:
                                    should_recreate = True
                                    logger.info(f"🔄 Эмбеддинг для вакансии {job.job_id} будет пересоздан (без предобработки)")
                                else:
                                    logger.info(f"✅ Эмбеддинг для вакансии {job.job_id} уже создан с предобработкой")
                            else:
                                should_recreate = True
                        except Exception as e:
                            logger.warning(f"⚠️ Ошибка при проверке метаданных для вакансии {job.job_id}: {e}")
                            should_recreate = True
                    else:
                        should_recreate = True
                
                if should_recreate:
                    # Удаляем существующий эмбеддинг если есть
                    existing_embedding = embedding_crud.get_by_source_id_and_type(
                        db, str(job.job_id), 'job'
                    )
                    
                    if existing_embedding:
                        try:
                            job_collection.delete(ids=[existing_embedding.chroma_document_id])
                        except Exception as e:
                            logger.warning(f"⚠️ Не удалось удалить эмбеддинг из ChromaDB: {e}")
                        
                        db.delete(existing_embedding)
                    
                    # Предобрабатываем текст
                    processed_text, preprocessing_stats = preprocess_text_with_stats(
                        job.description,
                        config=None  # Используем стандартную конфигурацию для вакансий
                    )
                    
                    logger.info(f"📄 Предобработка вакансии '{job.title}': "
                              f"было {preprocessing_stats['original_length']} символов, "
                              f"стало {preprocessing_stats['processed_length']} символов "
                              f"(сжатие: {preprocessing_stats['compression_ratio']:.2%})")
                    
                    # Создаём эмбеддинг
                    embedding_response = chroma_client.create_embedding(processed_text)
                    
                    if embedding_response and 'embedding' in embedding_response:
                        chroma_doc_id = str(uuid.uuid4())
                        
                        # Метаданные с информацией о предобработке
                        metadata = {
                            'job_id': str(job.job_id),
                            'job_title': job.title,
                            'company_name': job.company.company_name if job.company else '',
                            'preprocessing_stats': preprocessing_stats,
                            'original_length': preprocessing_stats['original_length'],
                            'processed_length': preprocessing_stats['processed_length'],
                            'compression_ratio': preprocessing_stats['compression_ratio'],
                            'created_with_preprocessing': True,
                            'preprocessing_version': '1.0',
                            'created_at': datetime.now().isoformat()
                        }
                        
                        # Добавляем в ChromaDB
                        job_collection.add(
                            documents=[processed_text],
                            embeddings=[embedding_response['embedding']],
                            metadatas=[metadata],
                            ids=[chroma_doc_id]
                        )
                        
                        # Сохраняем в PostgreSQL
                        embedding_crud.create(db, {
                            'source_id': str(job.job_id),
                            'source_type': 'job',
                            'chroma_document_id': chroma_doc_id,
                            'text_content': processed_text,
                            'metadata': metadata
                        })
                        
                        processed_jobs += 1
                        logger.info(f"✅ Создан эмбеддинг для вакансии: {job.title}")
                    else:
                        errors += 1
                        logger.error(f"❌ Не удалось создать эмбеддинг для вакансии {job.job_id}")
                
            except Exception as e:
                errors += 1
                logger.error(f"❌ Ошибка при обработке вакансии {job.job_id}: {str(e)}")
                continue
        
        # Финальная фиксация изменений
        db.commit()
        
        # Обновляем финальный прогресс
        self.update_state(
            state='SUCCESS',
            meta={
                'progress': 100,
                'status': 'Завершено',
                'processed_resumes': processed_resumes,
                'processed_jobs': processed_jobs,
                'errors': errors
            }
        )
        
        logger.info(f"✅ Пересоздание эмбеддингов завершено. "
                   f"Обработано резюме: {processed_resumes}, вакансий: {processed_jobs}, ошибок: {errors}")
        
        return {
            'status': 'completed',
            'processed_resumes': processed_resumes,
            'processed_jobs': processed_jobs,
            'errors': errors,
            'total_items': total_items
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка при пересоздании эмбеддингов: {str(e)}")
        self.update_state(
            state='FAILURE',
            meta={
                'error': str(e),
                'status': 'Ошибка выполнения'
            }
        )
        raise
    finally:
        db.close()


@get_celery_app().task(bind=True, name='tasks.embedding_tasks.quick_recreate_embeddings')
def quick_recreate_embeddings(self):
    """
    Быстрое пересоздание эмбеддингов - только те, которые созданы без предобработки
    """
    return recreate_all_embeddings_with_preprocessing.apply_async(args=[False]).get()


@get_celery_app().task(bind=True, name='tasks.embedding_tasks.force_recreate_all_embeddings')
def force_recreate_all_embeddings(self):
    """
    Принудительное пересоздание всех эмбеддингов с предобработкой
    """
    return recreate_all_embeddings_with_preprocessing.apply_async(args=[True]).get()


@get_celery_app().task(bind=True, name='tasks.embedding_tasks.preprocess_resume_text_task')
def preprocess_resume_text_task(self, submission_id: str):
    """
    Задача предобработки текста резюме
    
    Args:
        submission_id: ID заявки
    """
    try:
        session = database.get_session()
        
        try:
            # Получаем заявку
            submission = session.query(Submission).filter(
                Submission.submission_id == submission_id
            ).first()
            
            if not submission:
                raise ValueError(f"Заявка {submission_id} не найдена")
            
            if not submission.resume_raw_text:
                raise ValueError(f"У заявки {submission_id} нет текста резюме")
            
            # Предобрабатываем текст
            original_text = str(submission.resume_raw_text)
            processed_text = preprocess_resume_text(original_text)
            
            # Обновляем запись
            session.query(Submission).filter(
                Submission.submission_id == submission_id
            ).update({
                'resume_raw_text': processed_text,
                'resume_parsed_at': datetime.utcnow()
            })
            
            session.commit()
            
            logger.info(f"✅ Текст резюме для заявки {submission_id} предобработан")
            
            # Возвращаем информацию о процессе
            return {
                'submission_id': submission_id,
                'status': 'completed',
                'processed_length': len(processed_text),
                'timestamp': datetime.utcnow().isoformat()
            }
            
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"❌ Ошибка предобработки текста резюме {submission_id}: {e}")
        self.retry(countdown=60, max_retries=3)


@get_celery_app().task(bind=True, name='tasks.embedding_tasks.preprocess_job_text_task')
def preprocess_job_text_task(self, job_id: int):
    """
    Задача предобработки текста вакансии
    
    Args:
        job_id: ID вакансии
    """
    try:
        session = database.get_session()
        
        try:
            # Получаем вакансию
            job = session.query(Job).filter(Job.job_id == job_id).first()
            
            if not job:
                raise ValueError(f"Вакансия {job_id} не найдена")
            
            if not job.description:
                raise ValueError(f"У вакансии {job_id} нет описания")
            
            # Предобрабатываем текст
            original_text = str(job.description)
            processed_text = preprocess_job_description_text(original_text)
            
            # Обновляем запись
            session.query(Job).filter(Job.job_id == job_id).update({
                'description': processed_text
            })
            
            session.commit()
            
            logger.info(f"✅ Текст вакансии {job_id} предобработан")
            
            return {
                'job_id': job_id,
                'status': 'completed',
                'processed_length': len(processed_text),
                'timestamp': datetime.utcnow().isoformat()
            }
            
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"❌ Ошибка предобработки текста вакансии {job_id}: {e}")
        self.retry(countdown=60, max_retries=3)


@get_celery_app().task(bind=True, name='tasks.embedding_tasks.clear_all_embeddings_task')
def clear_all_embeddings_task(self):
    """
    Задача очистки всех эмбеддингов из ChromaDB
    """
    try:
        # Очищаем коллекцию резюме
        try:
            chroma_client.client.delete_collection(name="resumes")
            logger.info("✅ Коллекция резюме удалена")
        except Exception as e:
            logger.info(f"Коллекция резюме не существует или уже удалена: {e}")
        
        # Очищаем коллекцию вакансий
        try:
            chroma_client.client.delete_collection(name="job_descriptions")
            logger.info("✅ Коллекция вакансий удалена")
        except Exception as e:
            logger.info(f"Коллекция вакансий не существует или уже удалена: {e}")
        
        # Создаем новые пустые коллекции
        chroma_client.client.create_collection(name="resumes")
        chroma_client.client.create_collection(name="job_descriptions")
        
        logger.info("✅ Все эмбеддинги очищены, созданы новые коллекции")
        
        return {
            'status': 'completed',
            'message': 'Все эмбеддинги очищены',
            'timestamp': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка очистки эмбеддингов: {e}")
        self.retry(countdown=60, max_retries=3)


@get_celery_app().task(bind=True, name='tasks.embedding_tasks.recreate_all_embeddings_task')
def recreate_all_embeddings_task(self):
    """
    Главная задача пересоздания всех эмбеддингов с предобработкой
    
    Выполняет следующие шаги:
    1. Очищает все существующие эмбеддинги
    2. Предобрабатывает все тексты
    3. Создает новые эмбеддинги
    """
    try:
        logger.info("🚀 Начинаем пересоздание всех эмбеддингов")
        
        # Шаг 1: Очищаем все эмбеддинги
        logger.info("🧹 Очищаем существующие эмбеддинги...")
        clear_result = clear_all_embeddings_task.apply()
        logger.info(f"Результат очистки: {clear_result.get()}")
        
        # Шаг 2: Получаем все заявки и вакансии
        session = database.get_session()
        
        try:
            submissions = session.query(Submission).filter(
                Submission.resume_raw_text.isnot(None)
            ).all()
            
            jobs = session.query(Job).filter(
                Job.description.isnot(None)
            ).all()
            
            logger.info(f"📊 Найдено {len(submissions)} резюме и {len(jobs)} вакансий")
            
        finally:
            session.close()
        
        # Шаг 3: Запускаем предобработку текстов
        logger.info("🔄 Запускаем предобработку текстов...")
        
        # Предобрабатываем резюме
        for submission in submissions:
            preprocess_resume_text_task.delay(str(submission.submission_id))
        
        # Предобрабатываем вакансии
        for job in jobs:
            preprocess_job_text_task.delay(job.job_id)
        
        # Шаг 4: Ждем завершения предобработки и запускаем создание эмбеддингов
        # Используем countdown для задержки, чтобы предобработка успела завершиться
        logger.info("⏳ Ждем завершения предобработки...")
        
        # Запускаем создание эмбеддингов с задержкой
        for submission in submissions:
            generate_resume_embeddings.apply_async(
                args=[[str(submission.submission_id)]],
                countdown=60  # 60 секунд задержки
            )
        
        for job in jobs:
            generate_job_embeddings.apply_async(
                args=[[job.job_id]],
                countdown=60  # 60 секунд задержки
            )
        
        logger.info("✅ Все задачи пересоздания эмбеддингов запущены")
        
        return {
            'status': 'completed',
            'resumes_count': len(submissions),
            'jobs_count': len(jobs),
            'message': 'Пересоздание эмбеддингов запущено',
            'timestamp': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка пересоздания эмбеддингов: {e}")
        self.retry(countdown=120, max_retries=2)
