"""
Reranking tasks for improving matching quality between resumes and jobs

Задача 4: Реранкинг резюме и вакансий после генерации эмбеддингов
"""

import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from celery.utils.log import get_task_logger
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from uuid import UUID
from celery.signals import worker_process_init

from common.celery_app.celery_app import celery_app
from common.database.config import database
from common.database.operations.analysis_operations import RerankerAnalysisResultCRUD
from common.database.operations.embedding_operations import embedding_crud
from common.database.operations.candidate_operations import SubmissionCRUD
from common.database.operations.company_operations import JobCRUD
from common.celery_app.queue_names import RERANKING_QUEUE

from common.models.candidates import Submission
from common.models.companies import Job
from common.models.analysis_results import RerankerAnalysisResult
from common.utils.chroma_config import chroma_client, ChromaConfig
from common.utils.reranker_config import get_reranker_client

# Загружаем переменные окружения
load_dotenv()

logger = get_task_logger(__name__)

# CRUD экземпляры
analysis_crud = RerankerAnalysisResultCRUD()
submission_crud = SubmissionCRUD()
job_crud = JobCRUD()


@celery_app.task(
    bind=True,
    name='common.tasks.reranking_tasks.rerank_resumes_for_job',
    soft_time_limit=600,
    time_limit=720,
    max_retries=3
)
def rerank_resumes_for_job(self, job_id: int, top_k: int = 50) -> Dict[str, Any]:
    """
    Task 4A: Реранкинг топ-50 резюме для конкретной вакансии
    
    Находит топ-50 наиболее похожих резюме через ChromaDB,
    затем переранжирует их с помощью BGE Reranker для улучшения качества.
    Сохраняет результаты в PostgreSQL.
    
    Args:
        job_id: ID вакансии для поиска резюме
        top_k: Количество резюме для реранкинга (по умолчанию 50)
    
    Returns:
        Dict с результатами реранкинга
    """
    logger.info(f"🔍 Запуск реранкинга резюме для вакансии {job_id}")

    # Исправление: если job_id не int, задача пропускается
    if not isinstance(job_id, int):
        logger.warning(f"⚠️ job_id передан в неверном формате: {type(job_id)}. Пропуск задачи.")
        return {
            'status': 'skipped',
            'error': 'job_id должен быть int',
            'processed_matches': 0,
            'error_matches': 0
        }
    
    try:
        db = database.get_session()
        processed_count = 0
        error_count = 0
        
        try:
            # 1. Получаем вакансию
            job = job_crud.get_by_id(db, job_id)
            if not job:
                logger.error(f"❌ Вакансия {job_id} не найдена")
                return {
                    'status': 'error',
                    'error': f'Вакансия {job_id} не найдена',
                    'processed_matches': 0,
                    'error_matches': 0
                }
            
            # 2. Проверяем наличие текста вакансии
            if not getattr(job, 'job_description_raw_text', None) or not str(job.job_description_raw_text).strip():
                logger.error(f"❌ У вакансии {job_id} нет распарсенного текста")
                return {
                    'status': 'error',
                    'error': f'У вакансии {job_id} нет распарсенного текста',
                    'processed_matches': 0,
                    'error_matches': 0
                }
            
            # 3. Получаем текст вакансии (обрезаем до 32,000 символов для реранкера)
            job_text_full = str(job.job_description_raw_text)
            original_length = len(job_text_full)
            
            if original_length > 32000:
                job_text = job_text_full[:32000]
                logger.info(f"📏 Текст вакансии {job_id} обрезан для реранкера с {original_length} до {len(job_text)} символов")
            else:
                job_text = job_text_full
                logger.info(f"📏 Текст вакансии {job_id}: {original_length} символов (в пределах лимита реранкера)")
            
            # 4. Поиск похожих резюме в ChromaDB
            logger.info(f"🔍 Поиск топ-{top_k} похожих резюме в ChromaDB")
            resume_collection = chroma_client.get_resume_collection()
            
            if resume_collection.count() == 0:
                logger.warning("⚠️ Коллекция резюме пуста")
                return {
                    'status': 'completed',
                    'processed_matches': 0,
                    'error_matches': 0,
                    'total_found': 0
                }
            
            # Поиск похожих резюме
            search_results = resume_collection.query(
                query_texts=[job_text],
                n_results=min(top_k, resume_collection.count()),
                include=['documents', 'metadatas', 'distances']
            )
            
            if not search_results['documents'] or not search_results['documents'][0]:
                logger.warning("⚠️ Не найдено похожих резюме")
                return {
                    'status': 'completed',
                    'processed_matches': 0,
                    'error_matches': 0,
                    'total_found': 0
                }
            
            # 5. Подготавливаем данные для реранкинга
            documents = search_results['documents'][0]
            metadatas = search_results['metadatas'][0]
            distances = search_results['distances'][0]

            # Проверка: distances и metadatas должны быть списками
            if not isinstance(distances, list):
                logger.error(f"[FATAL] distances не список: {type(distances)}, value={distances}")
                return {
                    'status': 'error',
                    'error': 'distances не список',
                    'processed_matches': 0,
                    'error_matches': 0
                }
            if not isinstance(metadatas, list):
                logger.error(f"[FATAL] metadatas не список: {type(metadatas)}, value={metadatas}")
                return {
                    'status': 'error',
                    'error': 'metadatas не список',
                    'processed_matches': 0,
                    'error_matches': 0
                }
            
            logger.info(f"📊 Найдено {len(documents)} резюме для реранкинга")
            
            # 6. Реранкинг с помощью BGE Reranker
            reranker = get_reranker_client()
            
            if not reranker.health_check():
                logger.error("❌ BGE Reranker недоступен")
                return {
                    'status': 'error',
                    'error': 'BGE Reranker недоступен',
                    'processed_matches': 0,
                    'error_matches': 0
                }
            
            logger.info(f"🧠 Начинаем реранкинг {len(documents)} резюме")
            reranked_results = reranker.rerank_texts(job_text, documents)
            
            if not reranked_results:
                logger.warning("⚠️ Реранкинг не вернул результатов")
                return {
                    'status': 'completed',
                    'processed_matches': 0,
                    'error_matches': 0,
                    'total_found': len(documents)
                }
            
            # 7. Сохраняем результаты в PostgreSQL пакетами для экономии памяти
            logger.info(f"💾 Сохранение {len(reranked_results)} результатов реранкинга")
            
            batch_size = 1  # Размер пакета для сохранения
            batch_results = []
            
            for rank_position, (doc_idx, rerank_score) in enumerate(reranked_results, 1):
                try:
                    if doc_idx >= len(metadatas):
                        logger.warning(f"⚠️ Индекс {doc_idx} выходит за границы метаданных")
                        continue
                    
                    metadata = metadatas[doc_idx]
                    source_id = metadata.get('source_id')
                    
                    if not source_id:
                        logger.warning(f"⚠️ Нет source_id в метаданных для индекса {doc_idx}")
                        continue
                    
                    # Получаем submission
                    submission = submission_crud.get_by_id(db, source_id)
                    if not submission:
                        logger.warning(f"⚠️ Submission {source_id} не найден")
                        continue
                    
                    # Создаем запись результата реранкинга
                    original_distance = distances[doc_idx] if doc_idx < len(distances) else 0.0
                    original_similarity = max(0.0, 1.0 - original_distance)
                    
                    # Нормализуем rerank_score от [-10, +10] к [0, 1]
                    normalized_rerank_score = max(0.0, min(1.0, (rerank_score + 10.0) / 20.0))
                    
                    # Вычисляем финальный score (комбинация original similarity и normalized rerank score)
                    final_score = (original_similarity * 0.3) + (normalized_rerank_score * 0.7)
                    score_improvement = normalized_rerank_score - original_similarity
                    
                    # Подготавливаем параметры поиска
                    search_params = {
                        'top_k': top_k,
                        'min_similarity': 0.0,
                        'min_rerank_score': -10.0,
                        'search_type': 'job_to_resumes',
                        'query_text_length': len(job_text),
                        'original_text_length': original_length,
                        'text_truncated': original_length > 32000
                    }
                    
                    # Статистика workflow
                    workflow_stats = {
                        'total_candidates_found': len(documents),
                        'reranked_candidates': len(reranked_results),
                        'processing_time': datetime.utcnow().isoformat(),
                        'chroma_collection': ChromaConfig.RESUME_COLLECTION
                    }
                    
                    # Создаем запись анализа
                    analysis_result = RerankerAnalysisResult(
                        job_id=job_id,
                        submission_id=submission.submission_id,
                        original_similarity=original_similarity,
                        rerank_score=rerank_score,
                        final_score=final_score,
                        score_improvement=score_improvement,
                        rank_position=rank_position,
                        search_params=search_params,
                        reranker_model=reranker.model_name,
                        workflow_stats=workflow_stats,
                        job_title=job.title or 'Unknown',
                        company_id=job.company_id,
                        candidate_name=f"{submission.candidate.first_name or ''} {submission.candidate.last_name or ''}".strip() or 'Unknown',
                        candidate_email=submission.candidate.email or 'Unknown',
                        total_candidates_found=len(documents),
                        analysis_type='job_to_resumes_rerank'
                    )
                    
                    batch_results.append(analysis_result)
                    processed_count += 1
                    
                    # Сохраняем пакетами для экономии памяти
                    if len(batch_results) >= batch_size:
                        db.add_all(batch_results)
                        db.commit()
                        logger.info(f"💾 Сохранен пакет из {len(batch_results)} результатов реранкинга")
                        batch_results.clear()  # Очищаем память
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка сохранения результата реранкинга: {e}")
                    error_count += 1
                    continue
            
            # Сохраняем оставшиеся результаты
            if batch_results:
                db.add_all(batch_results)
                db.commit()
                logger.info(f"💾 Сохранен финальный пакет из {len(batch_results)} результатов реранкинга")
            
            logger.info(f"✅ Реранкинг резюме для вакансии {job_id} завершен: {processed_count} обработано, {error_count} ошибок")
            
            return {
                'status': 'completed',
                'processed_matches': processed_count,
                'error_matches': error_count,
                'total_found': len(documents)
            }
            
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"❌ Критическая ошибка реранкинга резюме для вакансии {job_id}: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'processed_matches': 0,
            'error_matches': 0
        }


@celery_app.task(
    bind=True,
    name='common.tasks.reranking_tasks.rerank_jobs_for_resume',
    soft_time_limit=600,
    time_limit=720,
    max_retries=3
)
def rerank_jobs_for_resume(self, submission_id: str, top_k: int = 50) -> Dict[str, Any]:
    """
    Task 4B: Реранкинг топ-50 вакансий для конкретного резюме
    
    Находит топ-50 наиболее похожих вакансий через ChromaDB,
    затем переранжирует их с помощью BGE Reranker для улучшения качества.
    Сохраняет результаты в PostgreSQL.
    
    Args:
        submission_id: ID резюме для поиска вакансий
        top_k: Количество вакансий для реранкинга (по умолчанию 50)
    
    Returns:
        Dict с результатами реранкинга
    """
    logger.info(f"🔍 Запуск реранкинга вакансий для резюме {submission_id}")

    # Исправление: если submission_id не строка (UUID), а список или словарь — пропускаем
    if not isinstance(submission_id, str):
        logger.warning(f"⚠️ submission_id передан в неверном формате: {type(submission_id)}. Пропуск задачи.")
        return {
            'status': 'skipped',
            'error': 'submission_id должен быть строкой UUID',
            'processed_matches': 0,
            'error_matches': 0
        }
    
    try:
        db = database.get_session()
        processed_count = 0
        error_count = 0
        
        try:
            # 1. Получаем резюме
            submission_uuid = UUID(submission_id) if isinstance(submission_id, str) else submission_id
            submission = submission_crud.get_by_id(db, submission_uuid)
            if not submission:
                logger.error(f"❌ Резюме {submission_id} не найдено")
                return {
                    'status': 'error',
                    'error': f'Резюме {submission_id} не найдено',
                    'processed_matches': 0,
                    'error_matches': 0
                }
            
            # 2. Проверяем наличие текста резюме
            if not getattr(submission, 'resume_raw_text', None) or not str(submission.resume_raw_text).strip():
                logger.error(f"❌ У резюме {submission_id} нет распарсенного текста")
                return {
                    'status': 'error',
                    'error': f'У резюме {submission_id} нет распарсенного текста',
                    'processed_matches': 0,
                    'error_matches': 0
                }
            
            # 3. Получаем текст резюме (обрезаем до 32,000 символов для реранкера)
            resume_text_full = str(submission.resume_raw_text)
            original_length = len(resume_text_full)
            
            if original_length > 32000:
                resume_text = resume_text_full[:32000]
                logger.info(f"📏 Текст резюме {submission_id} обрезан для реранкера с {original_length} до {len(resume_text)} символов")
            else:
                resume_text = resume_text_full
                logger.info(f"📏 Текст резюме {submission_id}: {original_length} символов (в пределах лимита реранкера)")
            
            # 4. Поиск похожих вакансий в ChromaDB
            logger.info(f"🔍 Поиск топ-{top_k} подходящих вакансий в ChromaDB")
            job_collection = chroma_client.get_job_collection()
            
            if job_collection.count() == 0:
                logger.warning("⚠️ Коллекция вакансий пуста")
                return {
                    'status': 'completed',
                    'processed_matches': 0,
                    'error_matches': 0,
                    'total_found': 0
                }
            
            # Поиск похожих вакансий
            search_results = job_collection.query(
                query_texts=[resume_text],
                n_results=min(top_k, job_collection.count()),
                include=['documents', 'metadatas', 'distances']
            )
            
            if not search_results['documents'] or not search_results['documents'][0]:
                logger.warning("⚠️ Не найдено похожих вакансий")
                return {
                    'status': 'completed',
                    'processed_matches': 0,
                    'error_matches': 0,
                    'total_found': 0
                }
            
            # 5. Подготавливаем данные для реранкинга
            documents = search_results['documents'][0]
            metadatas = search_results['metadatas'][0]
            distances = search_results['distances'][0]

            # Проверка: distances и metadatas должны быть списками
            if not isinstance(distances, list):
                logger.error(f"[FATAL] distances не список: {type(distances)}, value={distances}")
                return {
                    'status': 'error',
                    'error': 'distances не список',
                    'processed_matches': 0,
                    'error_matches': 0
                }
            if not isinstance(metadatas, list):
                logger.error(f"[FATAL] metadatas не список: {type(metadatas)}, value={metadatas}")
                return {
                    'status': 'error',
                    'error': 'metadatas не список',
                    'processed_matches': 0,
                    'error_matches': 0
                }
            
            logger.info(f"📊 Найдено {len(documents)} вакансий для реранкинга")
            
            # 6. Реранкинг с помощью BGE Reranker
            reranker = get_reranker_client()
            
            if not reranker.health_check():
                logger.error("❌ BGE Reranker недоступен")
                return {
                    'status': 'error',
                    'error': 'BGE Reranker недоступен',
                    'processed_matches': 0,
                    'error_matches': 0
                }
            
            logger.info(f"🧠 Начинаем реранкинг {len(documents)} вакансий")
            reranked_results = reranker.rerank_texts(resume_text, documents)
            
            if not reranked_results:
                logger.warning("⚠️ Реранкинг не вернул результатов")
                return {
                    'status': 'completed',
                    'processed_matches': 0,
                    'error_matches': 0,
                    'total_found': len(documents)
                }
            
            # 7. Сохраняем результаты в PostgreSQL пакетами для экономии памяти
            logger.info(f"💾 Сохранение {len(reranked_results)} результатов реранкинга")
            
            batch_size = 10  # Размер пакета для сохранения
            batch_results = []
            
            for rank_position, (doc_idx, rerank_score) in enumerate(reranked_results, 1):
                try:
                    # Подробное логирование для диагностики вложенных списков и неверных типов
                    logger.error(f"[CHECK] doc_idx={doc_idx} ({type(doc_idx)}), distances type={type(distances)}, metadatas type={type(metadatas)}")
                    if isinstance(distances, list) and len(distances) > 0:
                        logger.error(f"[CHECK] distances[0] type={type(distances[0])}, value={distances[0]}")
                    if isinstance(metadatas, list) and len(metadatas) > 0:
                        logger.error(f"[CHECK] metadatas[0] type={type(metadatas[0])}, value={metadatas[0]}")
                    if not isinstance(doc_idx, int):
                        logger.error(f"[FATAL] doc_idx не int: {type(doc_idx)}, value={doc_idx}")
                        error_count += 1
                        continue
                    if doc_idx >= len(metadatas):
                        logger.warning(f"⚠️ Индекс {doc_idx} выходит за границы метаданных")
                        continue
                    if doc_idx >= len(distances):
                        logger.warning(f"⚠️ Индекс {doc_idx} выходит за границы distances")
                        continue
                    if isinstance(distances[doc_idx], list):
                        logger.error(f"[FATAL] distances[{doc_idx}] вложенный список: {distances[doc_idx]}")
                        error_count += 1
                        continue
                    
                    metadata = metadatas[doc_idx]
                    job_id = metadata.get('source_id')
                    
                    if not job_id:
                        logger.warning(f"⚠️ Нет source_id в метаданных для индекса {doc_idx}")
                        continue
                    
                    # Получаем вакансию
                    job = job_crud.get_by_id(db, job_id)
                    if not job:
                        logger.warning(f"⚠️ Вакансия {job_id} не найдена")
                        continue
                    
                    # Создаем запись результата реранкинга
                    original_distance = distances[doc_idx] if doc_idx < len(distances) else 0.0
                    original_similarity = max(0.0, 1.0 - original_distance)
                    
                    # Нормализуем rerank_score от [-10, +10] к [0, 1]
                    normalized_rerank_score = max(0.0, min(1.0, (rerank_score + 10.0) / 20.0))
                    
                    # Вычисляем финальный score (комбинация original similarity и normalized rerank score)
                    final_score = (original_similarity * 0.3) + (normalized_rerank_score * 0.7)
                    score_improvement = normalized_rerank_score - original_similarity
                    
                    # Подготавливаем параметры поиска
                    search_params = {
                        'top_k': top_k,
                        'min_similarity': 0.0,
                        'min_rerank_score': -10.0,
                        'search_type': 'resume_to_jobs',
                        'query_text_length': len(resume_text),
                        'original_text_length': original_length,
                        'text_truncated': original_length > 32000
                    }
                    
                    # Статистика workflow
                    workflow_stats = {
                        'total_candidates_found': len(documents),
                        'reranked_candidates': len(reranked_results),
                        'processing_time': datetime.utcnow().isoformat(),
                        'chroma_collection': ChromaConfig.JOB_COLLECTION
                    }
                    
                    # Создаем запись анализа
                    analysis_result = RerankerAnalysisResult(
                        job_id=job_id,
                        submission_id=submission.submission_id,
                        original_similarity=original_similarity,
                        rerank_score=rerank_score,
                        final_score=final_score,
                        score_improvement=score_improvement,
                        rank_position=rank_position,
                        search_params=search_params,
                        reranker_model=reranker.model_name,
                        workflow_stats=workflow_stats,
                        job_title=job.title or 'Unknown',
                        company_id=job.company_id,
                        candidate_name=f"{submission.candidate.first_name or ''} {submission.candidate.last_name or ''}".strip() or 'Unknown',
                        candidate_email=submission.candidate.email or 'Unknown',
                        total_candidates_found=len(documents),
                        analysis_type='resume_to_jobs_rerank'
                    )
                    
                    batch_results.append(analysis_result)
                    processed_count += 1
                    
                    # Сохраняем пакетами для экономии памяти
                    if len(batch_results) >= batch_size:
                        db.add_all(batch_results)
                        db.commit()
                        logger.info(f"💾 Сохранен пакет из {len(batch_results)} результатов реранкинга")
                        batch_results.clear()  # Очищаем память
                    
                except Exception as e:
                    import traceback
                    logger.error(f"❌ Ошибка сохранения результата реранкинга: {e}\nTRACEBACK:\n{traceback.format_exc()}")
                    logger.error(f"[EXCEPT-DIAG] doc_idx={locals().get('doc_idx', None)} ({type(locals().get('doc_idx', None))}), distances type={type(locals().get('distances', None))}, metadatas type={type(locals().get('metadatas', None))}")
                    try:
                        logger.error(f"[EXCEPT-DIAG] distances={locals().get('distances', None)}")
                        logger.error(f"[EXCEPT-DIAG] metadatas={locals().get('metadatas', None)}")
                    except Exception as diag_e:
                        logger.error(f"[EXCEPT-DIAG] Ошибка при логировании distances/metadatas: {diag_e}")
                    error_count += 1
                    continue
            
            # Сохраняем оставшиеся результаты
            if batch_results:
                db.add_all(batch_results)
                db.commit()
                logger.info(f"💾 Сохранен финальный пакет из {len(batch_results)} результатов реранкинга")
            
            logger.info(f"✅ Реранкинг вакансий для резюме {submission_id} завершен: {processed_count} обработано, {error_count} ошибок")
            
            return {
                'status': 'completed',
                'processed_matches': processed_count,
                'error_matches': error_count,
                'total_found': len(documents)
            }
            
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"❌ Критическая ошибка реранкинга вакансий для резюме {submission_id}: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'processed_matches': 0,
            'error_matches': 0
        }


# Отключаем задачу process_all_reranking (временно)
# @celery_app.task(
#     bind=True,
#     name='tasks.reranking_tasks.process_all_reranking',
#     soft_time_limit=1800,
#     time_limit=2400,
#     max_retries=2
# )
# def process_all_reranking(self, top_k: int = 50) -> Dict[str, Any]:
#     """
#     Task 4: Основная задача реранкинга - обрабатывает все резюме и вакансии
    
#     Запускается после завершения задач генерации эмбеддингов.
#     Выполняет реранкинг для всех новых резюме и вакансий.
    
#     Args:
#         top_k: Количество элементов для реранкинга (по умолчанию 50)
    
#     Returns:
#         Dict с общими результатами реранкинга
#     """
#     logger.info(f"🚀 Запуск полного процесса реранкинга (топ-{top_k})")
    
#     try:
#         db = database.get_session()
#         total_resume_matches = 0
#         total_job_matches = 0
#         processed_resumes = 0
#         processed_jobs = 0
#         error_resumes = 0
#         error_jobs = 0
        
#         try:
#             # 1. Получаем все резюме с текстом, но без результатов реранкинга
#             resumes_for_reranking = db.query(Submission).filter(
#                 Submission.resume_raw_text.isnot(None),
#                 Submission.resume_raw_text != '',
#                 ~Submission.submission_id.in_(
#                     db.query(RerankerAnalysisResult.submission_id).filter(
#                         RerankerAnalysisResult.analysis_type == 'resume_to_jobs_rerank'
#                     )
#                 )
#             ).all()
            
#             logger.info(f"📋 Найдено {len(resumes_for_reranking)} резюме для реранкинга")
            
#             # 2. Получаем все вакансии с текстом, но без результатов реранкинга
#             jobs_for_reranking = db.query(Job).filter(
#                 Job.job_description_raw_text.isnot(None),
#                 Job.job_description_raw_text != '',
#                 ~Job.job_id.in_(
#                     db.query(RerankerAnalysisResult.job_id).filter(
#                         RerankerAnalysisResult.analysis_type == 'job_to_resumes_rerank'
#                     )
#                 )
#             ).all()
            
#             logger.info(f"📋 Найдено {len(jobs_for_reranking)} вакансий для реранкинга")
            
#             # 3. Обрабатываем резюме (цепочка B: резюме -> топ-50 вакансий)
#             for submission in resumes_for_reranking:
#                 try:
#                     logger.info(f"🔄 Отправка задачи реранкинга для резюме {submission.submission_id}")
                    
#                     # Запускаем задачу реранкинга для резюме асинхронно (БЕЗ .get()!)
#                     task_result = rerank_jobs_for_resume.apply_async(
#                         args=[str(submission.submission_id), top_k],
#                         queue=RERANKING_QUEUE
#                     )
                    
#                     # НЕ вызываем .get() - просто логируем запуск задачи
#                     logger.info(f"📤 Задача реранкинга для резюме {submission.submission_id} отправлена: {task_result.id}")
#                     processed_resumes += 1
                    
#                 except Exception as e:
#                     logger.error(f"❌ Ошибка отправки задачи для резюме {submission.submission_id}: {e}")
#                     error_resumes += 1
#                     continue
            
#             # 4. Обрабатываем вакансии (цепочка A: вакансия -> топ-50 резюме)
#             for job in jobs_for_reranking:
#                 try:
#                     logger.info(f"🔄 Отправка задачи реранкинга для вакансии {job.job_id}")
                    
#                     # Запускаем задачу реранкинга для вакансии асинхронно (БЕЗ .get()!)
#                     task_result = rerank_resumes_for_job.apply_async(
#                         args=[job.job_id, top_k],
#                         queue=RERANKING_QUEUE
#                     )
                    
#                     # НЕ вызываем .get() - просто логируем запуск задачи
#                     logger.info(f"📤 Задача реранкинга для вакансии {job.job_id} отправлена: {task_result.id}")
#                     processed_jobs += 1
                    
#                 except Exception as e:
#                     logger.error(f"❌ Ошибка отправки задачи для вакансии {job.job_id}: {e}")
#                     error_jobs += 1
#                     continue
            
#             logger.info(f"✅ Все задачи реранкинга отправлены: {processed_resumes} резюме, {processed_jobs} вакансий")
            
#             return {
#                 'status': 'completed',
#                 'total_resumes_submitted': processed_resumes,
#                 'total_jobs_submitted': processed_jobs,
#                 'error_resumes': error_resumes,
#                 'error_jobs': error_jobs,
#                 'total_found_resumes': len(resumes_for_reranking),
#                 'total_found_jobs': len(jobs_for_reranking)
#             }
            
#         finally:
#             db.close()
            
#     except Exception as e:
#         logger.error(f"❌ Критическая ошибка процесса реранкинга: {e}")
#         return {
#             'status': 'error',
#             'error': str(e),
#             'total_resumes_submitted': 0,
#             'total_jobs_submitted': 0
#         }
