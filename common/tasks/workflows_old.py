"""
Цепочки задач для автоматической обработки данных

Цепочка A: Обработка нового резюме
Цепочка B: Обработка новой вакансии
"""

from celery import chain, group, chord
from celery.utils.log import get_task_logger
from typing import Dict, List, Any, Optional
from datetime import datetime
import os

# Импортируем правильное приложение Celery
try:
    from ..celery_app.celery_app import celery_app as app
except ImportError:
    from celery_app.celery_app import celery_app as app

logger = get_task_logger(__name__)


# Старые workflow функции удалены - используются обновленные цепочки

@app.task(
    bind=True, 
    name='common.tasks.workflows.run_full_processing_pipeline',
    soft_time_limit=2400,  # 40 минут
    time_limit=2700,       # 45 минут
    max_retries=3
)
def run_full_processing_pipeline(self) -> Dict[str, Any]:
    """
    ЦЕПОЧКА A: Полная обработка резюме
    
    Последовательность:
    1. pull_fillout_resumes (получение данных)
    2. parse_documents (парсинг резюме)
    3. check_and_start_gpu_server (проверка GPU)
    4. generate_resume_embeddings
    5. batch_find_matches_for_resumes
    6. rerank_resume_matches
    7. ai_analysis (если GPU доступен)
    8. save_analysis_results
    
    Returns:
        Результат выполнения цепочки
    """
    logger.info("🔗 Цепочка A: Начало полной обработки резюме")
    
    try:
        # Шаг A.0: Получение данных резюме из Fillout
        logger.info("📥 A.0: Получение данных резюме из Fillout API")
        fillout_result = app.send_task(
            'tasks.fillout_tasks.pull_fillout_resumes',
            queue='fillout'
        ).get(timeout=300)
        
        if not fillout_result or fillout_result.get('status') != 'completed':
            logger.warning("⚠️ Данные резюме не получены из Fillout")
            return {
                'status': 'no_data',
                'message': 'Данные резюме не получены из Fillout',
                'chain': 'A'
            }
        
        # Извлекаем submission_ids из результата Fillout
        cv_data = fillout_result.get('cv_data', {})
        submission_ids = cv_data.get('submission_ids', [])
        
        if not submission_ids:
            logger.warning("⚠️ Нет submission_ids для обработки")
            return {
                'status': 'no_data',
                'message': 'Нет submission_ids для обработки',
                'chain': 'A'
            }
        
        # Подготавливаем данные документов для парсинга
        documents_data = []
        for submission_id in submission_ids:
            # Здесь должна быть логика извлечения URL резюме из submission
            documents_data.append({
                'id': submission_id,
                'submission_id': submission_id,
                'type': 'resume'
            })
        
        # Шаг A.1: Парсинг документов резюме
        logger.info(f"📄 A.1: Парсинг {len(documents_data)} документов резюме")
        parse_result = app.send_task(
            'tasks.parse_tasks.parse_documents',
            args=[documents_data, 'resume'],
            queue='cpu_intensive'
        ).get(timeout=600)  # 10 минут на парсинг
        
        if parse_result.get('status') != 'completed':
            logger.error("❌ Ошибка парсинга документов резюме")
            return {
                'status': 'error',
                'message': 'Ошибка парсинга документов',
                'chain': 'A'
            }
        
        # Шаг A.2: Проверка и запуск GPU сервера
        logger.info("🔍 A.2: Проверка доступности GPU сервера")
        gpu_check_result = app.send_task(
            'tasks.gpu_tasks.check_and_start_gpu_server',
            args=['resume_processing'],
            queue='system'
        ).get(timeout=360)  # 6 минут на проверку и запуск GPU
        
        gpu_available = gpu_check_result.get('status') in ['available', 'started_and_available']
        
        # Шаг A.3: Генерация эмбеддингов резюме
        logger.info(f"📊 A.3: Генерация эмбеддингов для {len(submission_ids)} резюме")
        if gpu_available:
            embeddings_result = app.send_task(
                'tasks.embedding_tasks.generate_resume_embeddings',
                args=[submission_ids],
                queue='embeddings_gpu'
            ).get(timeout=600)  # 10 минут на эмбеддинги
        else:
            logger.warning("⚠️ GPU недоступен, используем CPU для эмбеддингов")
            embeddings_result = app.send_task(
                'tasks.embedding_tasks.generate_resume_embeddings_cpu',
                args=[submission_ids],
                queue='embeddings_cpu'
            ).get(timeout=1200)  # 20 минут на CPU эмбеддинги
        
        if embeddings_result.get('status') != 'success':
            logger.error("❌ Ошибка генерации эмбеддингов резюме")
            return {
                'status': 'error',
                'message': 'Ошибка генерации эмбеддингов',
                'chain': 'A'
            }
        
        # Шаг A.4: Пакетный поиск вакансий для резюме
        logger.info(f"🔍 A.4: Пакетный поиск вакансий для резюме")
        search_result = app.send_task(
            'tasks.matching.batch_find_matches_for_resumes',
            args=[submission_ids],
            queue='search_basic'
        ).get(timeout=300)  # 5 минут на поиск
        
        if search_result.get('status') != 'success':
            logger.error("❌ Ошибка поиска совпадений")
            return {
                'status': 'error',
                'message': 'Ошибка поиска совпадений',
                'chain': 'A'
            }
        
        # Шаг A.5: Реранкинг результатов
        matches = search_result.get('matches', [])
        if matches:
            logger.info(f"🎯 A.5: Реранкинг {len(matches)} совпадений")
            
            # Группируем совпадения по job_id для реранкинга
            matches_by_job = {}
            for match in matches:
                job_id = match.get('job_id')
                if job_id:
                    if job_id not in matches_by_job:
                        matches_by_job[job_id] = []
                    matches_by_job[job_id].append(match)
            
            rerank_results = []
            for job_id, job_matches in matches_by_job.items():
                rerank_result = app.send_task(
                    'tasks.scoring_tasks.rerank_resume_matches',
                    args=[job_id, job_matches],
                    queue='scoring_tasks'
                ).get(timeout=180)  # 3 минуты на реранкинг
                rerank_results.append(rerank_result)
        else:
            logger.info("ℹ️ A.5: Нет совпадений для реранкинга")
            rerank_results = []
            matches_by_job = {}
        
        # Шаг A.6: AI анализ (если GPU доступен)
        ai_results = []
        if gpu_available and rerank_results:
            logger.info("🤖 A.6: AI анализ результатов")
            
            # Подготавливаем данные для AI анализа
            ai_documents = {
                'documents': [],
                'type': 'resume_job_matching'
            }
            
            for rerank_result in rerank_results:
                if rerank_result.get('status') == 'success':
                    ai_documents['documents'].extend(rerank_result.get('matches', []))
            
            if ai_documents['documents']:
                ai_result = app.send_task(
                    'tasks.gpu_tasks.ai_analysis',
                    args=[ai_documents, 'match_scoring'],
                    queue='ai_analysis'
                ).get(timeout=900)  # 15 минут на AI анализ
                
                if ai_result.get('status') == 'completed':
                    ai_results.append(ai_result)
                    logger.info("✅ AI анализ завершен успешно")
                else:
                    logger.warning("⚠️ AI анализ завершился с ошибкой")
        
        # Шаг A.7: Сохранение результатов анализа
        if rerank_results:
            logger.info("💾 A.7: Сохранение результатов анализа")
            for i, rerank_result in enumerate(rerank_results):
                if rerank_result.get('status') == 'success':
                    job_id = rerank_result.get('job_id')
                    
                    # Добавляем AI результаты если есть
                    final_result = rerank_result.copy()
                    if ai_results and i < len(ai_results):
                        final_result['ai_analysis'] = ai_results[i]
                    
                    app.send_task(
                        'tasks.analysis_tasks.save_analysis_results',
                        args=[job_id, final_result, 'resume_processing'],
                        queue='default'
                    )
        
        logger.info("✅ Цепочка A завершена успешно")
        return {
            'status': 'success',
            'message': f'Цепочка A завершена: обработано {len(submission_ids)} резюме',
            'chain': 'A',
            'steps_completed': {
                'fillout_data': True,
                'document_parsing': True,
                'gpu_check': True,
                'embeddings': True,
                'matching': True,
                'reranking': len(rerank_results) > 0,
                'ai_analysis': len(ai_results) > 0,
                'results_saved': True
            },
            'stats': {
                'resumes_processed': len(submission_ids),
                'matches_found': len(matches),
                'jobs_matched': len(matches_by_job),
                'rerank_results': len(rerank_results),
                'ai_results': len(ai_results)
            },
            'processed_at': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка в цепочке A: {e}")
        if self.request.retries < self.max_retries:
            logger.info(f"🔄 Повторная попытка цепочки A {self.request.retries + 1}/{self.max_retries}")
            raise self.retry(countdown=60 * (self.request.retries + 1))
        raise
        
        if search_result.get('status') != 'success':
            logger.warning("⚠️ Поиск вакансий не дал результатов")
            return {
                'status': 'no_matches',
                'message': 'Поиск не дал результатов',
                'chain': 'A'
            }
        
        # Шаг A.3: Реранкинг резюме
        matches = search_result.get('matches', [])
        if matches:
            logger.info(f"🎯 A.3: Реранкинг {len(matches)} совпадений")
            
            # ИСПРАВЛЕНИЕ: Используем send_task вместо прямого импорта
            rerank_result = app.send_task(
                'tasks.scoring_tasks.rerank_resume_matches',
                args=[matches],
                queue='scoring_gpu'
            ).get(timeout=300)  # 5 минут на реранкинг
            
            # Группируем matches по job_id для реранкинга
            matches_by_job = {}
            for match in matches:
                job_id = match.get('job_id')
                if job_id:
                    if job_id not in matches_by_job:
                        matches_by_job[job_id] = []
                    matches_by_job[job_id].append(match)
            
            rerank_results = []
            for job_id, job_matches in matches_by_job.items():
                rerank_result = rerank_resume_matches.apply_async(
                    args=[job_id, job_matches],
                    queue='scoring_tasks'
                ).get(timeout=180)  # 3 минуты на реранкинг
                rerank_results.append(rerank_result)
        else:
            logger.info("ℹ️ A.3: Нет совпадений для реранкинга")
            rerank_results = []
        
        # Шаг A.4: Сохранение результатов анализа (ИСПРАВЛЕНИЕ: убираем циклический импорт)
        if rerank_results:
            logger.info("💾 A.4: Сохранение результатов анализа")
            for rerank_result in rerank_results:
                if rerank_result.get('status') == 'success':
                    job_id = rerank_result.get('job_id')
                    app.send_task(
                        'tasks.analysis_tasks.save_reranker_analysis_results',
                        args=[job_id, rerank_result],
                        queue='default'
                    )
        
        logger.info("✅ Цепочка A завершена успешно")
        return {
            'status': 'success',
            'message': f'Цепочка A завершена: обработано {len(submission_ids)} резюме',
            'chain': 'A',
            'submission_ids': submission_ids,
            'matches_found': len(matches),
            'rerank_results': len(rerank_results),
            'processed_at': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка в цепочке A: {e}")
        if self.request.retries < self.max_retries:
            logger.info(f"🔄 Повторная попытка цепочки A {self.request.retries + 1}/{self.max_retries}")
            raise self.retry(countdown=60 * (self.request.retries + 1))
        raise


@app.task(
    bind=True, 
    name='tasks.workflows.job_processing_chain', 
    soft_time_limit=2400,  # 40 минут  
    time_limit=2700,       # 45 минут
    max_retries=3
)
def job_processing_chain(self) -> Dict[str, Any]:
    """
    ЦЕПОЧКА B: Полная обработка вакансий
    
    Последовательность:
    1. pull_fillout_jobs (получение данных)
    2. parse_documents (парсинг вакансий)
    3. check_and_start_gpu_server (проверка GPU)
    4. generate_job_embeddings
    5. batch_find_matches_for_jobs
    6. rerank_job_matches
    7. ai_analysis (если GPU доступен)
    8. save_analysis_results
    
    Returns:
        Результат выполнения цепочки
    """
    logger.info("🔗 Цепочка B: Начало полной обработки вакансий")
    
    try:
        # Шаг B.0: Получение данных вакансий из Fillout
        logger.info("📥 B.0: Получение данных вакансий из Fillout API")
        fillout_result = app.send_task(
            'tasks.fillout_tasks.pull_fillout_jobs',
            queue='fillout'
        ).get(timeout=300)
        
        if not fillout_result or fillout_result.get('status') != 'completed':
            logger.warning("⚠️ Данные вакансий не получены из Fillout")
            return {
                'status': 'no_data',
                'message': 'Данные вакансий не получены из Fillout',
                'chain': 'B'
            }
        
        # Извлекаем job_ids из результата Fillout
        company_data = fillout_result.get('company_data', {})
        job_ids = company_data.get('job_ids', [])
        
        if not job_ids:
            logger.warning("⚠️ Нет job_ids для обработки")
            return {
                'status': 'no_data',
                'message': 'Нет job_ids для обработки',
                'chain': 'B'
            }
        
        # Подготавливаем данные документов для парсинга
        documents_data = []
        for job_id in job_ids:
            documents_data.append({
                'id': job_id,
                'job_id': job_id,
                'type': 'job_description'
            })
        
        # Шаг B.1: Парсинг документов вакансий
        logger.info(f"📄 B.1: Парсинг {len(documents_data)} документов вакансий")
        parse_result = app.send_task(
            'tasks.parse_tasks.parse_documents',
            args=[documents_data, 'job_description'],
            queue='cpu_intensive'
        ).get(timeout=600)  # 10 минут на парсинг
        
        if parse_result.get('status') != 'completed':
            logger.error("❌ Ошибка парсинга документов вакансий")
            return {
                'status': 'error',
                'message': 'Ошибка парсинга документов',
                'chain': 'B'
            }
        
        # Шаг B.2: Проверка и запуск GPU сервера
        logger.info("🔍 B.2: Проверка доступности GPU сервера")
        gpu_check_result = app.send_task(
            'tasks.gpu_tasks.check_and_start_gpu_server',
            args=['job_processing'],
            queue='system'
        ).get(timeout=360)  # 6 минут на проверку и запуск GPU
        
        gpu_available = gpu_check_result.get('status') in ['available', 'started_and_available']
        
        # Шаг B.3: Генерация эмбеддингов вакансий
        logger.info(f"📊 B.3: Генерация эмбеддингов для {len(job_ids)} вакансий")
        if gpu_available:
            embeddings_result = app.send_task(
                'tasks.embedding_tasks.generate_job_embeddings',
                args=[job_ids],
                queue='embeddings_gpu'
            ).get(timeout=600)  # 10 минут на эмбеддинги
        else:
            logger.warning("⚠️ GPU недоступен, используем CPU для эмбеддингов")
            embeddings_result = app.send_task(
                'tasks.embedding_tasks.generate_job_embeddings_cpu',
                args=[job_ids],
                queue='embeddings_cpu'
            ).get(timeout=1200)  # 20 минут на CPU эмбеддинги
        
        if embeddings_result.get('status') != 'success':
            logger.error("❌ Ошибка генерации эмбеддингов вакансий")
            return {
                'status': 'error',
                'message': 'Ошибка генерации эмбеддингов',
                'chain': 'B'
            }
        
        # Шаг B.4: Пакетный поиск резюме для вакансий
        logger.info(f"🔍 B.4: Пакетный поиск резюме для вакансий")
        search_result = app.send_task(
            'tasks.matching.batch_find_matches_for_jobs',
            args=[job_ids],
            queue='search_basic'
        ).get(timeout=300)  # 5 минут на поиск
        
        if search_result.get('status') != 'success':
            logger.error("❌ Ошибка поиска совпадений")
            return {
                'status': 'error',
                'message': 'Ошибка поиска совпадений',
                'chain': 'B'
            }
        
        # Шаг B.5: Реранкинг результатов
        matches = search_result.get('matches', [])
        if matches:
            logger.info(f"🎯 B.5: Реранкинг {len(matches)} совпадений")
            
            # Группируем совпадения по submission_id для реранкинга
            matches_by_submission = {}
            for match in matches:
                submission_id = match.get('submission_id')
                if submission_id:
                    if submission_id not in matches_by_submission:
                        matches_by_submission[submission_id] = []
                    matches_by_submission[submission_id].append(match)
            
            rerank_results = []
            for submission_id, submission_matches in matches_by_submission.items():
                rerank_result = app.send_task(
                    'tasks.scoring_tasks.rerank_job_matches',
                    args=[submission_id, submission_matches],
                    queue='scoring_tasks'
                ).get(timeout=180)  # 3 минуты на реранкинг
                rerank_results.append(rerank_result)
        else:
            logger.info("ℹ️ B.5: Нет совпадений для реранкинга")
            rerank_results = []
            matches_by_submission = {}
        
        # Шаг B.6: AI анализ (если GPU доступен)
        ai_results = []
        if gpu_available and rerank_results:
            logger.info("🤖 B.6: AI анализ результатов")
            
            # Подготавливаем данные для AI анализа
            ai_documents = {
                'documents': [],
                'type': 'job_candidate_matching'
            }
            
            for rerank_result in rerank_results:
                if rerank_result.get('status') == 'success':
                    ai_documents['documents'].extend(rerank_result.get('matches', []))
            
            if ai_documents['documents']:
                ai_result = app.send_task(
                    'tasks.gpu_tasks.ai_analysis',
                    args=[ai_documents, 'match_scoring'],
                    queue='ai_analysis'
                ).get(timeout=900)  # 15 минут на AI анализ
                
                if ai_result.get('status') == 'completed':
                    ai_results.append(ai_result)
                    logger.info("✅ AI анализ завершен успешно")
                else:
                    logger.warning("⚠️ AI анализ завершился с ошибкой")
        
        # Шаг B.7: Сохранение результатов анализа
        if rerank_results:
            logger.info("💾 B.7: Сохранение результатов анализа")
            for i, rerank_result in enumerate(rerank_results):
                if rerank_result.get('status') == 'success':
                    submission_id = rerank_result.get('submission_id')
                    
                    # Добавляем AI результаты если есть
                    final_result = rerank_result.copy()
                    if ai_results and i < len(ai_results):
                        final_result['ai_analysis'] = ai_results[i]
                    
                    app.send_task(
                        'tasks.analysis_tasks.save_analysis_results',
                        args=[submission_id, final_result, 'job_processing'],
                        queue='default'
                    )
        
        logger.info("✅ Цепочка B завершена успешно")
        return {
            'status': 'success',
            'message': f'Цепочка B завершена: обработано {len(job_ids)} вакансий',
            'chain': 'B',
            'steps_completed': {
                'fillout_data': True,
                'document_parsing': True,
                'gpu_check': True,
                'embeddings': True,
                'matching': True,
                'reranking': len(rerank_results) > 0,
                'ai_analysis': len(ai_results) > 0,
                'results_saved': True
            },
            'stats': {
                'jobs_processed': len(job_ids),
                'matches_found': len(matches),
                'submissions_matched': len(matches_by_submission),
                'rerank_results': len(rerank_results),
                'ai_results': len(ai_results)
            },
            'processed_at': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка в цепочке B: {e}")
        if self.request.retries < self.max_retries:
            logger.info(f"🔄 Повторная попытка цепочки B {self.request.retries + 1}/{self.max_retries}")
            raise self.retry(countdown=60 * (self.request.retries + 1))
        raise
    
    try:
        # Шаг B.0: Получение данных вакансий из Fillout
        logger.info("📥 B.0: Получение данных вакансий из Fillout API")
        fillout_result = app.send_task(
            'tasks.fillout_tasks.pull_fillout_jobs',
            queue='fillout'
        ).get(timeout=300)
        
        if not fillout_result or fillout_result.get('status') != 'completed':
            logger.warning("⚠️ Данные вакансий не получены из Fillout")
            return {
                'status': 'no_data',
                'message': 'Данные вакансий не получены из Fillout',
                'chain': 'B'
            }
        
        # Извлекаем job_ids из результата Fillout
        company_data = fillout_result.get('company_data', {})
        job_ids = company_data.get('job_ids', [])
        
        if not job_ids:
            logger.warning("⚠️ Нет job_ids для генерации эмбеддингов")
            return {
                'status': 'no_data',
                'message': 'Нет job_ids для обработки',
                'chain': 'B'
            }
        
        # Шаг B.1: Генерация эмбеддингов вакансий
        logger.info(f"📊 B.1: Генерация эмбеддингов для {len(job_ids)} вакансий")
        embeddings_result = app.send_task(
            'tasks.embedding_tasks.generate_job_embeddings',
            args=[job_ids],
            queue='embeddings_gpu'
        ).get(timeout=600)  # 10 минут на эмбеддинги
        
        if embeddings_result.get('status') != 'success':
            logger.error("❌ Ошибка генерации эмбеддингов вакансий")
            return {
                'status': 'error',
                'message': 'Ошибка генерации эмбеддингов',
                'chain': 'B'
            }
        
        # Шаг B.2: Пакетный поиск резюме для вакансий (ИСПРАВЛЕНИЕ: убираем циклический импорт)
        logger.info(f"🔍 B.2: Пакетный поиск резюме для вакансий")
        search_result = app.send_task(
            'tasks.matching.batch_find_matches_for_jobs',
            args=[job_ids],
            queue='search_basic'
        ).get(timeout=300)  # 5 минут на поиск
        
        if search_result.get('status') != 'success':
            logger.warning("⚠️ Поиск резюме не дал результатов")
            return {
                'status': 'no_matches',
                'message': 'Поиск не дал результатов',
                'chain': 'B'
            }
        
        # Шаг B.3: Реранкинг вакансий (ИСПРАВЛЕНИЕ: убираем циклический импорт)
        matches = search_result.get('matches', [])
        if matches:
            logger.info(f"🎯 B.3: Реранкинг {len(matches)} совпадений")
            
            # Группируем matches по submission_id для реранкинга
            matches_by_submission = {}
            for match in matches:
                submission_id = match.get('submission_id')
                if submission_id:
                    if submission_id not in matches_by_submission:
                        matches_by_submission[submission_id] = []
                    matches_by_submission[submission_id].append(match)
            
            rerank_results = []
            for submission_id, submission_matches in matches_by_submission.items():
                rerank_result = app.send_task(
                    'tasks.scoring_tasks.rerank_job_matches',
                    args=[submission_id, submission_matches],
                    queue='scoring_tasks'
                ).get(timeout=180)  # 3 минуты на реранкинг
                rerank_results.append(rerank_result)
        else:
            logger.info("ℹ️ B.3: Нет совпадений для реранкинга")
            rerank_results = []
        
        # Шаг B.4: Сохранение результатов анализа (ИСПРАВЛЕНИЕ: убираем циклический импорт)
        if rerank_results:
            logger.info("💾 B.4: Сохранение результатов анализа")
            for rerank_result in rerank_results:
                if rerank_result.get('status') == 'success':
                    # Для вакансий используем первый job_id из списка
                    job_id = job_ids[0] if job_ids else None
                    if job_id:
                        app.send_task(
                            'tasks.analysis_tasks.save_reranker_analysis_results',
                            args=[job_id, rerank_result],
                            queue='default'
                        )
        
        logger.info("✅ Цепочка B завершена успешно")
        return {
            'status': 'success',
            'message': f'Цепочка B завершена: обработано {len(job_ids)} вакансий',
            'chain': 'B',
            'job_ids': job_ids,
            'matches_found': len(matches),
            'rerank_results': len(rerank_results),
            'processed_at': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка в цепочке B: {e}")
        if self.request.retries < self.max_retries:
            logger.info(f"🔄 Повторная попытка цепочки B {self.request.retries + 1}/{self.max_retries}")
            raise self.retry(countdown=60 * (self.request.retries + 1))
        raise


# Задача scheduled_data_processing удалена - используются прямые цепочки
