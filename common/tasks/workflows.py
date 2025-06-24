"""
Workflow tasks for orchestrating the processing pipeline
"""

from typing import Dict, Any
from celery.utils.log import get_task_logger
from celery import group, chain, chord, signature
from celery_app.celery_app import celery_app
from database.operations.embedding_operations import embedding_crud
from database.config import database

from celery_app.celery_app import celery_app

logger = get_task_logger(__name__)


@celery_app.task(
    bind=True,
    name='tasks.workflows.run_full_processing_pipeline',
    soft_time_limit=3600,
    time_limit=4200,
    max_retries=2
)
def run_full_processing_pipeline(self, previous_results=None) -> Dict[str, Any]:
    """
    Полный chain-based pipeline обработки данных: получение → парсинг → эмбеддинги → реранкинг
    
    Использует Celery chain для автоматической передачи результатов между этапами.
    Каждый этап получает результаты предыдущей через параметр previous_results.
    
    Args:
        previous_results: Результаты предыдущих этапов (для совместимости с chain)
        
    Returns:
        Dict с информацией о запущенном pipeline
    """
    logger.info("🚀 Запуск полного chain-based pipeline обработки данных")
    
    # Если получены результаты предыдущего этапа, логируем их
    if previous_results:
        logger.info(f"📥 Получены результаты предыдущего этапа: {previous_results}")
    
    try:
        # Импортируем задачи
        from tasks.fillout_tasks import fetch_resume_data, fetch_company_data
        from tasks.parsing_tasks import parse_resume_text, parse_job_text
        from tasks.embedding_tasks import generate_resume_embeddings, generate_job_embeddings
        from tasks.reranking_tasks import rerank_resumes_for_job, rerank_jobs_for_resume
        
        def flatten(items):
            for x in items:
                if isinstance(x, (list, tuple)):
                    yield from flatten(x)
                else:
                    yield x

        # Создаем автоматический chain pipeline
        # Каждая задача автоматически получит результаты предыдущей через параметр previous_results
        embedding_group = group([
            generate_resume_embeddings.s(),
            generate_job_embeddings.s()
        ])
        pipeline_chain = chain(
            group([
                fetch_resume_data.s(),
                fetch_company_data.s()
            ]),
            group([
                parse_resume_text.s(),
                parse_job_text.s()
            ]),
            chord(embedding_group, launch_reranking_tasks.s())
        )
        # Запускаем chain и возвращаем AsyncResult без блокировки воркера
        result = pipeline_chain.apply_async()
        result_id = getattr(result, 'id', 'unknown')
        logger.info(f"✅ Chain pipeline запущен успешно: ID={result_id}")
        logger.info("📋 Этапы pipeline:")
        logger.info("  1. Получение данных: fetch_resume_data + fetch_company_data")
        logger.info("  2. Парсинг текстов: parse_resume_text + parse_job_text")
        logger.info("  3. Генерация эмбеддингов: generate_all_embeddings")
        logger.info("  4. Реранкинг: rerank_resumes_for_job + rerank_jobs_for_resume")
        logger.info(f"🔗 Результаты будут автоматически переданы между этапами")
        return {
            'status': 'pipeline_started',
            'pipeline_id': result_id,
            'message': 'Chain pipeline запущен, результаты передаются автоматически',
            'stages': [
                'data_fetching',
                'text_parsing', 
                'embedding_generation',
                'reranking'
            ],
            'tracking': 'Отслеживайте прогресс в Flower: http://localhost:5555'
        }
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка pipeline: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'processed_files': 0,
            'error_files': 0,
            'processed_embeddings': 0,
            'error_embeddings': 0
        }


@celery_app.task(
    bind=True,
    name='tasks.workflows.run_parsing_only',
    soft_time_limit=1800,
    time_limit=2100,
    max_retries=2
)
def run_parsing_only(self, previous_results=None) -> Dict[str, Any]:
    """
    Запуск только парсинга текстов резюме и вакансий
    
    Args:
        previous_results: Результаты предыдущих этапов (для совместимости с chain)
        
    Returns:
        Dict с результатами парсинга
    """
    logger.info("📄 Запуск парсинга текстов резюме и вакансий")
    
    if previous_results:
        logger.info(f"📥 Получены результаты предыдущего этапа: {previous_results}")
    
    try:
        from tasks.parsing_tasks import parse_resume_text, parse_job_text
        
        # Создаем pipeline только для парсинга
        parsing_chain = group([
            parse_resume_text.s(previous_results),
            parse_job_text.s(previous_results)
        ])
        
        # Запускаем парсинг
        result = parsing_chain.apply_async()
        result_id = getattr(result, 'id', 'unknown')
        
        logger.info(f"✅ Парсинг запущен: ID={result_id}")
        
        return {
            'status': 'parsing_started',
            'pipeline_id': result_id,
            'message': 'Парсинг текстов запущен',
            'stages': ['text_parsing'],
            'tracking': 'Отслеживайте прогресс в Flower: http://localhost:5555'
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка запуска парсинга: {e}")
        return {
            'status': 'error',
            'error': str(e)
        }


@celery_app.task(
    bind=True,
    name='tasks.workflows.run_embeddings_only',
    soft_time_limit=1800,
    time_limit=2100,
    max_retries=2
)
def run_embeddings_only(self, previous_results=None) -> Dict[str, Any]:
    """
    Запуск только генерации эмбеддингов
    
    Args:
        previous_results: Результаты предыдущих этапов (для совместимости с chain)
        
    Returns:
        Dict с результатами генерации эмбеддингов
    """
    logger.info("🧠 Запуск генерации эмбеддингов")
    
    if previous_results:
        logger.info(f"📥 Получены результаты предыдущего этапа: {previous_results}")
    
    try:
        from tasks.embedding_tasks import generate_resume_embeddings, generate_job_embeddings
        # Запускаем генерацию эмбеддингов как group
        embedding_group = group([
            generate_resume_embeddings.s(previous_results),
            generate_job_embeddings.s(previous_results)
        ])
        result = embedding_group.apply_async()
        result_id = getattr(result, 'id', 'unknown')
        logger.info(f"✅ Генерация эмбеддингов (group) запущена: ID={result_id}")
        return {
            'status': 'embeddings_started',
            'pipeline_id': result_id,
            'message': 'Генерация эмбеддингов (group) запущена',
            'stages': ['embedding_generation'],
            'tracking': 'Отслеживайте прогресс в Flower: http://localhost:5555'
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка запуска генерации эмбеддингов: {e}")
        return {
            'status': 'error',
            'error': str(e)
        }


@celery_app.task(
    bind=True,
    name='tasks.workflows.run_reranking_only',
    soft_time_limit=1800,
    time_limit=2100,
    max_retries=2
)
def run_reranking_only(self, previous_results=None) -> Dict[str, Any]:
    """
    Запуск только реранкинга
    
    Args:
        previous_results: Результаты предыдущих этапов (для совместимости с chain)
        
    Returns:
        Dict с результатами реранкинга
    """
    logger.info("🔄 Запуск реранкинга")
    
    if previous_results:
        logger.info(f"📥 Получены результаты предыдущего этапа: {previous_results}")
    
    try:
        # from tasks.reranking_tasks import process_all_reranking  # Отключено: задача временно не импортируется
        
        # Запускаем реранкинг  
        # result = process_all_reranking.apply_async(args=[previous_results])  # Отключено: задача временно не вызывается
        result_id = 'unknown'  # Временно, пока задача не будет подключена
        
        logger.info(f"✅ Реранкинг запущен: ID={result_id}")
        
        return {
            'status': 'reranking_started',
            'pipeline_id': result_id,
            'message': 'Реранкинг запущен',
            'stages': ['reranking'],
            'tracking': 'Отслеживайте прогресс в Flower: http://localhost:5555'
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка запуска реранкинга: {e}")
        return {
            'status': 'error',
            'error': str(e)
        }


@celery_app.task(
    bind=True,
    name='tasks.workflows.launch_reranking_tasks',
    soft_time_limit=1800,
    time_limit=2100,
    max_retries=2
)
def launch_reranking_tasks(self, results) -> Dict[str, Any]:
    """
    Запуск задач реранкинга по всем source_id из embedding_metadata
    """

    from celery_app.queue_names import RERANKING_QUEUE
    from utils.chroma_config import ChromaConfig
    logger.info("🔄 [RERANK] Задача launch_reranking_tasks вызвана!")
    db = database.get_session()
    try:
        # Получаем все source_id для резюме
        resume_ids = [e.source_id for e in db.query(embedding_crud.model).filter(
            embedding_crud.model.collection_name == ChromaConfig.RESUME_COLLECTION,
            embedding_crud.model.source_type == 'resume'
        ).all()]
        # Получаем все source_id для вакансий
        job_ids = [e.source_id for e in db.query(embedding_crud.model).filter(
            embedding_crud.model.collection_name == ChromaConfig.JOB_COLLECTION,
            embedding_crud.model.source_type == 'job_description'
        ).all()]
        logger.info(f"[RERANK] Найдено {len(resume_ids)} resume_ids: {resume_ids}")
        logger.info(f"[RERANK] Найдено {len(job_ids)} job_ids: {job_ids}")
        if not resume_ids and not job_ids:
            logger.warning("[RERANK] Нет ни одного id для реранка! Проверьте таблицу embedding_metadata.")
        # Импортируем Celery-задачи по имени
        rerank_resumes_for_job = celery_app.tasks['tasks.reranking_tasks.rerank_resumes_for_job']
        rerank_jobs_for_resume = celery_app.tasks['tasks.reranking_tasks.rerank_jobs_for_resume']
        # Запускаем задачи реранка с явным указанием очереди
        result = group([
            group([rerank_jobs_for_resume.s(sub_id).set(queue=RERANKING_QUEUE) for sub_id in resume_ids]),
            group([rerank_resumes_for_job.s(job_id).set(queue=RERANKING_QUEUE) for job_id in job_ids])
        ]).apply_async()
        logger.info(f"✅ Реранк запущен для {len(resume_ids)} резюме и {len(job_ids)} вакансий")
        return {
            'status': 'reranking_started',
            'resume_count': len(resume_ids),
            'job_count': len(job_ids),
            'message': 'Реранкинг запущен по всем source_id из embedding_metadata',
            'tracking': f'Отслеживайте прогресс в Flower: http://localhost:5555/task/{result.id}'
        }
    finally:
        db.close()


# Функции для ручного запуска pipeline (простые shortcuts)
def trigger_full_pipeline():
    """Запуск полного pipeline"""
    return run_full_processing_pipeline.delay()

def trigger_parsing_only():
    """Запуск только парсинга"""
    return run_parsing_only.delay()

def trigger_embeddings_only():
    """Запуск только эмбеддингов"""
    return run_embeddings_only.delay()

def trigger_reranking_only():
    """Запуск только реранкинга"""
    return run_reranking_only.delay()
