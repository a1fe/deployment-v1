"""
Цепочки задач для автоматической обработки данных HR Analysis
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


@app.task(
    bind=True, 
    name='common.tasks.workflows.run_full_processing_pipeline',
    soft_time_limit=2400,  # 40 минут
    time_limit=2700,       # 45 минут
    max_retries=3
)
def run_full_processing_pipeline(self) -> Dict[str, Any]:
    """
    Полный цикл обработки данных HR Analysis
    
    Последовательность:
    1. Получение данных из Fillout
    2. Парсинг текстов
    3. Генерация эмбеддингов
    4. Поиск и реранжирование
    5. Сохранение результатов
    
    Returns:
        Результат выполнения цепочки
    """
    logger.info("🔗 Запуск полного цикла обработки данных HR Analysis")
    
    try:
        results = {
            'status': 'started',
            'timestamp': datetime.now().isoformat(),
            'steps': []
        }
        
        # Шаг 1: Получение данных из Fillout
        logger.info("📋 Шаг 1: Получение данных из Fillout")
        resume_result = app.send_task(
            'common.tasks.fillout_tasks.fetch_resume_data'
        ).get(timeout=300)
        results['steps'].append({
            'step': 'fetch_resume_data',
            'status': 'completed',
            'result': resume_result
        })
        
        company_result = app.send_task(
            'common.tasks.fillout_tasks.fetch_company_data'
        ).get(timeout=300)
        results['steps'].append({
            'step': 'fetch_company_data',
            'status': 'completed',
            'result': company_result
        })
        
        # Шаг 2: Генерация эмбеддингов
        logger.info("🧠 Шаг 2: Генерация эмбеддингов")
        embeddings_result = app.send_task(
            'common.tasks.embedding_tasks.generate_all_embeddings'
        ).get(timeout=600)
        results['steps'].append({
            'step': 'generate_all_embeddings',
            'status': 'completed',
            'result': embeddings_result
        })
        
        # Шаг 3: Запуск задач реранжирования
        logger.info("🎯 Шаг 3: Запуск задач реранжирования")
        reranking_result = app.send_task(
            'common.tasks.workflows.launch_reranking_tasks'
        ).get(timeout=900)
        results['steps'].append({
            'step': 'launch_reranking_tasks',
            'status': 'completed',
            'result': reranking_result
        })
        
        results['status'] = 'completed'
        results['total_time'] = (datetime.now() - datetime.fromisoformat(results['timestamp'])).total_seconds()
        
        logger.info(f"✅ Полный цикл обработки завершен за {results['total_time']} секунд")
        return results
        
    except Exception as e:
        logger.error(f"❌ Ошибка в полном цикле обработки: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }


@app.task(
    bind=True,
    name='common.tasks.workflows.run_parsing_only',
    soft_time_limit=600,
    time_limit=900,
    max_retries=2
)
def run_parsing_only(self) -> Dict[str, Any]:
    """Запуск только парсинг задач"""
    logger.info("📝 Запуск парсинг задач")
    
    try:
        # Параллельный запуск парсинг задач
        parse_tasks = group([
            app.signature('common.tasks.parsing_tasks.parse_resume_text'),
            app.signature('common.tasks.parsing_tasks.parse_job_text')
        ])
        
        results = parse_tasks.apply_async()
        parsed_results = results.get(timeout=600)
        
        logger.info("✅ Парсинг задачи завершены")
        return {
            'status': 'completed',
            'results': parsed_results,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка в парсинг задачах: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }


@app.task(
    bind=True,
    name='common.tasks.workflows.run_embeddings_only',
    soft_time_limit=1200,
    time_limit=1800,
    max_retries=2
)
def run_embeddings_only(self) -> Dict[str, Any]:
    """Запуск только задач генерации эмбеддингов"""
    logger.info("🧠 Запуск задач генерации эмбеддингов")
    
    try:
        result = app.send_task('common.tasks.embedding_tasks.generate_all_embeddings')
        embeddings_result = result.get(timeout=1200)
        
        logger.info("✅ Задачи генерации эмбеддингов завершены")
        return {
            'status': 'completed',
            'result': embeddings_result,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка в задачах эмбеддингов: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }


@app.task(
    bind=True,
    name='common.tasks.workflows.run_reranking_only',
    soft_time_limit=600,
    time_limit=900,
    max_retries=2
)
def run_reranking_only(self) -> Dict[str, Any]:
    """Запуск только задач реранжирования"""
    logger.info("🎯 Запуск задач реранжирования")
    
    try:
        result = app.send_task('common.tasks.workflows.launch_reranking_tasks')
        reranking_result = result.get(timeout=600)
        
        logger.info("✅ Задачи реранжирования завершены")
        return {
            'status': 'completed',
            'result': reranking_result,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка в задачах реранжирования: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }


@app.task(
    bind=True,
    name='common.tasks.workflows.launch_reranking_tasks',
    soft_time_limit=600,
    time_limit=900,
    max_retries=2
)
def launch_reranking_tasks(self) -> Dict[str, Any]:
    """Запуск группы задач реранжирования"""
    logger.info("🎯 Запуск группы задач реранжирования")
    
    try:
        # Параллельный запуск задач реранжирования
        rerank_tasks = group([
            app.signature('common.tasks.reranking_tasks.rerank_jobs_for_resume'),
            app.signature('common.tasks.reranking_tasks.rerank_resumes_for_job')
        ])
        
        results = rerank_tasks.apply_async()
        rerank_results = results.get(timeout=600)
        
        logger.info("✅ Группа задач реранжирования завершена")
        return {
            'status': 'completed',
            'results': rerank_results,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка в группе задач реранжирования: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }
