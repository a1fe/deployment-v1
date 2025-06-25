"""
Environment-specific configuration for Celery
"""

import os
from typing import Dict, Any
from .queue_names import (
    FILLOUT_PROCESSING_QUEUE, 
    TEXT_PROCESSING_QUEUE, 
    EMBEDDINGS_QUEUE, 
    RERANKING_QUEUE, 
    ORCHESTRATION_QUEUE
)


def get_environment_config(environment: str | None = None) -> Dict[str, Any]:
    """Get environment-specific Celery configuration"""
    if environment is None:
        environment = os.getenv('ENVIRONMENT', 'development')
    
    configs = {
        'production': {
            'task_always_eager': False,
            'task_eager_propagates': False,
            'worker_concurrency': 4,
            'task_compression': 'gzip',
            'worker_prefetch_multiplier': 1,
            'worker_max_tasks_per_child': 1000,
            'task_time_limit': 30 * 60,  # 30 minutes
            'task_soft_time_limit': 25 * 60,  # 25 minutes
        },
        'testing': {
            'task_always_eager': True,
            'task_eager_propagates': True,
            'worker_concurrency': 1,
            'task_time_limit': 5 * 60,  # 5 minutes for tests
        },
        'development': {
            'task_always_eager': False,
            'task_eager_propagates': False,
            'worker_concurrency': 2,
            'task_time_limit': 15 * 60,  # 15 minutes
            'task_soft_time_limit': 12 * 60,  # 12 minutes
        }
    }
    
    return configs.get(environment, configs['development'])


def get_task_routes() -> Dict[str, Dict[str, str]]:
    """
    Get task routing configuration using the new business queue architecture.
    
    Новая архитектура очередей:
    - fillout_processing: Получение данных из внешних источников
    - text_processing: Обработка и парсинг текстов
    - embeddings: Генерация эмбеддингов
    - reranking: AI-реранжирование результатов
    - orchestration: Управление workflow и координация задач
    """
    
    # Маршрутизация задач по бизнес-логике
    routes = {
        # 🔄 Workflow задачи (orchestration)
        'common.tasks.workflows.run_full_processing_pipeline': {'queue': ORCHESTRATION_QUEUE},
        'common.tasks.workflows.run_parsing_only': {'queue': ORCHESTRATION_QUEUE},
        'common.tasks.workflows.run_embeddings_only': {'queue': ORCHESTRATION_QUEUE},
        'common.tasks.workflows.run_reranking_only': {'queue': ORCHESTRATION_QUEUE},
        'common.tasks.workflows.launch_reranking_tasks': {'queue': ORCHESTRATION_QUEUE},
        
        # 📋 Fillout задачи (fillout_processing)
        'common.tasks.fillout_tasks.fetch_resume_data': {'queue': FILLOUT_PROCESSING_QUEUE},
        'common.tasks.fillout_tasks.fetch_company_data': {'queue': FILLOUT_PROCESSING_QUEUE},
        
        # � Парсинг задачи (text_processing)
        'common.tasks.parsing_tasks.parse_resume_text': {'queue': TEXT_PROCESSING_QUEUE},
        'common.tasks.parsing_tasks.parse_job_text': {'queue': TEXT_PROCESSING_QUEUE},
        
        # 🧠 Embedding задачи (embeddings)
        'common.tasks.embedding_tasks.generate_resume_embeddings': {'queue': EMBEDDINGS_QUEUE},
        'common.tasks.embedding_tasks.generate_job_embeddings': {'queue': EMBEDDINGS_QUEUE},
        'common.tasks.embedding_tasks.search_similar_resumes': {'queue': EMBEDDINGS_QUEUE},
        'common.tasks.embedding_tasks.search_similar_jobs': {'queue': EMBEDDINGS_QUEUE},
        'common.tasks.embedding_tasks.generate_all_embeddings': {'queue': EMBEDDINGS_QUEUE},
        
        # 🎯 Reranking задачи (reranking)
        'common.tasks.reranking_tasks.rerank_jobs_for_resume': {'queue': RERANKING_QUEUE},
        'common.tasks.reranking_tasks.rerank_resumes_for_job': {'queue': RERANKING_QUEUE},
    }
    
    return routes


def get_worker_configs() -> Dict[str, Dict[str, Any]]:
    """
    Конфигурация для каждого типа воркера с новой бизнес-архитектурой очередей.
    """
    
    configs = {
        # 📥 Воркер для получения данных из внешних источников
        'fillout_processing': {
            'concurrency': 2,
            'prefetch_multiplier': 1,
            'max_tasks_per_child': 100,
            'time_limit': 180,
            'soft_time_limit': 150,
        },
        
        # � Воркер для обработки и парсинга текстов
        'text_processing': {
            'concurrency': 2,
            'prefetch_multiplier': 1,
            'max_tasks_per_child': 100,
            'time_limit': 300,
            'soft_time_limit': 240,
        },
        
        # 🧠 Воркер для генерации эмбеддингов
        'embeddings': {
            'concurrency': 2,
            'prefetch_multiplier': 1,
            'max_tasks_per_child': 50,
            'time_limit': 600,
            'soft_time_limit': 540,
        },
        
        # 🎯 Воркер для AI-реранжирования результатов
        'reranking': {
            'concurrency': 1,  # Один процесс для AI задач
            'prefetch_multiplier': 1,
            'max_tasks_per_child': 50,
            'time_limit': 300,
            'soft_time_limit': 240,
        },
        
        # 🔄 Воркер для управления workflow и координации
        'orchestration': {
            'concurrency': 2,
            'prefetch_multiplier': 2,
            'max_tasks_per_child': 500,
            'time_limit': 300,
            'soft_time_limit': 240,
        },
    }
    
    return configs


def get_beat_schedule() -> Dict[str, Dict[str, Any]]:
    """Get periodic task schedule configuration"""
    return {
        # 🔗 Полный цикл обработки данных - каждые 30 минут
        'full-processing-pipeline': {
            'task': 'common.tasks.workflows.run_full_processing_pipeline',
            'schedule': 1800.0,  # каждые 30 минут
            'options': {
                'queue': ORCHESTRATION_QUEUE,
                'priority': 8  # Высокий приоритет
            }
        },
        
        # � Получение новых данных из Fillout - каждые 15 минут
        'fetch-resume-data': {
            'task': 'common.tasks.fillout_tasks.fetch_resume_data',
            'schedule': 900.0,  # каждые 15 минут
            'options': {
                'queue': FILLOUT_PROCESSING_QUEUE,
                'priority': 7
            }
        },
        
        # 🏢 Получение данных компаний - каждые 60 минут
        'fetch-company-data': {
            'task': 'common.tasks.fillout_tasks.fetch_company_data',
            'schedule': 3600.0,  # каждый час
            'options': {
                'queue': FILLOUT_PROCESSING_QUEUE,
                'priority': 5
            }
        },
        
        # 🧠 Генерация эмбеддингов - каждые 45 минут
        'generate-all-embeddings': {
            'task': 'common.tasks.embedding_tasks.generate_all_embeddings',
            'schedule': 2700.0,  # каждые 45 минут
            'options': {
                'queue': EMBEDDINGS_QUEUE,
                'priority': 6
            }
        },
        
        # 🎯 Запуск задач реранжирования - каждые 2 часа
        'launch-reranking-tasks': {
            'task': 'common.tasks.workflows.launch_reranking_tasks',
            'schedule': 7200.0,  # каждые 2 часа
            'options': {
                'queue': ORCHESTRATION_QUEUE,
                'priority': 5
            }
        }
    }
