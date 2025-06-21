"""
Environment-specific configuration for Celery
"""

import os
from typing import Dict, Any


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
    Get task routing configuration with conditional GPU support.
    
    Если GPU_INSTANCE_NAME настроен - GPU задачи идут на GPU-очереди
    Если нет - GPU задачи выполняются на CPU-очередях
    
    ✅ АКТИВНЫЕ МАРШРУТЫ (используются в основных workflow):
    - Основные цепочки workflow
    - Задачи Fillout (получение данных)
    - Задачи генерации эмбеддингов (условно GPU/CPU)
    - Задачи поиска (matching)
    - Задачи скоринга (reranking, условно GPU/CPU)
    - Задачи анализа (сохранение результатов)
    """
    gpu_enabled = bool(os.environ.get('GPU_INSTANCE_NAME'))
    
    # Базовые маршруты (всегда одинаковые)
    routes = {
        # 🔄 Workflow задачи (ВКЛЮЧЕНЫ)
        'tasks.workflows.process_resume_workflow': {'queue': 'default'},
        'tasks.workflows.process_job_workflow': {'queue': 'default'},
        'tasks.workflows.enhanced_resume_search_workflow': {'queue': 'default'},
        'tasks.workflows.enhanced_job_search_workflow': {'queue': 'default'},
        
        # 📋 Fillout задачи (ВКЛЮЧЕНЫ)
        'tasks.fillout_tasks.fetch_fillout_responses': {'queue': 'fillout'},
        'tasks.fillout_tasks.process_fillout_response': {'queue': 'fillout'},
        'tasks.fillout_tasks.process_fillout_batch': {'queue': 'fillout'},
        
        # 🔍 Поиск и сопоставление (ВКЛЮЧЕНЫ)
        'tasks.matching.batch_find_matches_for_resumes': {'queue': 'search_basic'},
        'tasks.matching.batch_find_matches_for_jobs': {'queue': 'search_basic'},
        'tasks.matching.find_matching_resumes_for_job': {'queue': 'search_basic'},
        'tasks.matching.find_matching_jobs_for_resume': {'queue': 'search_basic'},
        
        # 💾 Анализ задачи (сохранение результатов)
        'tasks.analysis_tasks.save_reranker_analysis_results': {'queue': 'default'},
        
        # 📧 Интеграция и уведомления
        'tasks.integration_tasks.*': {'queue': 'default'},
        'tasks.notification_tasks.*': {'queue': 'default'},
    }
    
    # Условная маршрутизация для GPU-задач
    if gpu_enabled:
        # GPU сервер настроен - направляем GPU задачи на специальные очереди
        routes.update({
            # 🧠 Embedding задачи (на GPU)
            'tasks.embedding_tasks.generate_resume_embeddings': {'queue': 'embeddings_gpu'},
            'tasks.embedding_tasks.generate_job_embeddings': {'queue': 'embeddings_gpu'},
            'tasks.embedding_tasks.search_similar_resumes': {'queue': 'embeddings_gpu'},
            'tasks.embedding_tasks.search_similar_jobs': {'queue': 'embeddings_gpu'},
            'tasks.embedding_tasks.cleanup_embeddings': {'queue': 'embeddings_gpu'},
            
            # 🎯 Скоринг задачи (на GPU)
            'tasks.scoring_tasks.rerank_resume_matches': {'queue': 'scoring_tasks'},
            'tasks.scoring_tasks.rerank_job_matches': {'queue': 'scoring_tasks'},
        })
    else:
        # GPU сервер НЕ настроен - выполняем GPU задачи на CPU
        routes.update({
            # 🧠 Embedding задачи (на CPU)
            'tasks.embedding_tasks.generate_resume_embeddings': {'queue': 'default'},
            'tasks.embedding_tasks.generate_job_embeddings': {'queue': 'default'},
            'tasks.embedding_tasks.search_similar_resumes': {'queue': 'default'},
            'tasks.embedding_tasks.search_similar_jobs': {'queue': 'default'},
            'tasks.embedding_tasks.cleanup_embeddings': {'queue': 'default'},
            
            # 🎯 Скоринг задачи (на CPU)
            'tasks.scoring_tasks.rerank_resume_matches': {'queue': 'default'},
            'tasks.scoring_tasks.rerank_job_matches': {'queue': 'default'},
        })
    
    return routes


def get_worker_configs() -> Dict[str, Dict[str, Any]]:
    """
    Конфигурация для каждого типа воркера с условной поддержкой GPU.
    
    Если GPU_INSTANCE_NAME настроен - создаем конфигурации для GPU воркеров
    Если нет - все задачи выполняются на CPU воркерах
    """
    gpu_enabled = bool(os.environ.get('GPU_INSTANCE_NAME'))
    
    # Базовые конфигурации воркеров (всегда нужны)
    configs = {
        # 📥 Воркер для Fillout API
        'fillout': {
            'concurrency': 2,
            'prefetch_multiplier': 1,
            'max_tasks_per_child': 100,
            'time_limit': 180,
            'soft_time_limit': 150,
        },
        
        # � Воркер для базового поиска
        'search_basic': {
            'concurrency': 2,
            'prefetch_multiplier': 1,
            'max_tasks_per_child': 100,
            'time_limit': 300,
            'soft_time_limit': 240,
        },
        
        # 💾 Дефолтный воркер
        'default': {
            'concurrency': 2,
            'prefetch_multiplier': 2,
            'max_tasks_per_child': 500,
            'time_limit': 300,
            'soft_time_limit': 240,
        },
    }
    
    # Условные конфигурации в зависимости от наличия GPU
    if gpu_enabled:
        # GPU сервер настроен - добавляем специализированные GPU воркеры
        configs.update({
            'embeddings_gpu': {
                'concurrency': 1,  # GPU задачи - только 1 процесс
                'prefetch_multiplier': 1,
                'max_tasks_per_child': 50,
                'time_limit': 600,
                'soft_time_limit': 540,
            },
            'scoring_tasks': {
                'concurrency': 1,  # Скоринг задачи - 1 процесс
                'prefetch_multiplier': 1,
                'max_tasks_per_child': 50,
                'time_limit': 300,
                'soft_time_limit': 240,
            },
        })
    
    return configs


def get_beat_schedule() -> Dict[str, Dict[str, Any]]:
    """Get periodic task schedule configuration"""
    return {
        # Цепочка обработки резюме (включает получение данных) - каждые 30 минут
        'resume-processing-chain': {
            'task': 'tasks.workflows.resume_processing_chain',
            'schedule': 1800.0,  # каждые 30 минут
        },
        # Цепочка обработки вакансий (включает получение данных) - каждые 45 минут
        'job-processing-chain': {
            'task': 'tasks.workflows.job_processing_chain',
            'schedule': 2700.0,  # каждые 45 минут
        },
    }
