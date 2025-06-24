"""
Environment-specific configuration for Celery
"""

import os
from typing import Dict, Any
from .queue_names import FILLOUT_PROCESSING_QUEUE, TEXT_PROCESSING_QUEUE, EMBEDDINGS_QUEUE, RERANKING_QUEUE, ORCHESTRATION_QUEUE


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
        # Workflow задачи (оркестрация)
        'tasks.workflows.*': {'queue': ORCHESTRATION_QUEUE},

        # Fillout задачи
        'tasks.fillout_tasks.*': {'queue': FILLOUT_PROCESSING_QUEUE},

        # Parsing задачи
        'tasks.parsing_tasks.*': {'queue': TEXT_PROCESSING_QUEUE},

        # Embedding задачи
        'tasks.embedding_tasks.*': {'queue': EMBEDDINGS_QUEUE},

        # Reranking задачи
        'tasks.reranking_tasks.*': {'queue': RERANKING_QUEUE},
    }
    
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
        
        # 🔍 Воркер для базового поиска
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
        # 🔗 ЦЕПОЧКА A: Обработка резюме (включает получение данных, парсинг, GPU анализ) - каждые 30 минут
        'resume-processing-chain': {
            'task': 'tasks.workflows.resume_processing_chain',
            'schedule': 1800.0,  # каждые 30 минут
            'options': {
                'queue': 'default',
                'priority': 8  # Высокий приоритет
            }
        },
        
        # 🔗 ЦЕПОЧКА B: Обработка вакансий (включает получение данных, парсинг, GPU анализ) - каждые 45 минут
        'job-processing-chain': {
            'task': 'tasks.workflows.job_processing_chain',
            'schedule': 2700.0,  # каждые 45 минут
            'options': {
                'queue': 'default',
                'priority': 8  # Высокий приоритет
            }
        },
        
        # 🔧 Проверка состояния GPU сервера - каждые 15 минут
        'gpu-health-check': {
            'task': 'tasks.gpu_tasks.gpu_health_check',
            'schedule': 900.0,  # каждые 15 минут
            'options': {
                'queue': 'system',
                'priority': 3  # Низкий приоритет
            }
        },
        
        # 🔍 Проверка и запуск GPU сервера при необходимости - каждые 2 часа
        'gpu-server-maintenance': {
            'task': 'tasks.gpu_tasks.check_and_start_gpu_server',
            'schedule': 7200.0,  # каждые 2 часа
            'args': ['maintenance'],
            'options': {
                'queue': 'system',
                'priority': 5  # Средний приоритет
            }
        },
        
        # 🗑️ Очистка старых данных эмбеддингов - каждые 24 часа
        'cleanup-embeddings': {
            'task': 'tasks.embedding_tasks.cleanup_embeddings',
            'schedule': 86400.0,  # каждые 24 часа
            'options': {
                'queue': 'default',
                'priority': 1  # Минимальный приоритет
            }
        }
    }
