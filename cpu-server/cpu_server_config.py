"""
Конфигурация Celery для CPU-сервера (основной сервер)

Содержит конфигурацию воркеров без GPU-задач
"""

import os
from typing import Dict, Any, List
from celery_app.celery_config import CeleryConfig


class CPUServerConfig(CeleryConfig):
    """Конфигурация Celery для CPU-сервера"""
    
    @staticmethod
    def get_worker_configs() -> Dict[str, Dict[str, Any]]:
        """Конфигурация воркеров для CPU-сервера (без GPU)"""
        return {
            # Основные воркеры для API и простых задач
            'default': {
                'concurrency': 2,
                'prefetch_multiplier': 2,
                'max_tasks_per_child': 500,
                'time_limit': 300,
                'soft_time_limit': 240,
            },
            
            # Воркеры для интеграции с Fillout
            'fillout': {
                'concurrency': 2,
                'prefetch_multiplier': 1, 
                'max_tasks_per_child': 100,
                'time_limit': 180,
                'soft_time_limit': 150,
            },
            
            # Воркеры для поисковых задач (без GPU)
            'search_basic': {
                'concurrency': 2,
                'prefetch_multiplier': 1,
                'max_tasks_per_child': 100,
                'time_limit': 300,
                'soft_time_limit': 240,
            },
        }
    
    @staticmethod
    def get_task_routes() -> Dict[str, Dict[str, str]]:
        """Маршрутизация задач для CPU-сервера"""
        gpu_enabled = bool(os.environ.get('GPU_INSTANCE_NAME'))
        
        routes = {
            # Задачи для обычных воркеров
            'tasks.workflows.*': {'queue': 'default'},
            'tasks.analysis_tasks.*': {'queue': 'default'},
            'tasks.notification_tasks.*': {'queue': 'default'},
            'tasks.integration_tasks.*': {'queue': 'default'},
            
            # Fillout задачи
            'tasks.fillout_tasks.*': {'queue': 'fillout'},
            
            # Поисковые задачи (без GPU)
            'tasks.matching.*': {'queue': 'search_basic'},
        }
        
        # Если GPU отключен, выполняем GPU-задачи на CPU
        if not gpu_enabled:
            routes.update({
                'tasks.embedding_tasks.*': {'queue': 'default'},
                'tasks.scoring_tasks.*': {'queue': 'default'},
            })
        else:
            # Если GPU включен, направляем на GPU-очереди
            routes.update({
                'tasks.embedding_tasks.*': {'queue': 'embeddings_gpu'},
                'tasks.scoring_tasks.*': {'queue': 'scoring_tasks'},
            })
        
        return routes
    
    @staticmethod
    def get_queue_list() -> List[str]:
        """Список очередей для CPU-сервера"""
        gpu_enabled = bool(os.environ.get('GPU_INSTANCE_NAME'))
        
        queues = ['default', 'fillout', 'search_basic']
        
        # Если GPU отключен, обрабатываем GPU-задачи на CPU
        if not gpu_enabled:
            queues.extend(['embeddings_gpu', 'scoring_tasks'])
        
        return queues
