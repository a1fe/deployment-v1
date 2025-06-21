"""
Конфигурация Celery для GPU-сервера

Содержит только GPU-интенсивные задачи
"""

import os
from typing import Dict, Any, List


class GPUServerConfig:
    """Конфигурация Celery для GPU-сервера"""
    
    @staticmethod
    def get_worker_configs() -> Dict[str, Dict[str, Any]]:
        """Конфигурация воркеров для GPU-сервера"""
        return {
            # Воркер для генерации эмбеддингов (GPU)
            'embeddings_gpu': {
                'concurrency': 1,  # Один процесс на GPU
                'prefetch_multiplier': 1,
                'max_tasks_per_child': 50,
                'time_limit': 600,  # 10 минут
                'soft_time_limit': 540,
            },
            
            # Воркер для скоринга с BGE Reranker (GPU)
            'scoring_tasks': {
                'concurrency': 1,  # Один процесс на GPU
                'prefetch_multiplier': 1,
                'max_tasks_per_child': 50,
                'time_limit': 300,  # 5 минут
                'soft_time_limit': 240,
            },
        }
    
    @staticmethod
    def get_task_routes() -> Dict[str, Dict[str, str]]:
        """Маршрутизация задач для GPU-сервера"""
        return {
            # GPU-интенсивные задачи
            'tasks.embedding_tasks.*': {'queue': 'embeddings_gpu'},
            'tasks.scoring_tasks.*': {'queue': 'scoring_tasks'},
        }
    
    @staticmethod
    def get_queue_list() -> List[str]:
        """Список очередей для GPU-сервера"""
        return ['embeddings_gpu', 'scoring_tasks']
    
    @staticmethod
    def get_redis_config() -> Dict[str, Any]:
        """Конфигурация Redis для подключения к основному серверу"""
        # Безопасное подключение к Redis
        from deployment.common.utils.secret_manager import get_redis_url_with_auth
        redis_url = get_redis_url_with_auth()
        
        return {
            'broker_url': redis_url,
            'result_backend': redis_url,
        }
