"""
Redis and Celery configuration
"""

from .celery_app import get_celery_app

celery = get_celery_app()

__all__ = ['celery']
