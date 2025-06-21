"""
Logging configuration for Celery system
"""

import logging
import os
from typing import Dict, Any


def get_logging_config() -> Dict[str, Any]:
    """Get logging configuration based on environment"""
    environment = os.getenv('ENVIRONMENT', 'development')
    
    base_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            },
            'detailed': {
                'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'INFO',
                'formatter': 'standard',
                'stream': 'ext://sys.stdout'
            }
        },
        'root': {
            'level': 'INFO',
            'handlers': ['console']
        }
    }
    
    if environment == 'production':
        # In production, add file logging and reduce console verbosity
        base_config['handlers']['file'] = {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'INFO',
            'formatter': 'detailed',
            'filename': 'logs/celery.log',
            'maxBytes': 10485760,  # 10MB
            'backupCount': 5
        }
        base_config['root']['handlers'].append('file')
        
    elif environment == 'development':
        # In development, more verbose logging
        base_config['root']['level'] = 'DEBUG'
        base_config['handlers']['console']['level'] = 'DEBUG'
    
    return base_config


def setup_celery_logging():
    """Setup Celery-specific logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get Celery task logger
    from celery.utils.log import get_task_logger
    return get_task_logger(__name__)
