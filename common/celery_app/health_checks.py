"""
Health checks and system validation for Celery app
"""

import logging
from typing import Dict, Any
from celery_app.redis_manager import check_redis_connection, get_redis_connection_info

logger = logging.getLogger(__name__)


class SystemHealthChecker:
    """System health checker for Celery environment"""
    
    def __init__(self):
        self.checks = {
            'redis': self._check_redis,
            'environment': self._check_environment,
            'configuration': self._check_configuration
        }
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get system health status - direct method"""
        return self.run_all_checks()
    
    def _check_redis(self) -> Dict[str, Any]:
        """Check Redis health"""
        try:
            is_healthy = check_redis_connection()
            return {
                'status': 'healthy' if is_healthy else 'unhealthy',
                'details': get_redis_connection_info(),
                'error': None if is_healthy else 'Connection failed'
            }
        except Exception as e:
            return {
                'status': 'error',
                'details': 'Redis check failed',
                'error': str(e)
            }
    
    def _check_environment(self) -> Dict[str, Any]:
        """Check environment variables"""
        import os
        required_vars = ['ENVIRONMENT']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        return {
            'status': 'healthy' if not missing_vars else 'warning',
            'details': f"Environment: {os.getenv('ENVIRONMENT', 'development')}",
            'error': f"Missing variables: {missing_vars}" if missing_vars else None
        }
    
    def _check_configuration(self) -> Dict[str, Any]:
        """Check Celery configuration"""
        try:
            from celery_app.celery_env_config import get_environment_config
            config = get_environment_config()
            return {
                'status': 'healthy',
                'details': f"Configuration loaded: {len(config)} settings",
                'error': None
            }
        except Exception as e:
            return {
                'status': 'error',
                'details': 'Configuration check failed',
                'error': str(e)
            }
    
    def run_all_checks(self) -> Dict[str, Dict[str, Any]]:
        """Run all health checks"""
        results = {}
        for check_name, check_func in self.checks.items():
            try:
                results[check_name] = check_func()
            except Exception as e:
                results[check_name] = {
                    'status': 'error',
                    'details': f'{check_name} check failed',
                    'error': str(e)
                }
        return results
    
    def is_system_healthy(self) -> bool:
        """Check if entire system is healthy"""
        results = self.run_all_checks()
        return all(
            result['status'] in ['healthy', 'warning'] 
            for result in results.values()
        )
    
    def print_health_status(self):
        """Print formatted health status"""
        results = self.run_all_checks()
        
        print("🔍 System Health Check")
        print("=" * 50)
        
        for check_name, result in results.items():
            status_icon = {
                'healthy': '✅',
                'warning': '⚠️',
                'unhealthy': '❌',
                'error': '💥'
            }.get(result['status'], '❓')
            
            print(f"{status_icon} {check_name.title()}: {result['details']}")
            if result['error']:
                print(f"   Error: {result['error']}")
        
        if self.is_system_healthy():
            print("\n✅ System is ready for Celery operations!")
        else:
            print("\n❌ System has issues that need attention!")


def validate_system_readiness():
    """Validate system readiness for Celery operations"""
    checker = SystemHealthChecker()
    if not checker.is_system_healthy():
        logger.error("System health check failed")
        return False
    
    logger.info("System health check passed")
    return True


def get_system_status() -> Dict[str, Any]:
    """Get current system status"""
    checker = SystemHealthChecker()
    return checker.run_all_checks()


# Добавляем метод get_health_status для совместимости
def get_health_status() -> Dict[str, Any]:
    """Get current system health status"""
    return get_system_status()


# Определяем метод как часть класса SystemHealthChecker для явного добавления
def _add_health_status_method():
    def get_health_status(self):
        """Получить статус работоспособности системы"""
        return self.run_all_checks()
    
    # Динамически добавляем метод в класс
    setattr(SystemHealthChecker, 'get_health_status', get_health_status)

# Вызываем функцию для добавления метода
_add_health_status_method()
