"""
Централизованное управление секретами для production окружения
Использует Google Secret Manager для безопасного хранения секретов
"""

import os
import logging
from typing import Dict, Optional, Any, Union
import json

# Опциональный импорт Google Cloud секретов для production
try:
    from google.cloud import secretmanager
    from google.auth.exceptions import GoogleAuthError
    GOOGLE_CLOUD_AVAILABLE = True
except ImportError:
    secretmanager = None
    GoogleAuthError = Exception
    GOOGLE_CLOUD_AVAILABLE = False

logger = logging.getLogger(__name__)


class SecretManager:
    """Менеджер секретов с поддержкой Google Secret Manager и fallback на env переменные"""
    
    def __init__(self, project_id: Optional[str] = None):
        self.project_id = project_id or os.getenv('GOOGLE_CLOUD_PROJECT', '')
        self.environment = os.getenv('ENVIRONMENT', 'development')
        self._client = None
        self._secrets_cache = {}
        
        # Инициализация клиента Secret Manager для production
        if self.environment == 'production' and self.project_id and GOOGLE_CLOUD_AVAILABLE:
            try:
                self._client = secretmanager.SecretManagerServiceClient()
                logger.info(f"✅ Secret Manager инициализирован для проекта: {self.project_id}")
            except GoogleAuthError as e:
                logger.error(f"❌ Ошибка авторизации Google Cloud: {e}")
                self._client = None
            except Exception as e:
                logger.error(f"❌ Ошибка инициализации Secret Manager: {e}")
                self._client = None
        else:
            if not GOOGLE_CLOUD_AVAILABLE:
                logger.info(f"ℹ️ Google Cloud библиотеки не установлены")
            logger.info(f"ℹ️ Secret Manager отключен для {self.environment} окружения")
    
    def get_secret(self, secret_name: str, default: Optional[str] = None) -> Optional[str]:
        """
        Получение секрета из Google Secret Manager или переменных окружения
        
        Args:
            secret_name: Имя секрета
            default: Значение по умолчанию
            
        Returns:
            Значение секрета или default
        """
        # Проверяем кэш
        if secret_name in self._secrets_cache:
            return self._secrets_cache[secret_name]
        
        # Пытаемся получить из Secret Manager
        if self._client and self.project_id:
            try:
                secret_path = f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
                response = self._client.access_secret_version(request={"name": secret_path})
                secret_value = response.payload.data.decode("UTF-8")
                
                # Кэшируем секрет
                self._secrets_cache[secret_name] = secret_value
                logger.debug(f"✅ Секрет '{secret_name}' получен из Secret Manager")
                return secret_value
                
            except Exception as e:
                logger.warning(f"⚠️ Не удалось получить секрет '{secret_name}' из Secret Manager: {e}")
        
        # Fallback на переменные окружения
        env_value = os.getenv(secret_name, default)
        if env_value:
            logger.debug(f"✅ Секрет '{secret_name}' получен из переменных окружения")
            # Кэшируем для производительности
            self._secrets_cache[secret_name] = env_value
        else:
            logger.warning(f"⚠️ Секрет '{secret_name}' не найден ни в Secret Manager, ни в env")
        
        return env_value
    
    def get_database_config(self) -> Dict[str, Any]:
        """Получение конфигурации базы данных с секретами"""
        config = {}
        
        # Получаем DATABASE_URL целиком или собираем из частей
        database_url = self.get_secret('DATABASE_URL')
        if database_url:
            config['DATABASE_URL'] = database_url
        else:
            # Собираем из отдельных секретов
            config.update({
                'DB_HOST': self.get_secret('DB_HOST', 'localhost'),
                'DB_PORT': self.get_secret('DB_PORT', '5432'),
                'DB_NAME': self.get_secret('DB_NAME', 'hr_test'),
                'DB_USER': self.get_secret('DB_USER', 'test_user'),
                'DB_PASSWORD': self.get_secret('DB_PASSWORD', ''),
            })
            
            # Формируем DATABASE_URL
            config['DATABASE_URL'] = (
                f"postgresql://{config['DB_USER']}:{config['DB_PASSWORD']}@"
                f"{config['DB_HOST']}:{config['DB_PORT']}/{config['DB_NAME']}"
            )
        
        # SSL конфигурация для production
        if self.environment == 'production':
            config.update({
                'DB_SSL_MODE': self.get_secret('DB_SSL_MODE', 'require'),
                'DB_SSL_CERT': self.get_secret('DB_SSL_CERT'),
                'DB_SSL_KEY': self.get_secret('DB_SSL_KEY'),
                'DB_SSL_ROOTCERT': self.get_secret('DB_SSL_ROOTCERT'),
            })
        
        return config
    
    def get_redis_config(self) -> Dict[str, str]:
        """Получение конфигурации Redis с секретами"""
        return {
            'REDIS_URL': self.get_secret('REDIS_URL', 'redis://localhost:6379/0') or 'redis://localhost:6379/0',
            'REDIS_PASSWORD': self.get_secret('REDIS_PASSWORD', '') or '',
        }
    
    def get_celery_config(self) -> Dict[str, str]:
        """Получение конфигурации Celery с секретами"""
        redis_url = self.get_secret('REDIS_URL', 'redis://localhost:6379/0') or 'redis://localhost:6379/0'
        return {
            'CELERY_BROKER_URL': self.get_secret('CELERY_BROKER_URL') or redis_url,
            'CELERY_RESULT_BACKEND': self.get_secret('CELERY_RESULT_BACKEND') or redis_url,
        }
    
    def get_external_apis_config(self) -> Dict[str, str]:
        """Получение конфигурации внешних API с секретами"""
        return {
            'FILLOUT_API_KEY': self.get_secret('FILLOUT_API_KEY', '') or '',
            'OPENAI_API_KEY': self.get_secret('OPENAI_API_KEY', '') or '',
            'ANTHROPIC_API_KEY': self.get_secret('ANTHROPIC_API_KEY', '') or '',
        }
    
    def get_gcp_config(self) -> Dict[str, str]:
        """Получение конфигурации GCP с секретами"""
        return {
            'GOOGLE_CLOUD_PROJECT': self.get_secret('GOOGLE_CLOUD_PROJECT', self.project_id) or self.project_id or '',
            'GPU_INSTANCE_NAME': self.get_secret('GPU_INSTANCE_NAME', '') or '',
            'GPU_ZONE': self.get_secret('GPU_ZONE', 'us-central1-a') or 'us-central1-a',
            'GCP_SERVICE_ACCOUNT_KEY': self.get_secret('GCP_SERVICE_ACCOUNT_KEY', '') or '',
        }
    
    def validate_required_secrets(self) -> bool:
        """Проверка наличия всех необходимых секретов"""
        required_secrets = [
            'DATABASE_URL',
            'REDIS_URL',
        ]
        
        missing_secrets = []
        for secret in required_secrets:
            if not self.get_secret(secret):
                missing_secrets.append(secret)
        
        if missing_secrets:
            logger.error(f"❌ Отсутствуют обязательные секреты: {missing_secrets}")
            return False
        
        logger.info("✅ Все обязательные секреты найдены")
        return True
    
    def clear_cache(self):
        """Очистка кэша секретов"""
        self._secrets_cache.clear()
        logger.info("🗑️ Кэш секретов очищен")


# Глобальный экземпляр менеджера секретов
secret_manager = SecretManager()


def get_secret(secret_name: str, default: Optional[str] = None) -> Optional[str]:
    """Удобная функция для получения секрета"""
    return secret_manager.get_secret(secret_name, default)


def get_all_secrets() -> Dict[str, Any]:
    """Получение всех секретов для приложения"""
    config = {}
    config.update(secret_manager.get_database_config())
    config.update(secret_manager.get_redis_config())
    config.update(secret_manager.get_celery_config())
    config.update(secret_manager.get_external_apis_config())
    config.update(secret_manager.get_gcp_config())
    
    # Добавляем общие настройки
    config.update({
        'ENVIRONMENT': secret_manager.environment,
        'DEBUG': os.getenv('DEBUG', 'False').lower() == 'true',
        'LOG_LEVEL': get_secret('LOG_LEVEL', 'INFO'),
    })
    
    return config


def get_database_url_with_ssl() -> str:
    """Получить URL базы данных с SSL параметрами для безопасного подключения"""
    config = secret_manager.get_database_config()
    
    # Базовый URL
    db_url = config.get('DATABASE_URL', '')
    
    # Добавляем SSL параметры для production
    if secret_manager.environment == 'production':
        ssl_params = []
        
        # Обязательный SSL для production
        ssl_mode = config.get('DB_SSL_MODE', 'require')
        ssl_params.append(f"sslmode={ssl_mode}")
        
        # Дополнительные SSL параметры если они есть
        if config.get('DB_SSL_CERT'):
            ssl_params.append(f"sslcert={config['DB_SSL_CERT']}")
        if config.get('DB_SSL_KEY'):
            ssl_params.append(f"sslkey={config['DB_SSL_KEY']}")
        if config.get('DB_SSL_ROOTCERT'):
            ssl_params.append(f"sslrootcert={config['DB_SSL_ROOTCERT']}")
        
        # Добавляем параметры к URL
        separator = '&' if '?' in db_url else '?'
        db_url = f"{db_url}{separator}{'&'.join(ssl_params)}"
    else:
        # Для development используем prefer (пытаемся SSL, но не требуем)
        separator = '&' if '?' in db_url else '?'
        db_url = f"{db_url}{separator}sslmode=prefer"
    
    return db_url


def get_redis_url_with_auth() -> str:
    """Получить URL Redis с аутентификацией"""
    redis_config = secret_manager.get_redis_config()
    redis_url = redis_config.get('REDIS_URL', 'redis://localhost:6379/0')
    redis_password = redis_config.get('REDIS_PASSWORD', '')
    
    # Если пароль указан, но не включен в URL, добавляем его
    if redis_password and '://:' not in redis_url and redis_password not in redis_url:
        # Парсим URL и добавляем пароль
        if redis_url.startswith('redis://'):
            redis_url = redis_url.replace('redis://', f'redis://:{redis_password}@')
        elif redis_url.startswith('rediss://'):
            redis_url = redis_url.replace('rediss://', f'rediss://:{redis_password}@')
    
    return redis_url


def validate_security_settings() -> Dict[str, bool]:
    """Валидация настроек безопасности"""
    security_checks = {}
    
    # Проверка SSL для production
    if secret_manager.environment == 'production':
        security_checks['ssl_enabled'] = 'sslmode=require' in get_database_url_with_ssl()
        security_checks['redis_auth'] = bool(secret_manager.get_secret('REDIS_PASSWORD'))
    else:
        security_checks['ssl_enabled'] = True  # Не критично для dev
        security_checks['redis_auth'] = True   # Не критично для dev
    
    # Проверка наличия критически важных секретов
    critical_secrets = ['DATABASE_URL', 'REDIS_URL']
    for secret in critical_secrets:
        security_checks[f'{secret.lower()}_present'] = bool(secret_manager.get_secret(secret))
    
    # Проверка отсутствия дефолтных паролей
    security_checks['no_default_passwords'] = _check_no_default_passwords()
    
    return security_checks


def _check_no_default_passwords() -> bool:
    """Проверка отсутствия дефолтных/небезопасных паролей"""
    dangerous_defaults = ['password', 'admin', '123456', 'root', 'postgres', '']
    
    db_password = secret_manager.get_secret('DB_PASSWORD', '')
    redis_password = secret_manager.get_secret('REDIS_PASSWORD', '')
    
    # В production пароли не должны быть пустыми или дефолтными
    if secret_manager.environment == 'production':
        if not db_password or db_password.lower() in dangerous_defaults:
            logger.warning("⚠️ Небезопасный пароль базы данных")
            return False
        if not redis_password or redis_password.lower() in dangerous_defaults:
            logger.warning("⚠️ Небезопасный пароль Redis")
            return False
    
    return True
