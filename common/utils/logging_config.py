"""
Централизованная система логирования для всех компонентов приложения
Поддерживает структурированное логирование, ротацию файлов и отправку в Google Cloud Logging
"""

import os
import sys
import logging
import logging.handlers
import json
import traceback
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path

# Опциональный импорт Google Cloud Logging
try:
    from google.cloud import logging as gcp_logging
    GCP_LOGGING_AVAILABLE = True
except ImportError:
    GCP_LOGGING_AVAILABLE = False


class StructuredFormatter(logging.Formatter):
    """Форматировщик для структурированных логов в JSON формате"""
    
    def format(self, record: logging.LogRecord) -> str:
        # Базовая структура лога
        log_entry = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
        }
        
        # Добавляем дополнительные поля если они есть
        if hasattr(record, 'extra_fields') and getattr(record, 'extra_fields', None):
            log_entry.update(getattr(record, 'extra_fields'))
        
        # Добавляем исключение если есть
        if record.exc_info:
            log_entry['exception'] = {
                'type': record.exc_info[0].__name__ if record.exc_info[0] else None,
                'message': str(record.exc_info[1]) if record.exc_info[1] else None,
                'traceback': traceback.format_exception(*record.exc_info)
            }
        
        # Добавляем контекст окружения
        log_entry['environment'] = {
            'process_id': os.getpid(),
            'environment': os.getenv('ENVIRONMENT', 'development'),
            'hostname': os.getenv('HOSTNAME', 'unknown'),
            'service': os.getenv('SERVICE_NAME', 'hr-analysis'),
        }
        
        return json.dumps(log_entry, ensure_ascii=False)


class CloudLoggingHandler(logging.Handler):
    """Обработчик для отправки логов в Google Cloud Logging"""
    
    def __init__(self, project_id: Optional[str] = None):
        super().__init__()
        self.project_id = project_id or os.getenv('GOOGLE_CLOUD_PROJECT')
        self.client = None
        
        if GCP_LOGGING_AVAILABLE and self.project_id:
            try:
                self.client = gcp_logging.Client(project=self.project_id)
                self.cloud_logger = self.client.logger('hr-analysis')
                self.setFormatter(StructuredFormatter())
            except Exception as e:
                print(f"⚠️ Не удалось инициализировать Google Cloud Logging: {e}")
                self.client = None
    
    def emit(self, record: logging.LogRecord):
        if not self.client:
            return
        
        try:
            # Преобразуем запись в структурированный формат
            formatted_message = self.format(record)
            log_data = json.loads(formatted_message)
            
            # Отправляем в Cloud Logging
            severity = self._get_cloud_severity(record.levelname)
            self.cloud_logger.log_struct(log_data, severity=severity)
            
        except Exception as e:
            # Не логируем ошибки логирования чтобы избежать рекурсии
            print(f"⚠️ Ошибка отправки лога в Cloud: {e}")
    
    def _get_cloud_severity(self, level: str) -> str:
        """Преобразование уровней логирования для Google Cloud"""
        mapping = {
            'DEBUG': 'DEBUG',
            'INFO': 'INFO',
            'WARNING': 'WARNING',
            'ERROR': 'ERROR',
            'CRITICAL': 'CRITICAL'
        }
        return mapping.get(level, 'INFO')


class LoggingConfig:
    """Централизованная конфигурация логирования"""
    
    def __init__(self, service_name: str = 'hr-analysis'):
        self.service_name = service_name
        self.environment = os.getenv('ENVIRONMENT', 'development')
        self.log_level = os.getenv('LOG_LEVEL', 'INFO')
        self.log_dir = Path(os.getenv('LOG_DIR', './logs'))
        self.enable_cloud_logging = os.getenv('ENABLE_CLOUD_LOGGING', 'false').lower() == 'true'
        self.enable_file_logging = os.getenv('ENABLE_FILE_LOGGING', 'true').lower() == 'true'
        self.enable_console_logging = os.getenv('ENABLE_CONSOLE_LOGGING', 'true').lower() == 'true'
        
        # Создаем директорию для логов
        if self.enable_file_logging:
            self.log_dir.mkdir(parents=True, exist_ok=True)
    
    def setup_logging(self) -> logging.Logger:
        """Настройка централизованного логирования"""
        # Очищаем существующие обработчики
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        
        # Устанавливаем уровень логирования
        root_logger.setLevel(getattr(logging, self.log_level.upper()))
        
        # Настройка консольного логирования
        if self.enable_console_logging:
            console_handler = logging.StreamHandler(sys.stdout)
            if self.environment == 'production':
                console_handler.setFormatter(StructuredFormatter())
            else:
                console_formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                )
                console_handler.setFormatter(console_formatter)
            root_logger.addHandler(console_handler)
        
        # Настройка файлового логирования
        if self.enable_file_logging:
            # Основной файл логов
            main_log_file = self.log_dir / f'{self.service_name}.log'
            file_handler = logging.handlers.RotatingFileHandler(
                main_log_file,
                maxBytes=50 * 1024 * 1024,  # 50MB
                backupCount=10,
                encoding='utf-8'
            )
            file_handler.setFormatter(StructuredFormatter())
            root_logger.addHandler(file_handler)
            
            # Отдельный файл для ошибок
            error_log_file = self.log_dir / f'{self.service_name}-errors.log'
            error_handler = logging.handlers.RotatingFileHandler(
                error_log_file,
                maxBytes=10 * 1024 * 1024,  # 10MB
                backupCount=5,
                encoding='utf-8'
            )
            error_handler.setLevel(logging.ERROR)
            error_handler.setFormatter(StructuredFormatter())
            root_logger.addHandler(error_handler)
        
        # Настройка Cloud Logging для production
        if self.enable_cloud_logging and self.environment == 'production':
            cloud_handler = CloudLoggingHandler()
            if cloud_handler.client:
                root_logger.addHandler(cloud_handler)
                print("✅ Google Cloud Logging включен")
            else:
                print("⚠️ Google Cloud Logging не удалось настроить")
        
        # Логгер для приложения
        app_logger = logging.getLogger(self.service_name)
        app_logger.info(f"🚀 Логирование настроено для {self.service_name}")
        app_logger.info(f"📊 Уровень: {self.log_level}, Окружение: {self.environment}")
        
        return app_logger
    
    def get_component_logger(self, component_name: str) -> logging.Logger:
        """Получение логгера для конкретного компонента"""
        logger_name = f"{self.service_name}.{component_name}"
        return logging.getLogger(logger_name)


class LogContext:
    """Менеджер контекста для добавления дополнительных полей в логи"""
    
    def __init__(self, **kwargs):
        self.extra_fields = kwargs
        self._original_factory = None
    
    def __enter__(self):
        # Сохраняем оригинальную фабрику записей
        self._original_factory = logging.getLogRecordFactory()
        
        # Создаем новую фабрику с дополнительными полями
        def record_factory(*args, **kwargs):
            record = self._original_factory(*args, **kwargs) if self._original_factory else logging.LogRecord(*args, **kwargs)
            setattr(record, 'extra_fields', self.extra_fields)
            return record
        
        logging.setLogRecordFactory(record_factory)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Восстанавливаем оригинальную фабрику
        if self._original_factory:
            logging.setLogRecordFactory(self._original_factory)


# Глобальная конфигурация логирования
_logging_config = None
_app_logger = None


def setup_logging(service_name: str = 'hr-analysis') -> logging.Logger:
    """
    Глобальная функция для настройки логирования
    
    Args:
        service_name: Имя сервиса
        
    Returns:
        Настроенный логгер приложения
    """
    global _logging_config, _app_logger
    
    if _logging_config is None:
        _logging_config = LoggingConfig(service_name)
        _app_logger = _logging_config.setup_logging()
    
    return _app_logger or logging.getLogger(service_name)


def get_logger(component_name: str = '') -> logging.Logger:
    """
    Получение логгера для компонента
    
    Args:
        component_name: Имя компонента
        
    Returns:
        Логгер для компонента
    """
    if _logging_config is None:
        setup_logging()
    
    if component_name and _logging_config:
        return _logging_config.get_component_logger(component_name)
    else:
        return _app_logger or logging.getLogger()


def log_with_context(**context):
    """
    Декоратор для добавления контекста в логи функции
    
    Usage:
        @log_with_context(task_id='12345', user_id='user1')
        def my_function():
            logger = get_logger('my_component')
            logger.info("Выполняется задача")
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            with LogContext(**context):
                return func(*args, **kwargs)
        return wrapper
    return decorator


# Настройка логирования при импорте модуля
if __name__ != "__main__":
    setup_logging()
