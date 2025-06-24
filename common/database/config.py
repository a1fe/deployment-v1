"""
Конфигурация базы данных и подключение через SQLAlchemy
"""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import sys
sys.path.append('..')
from models.base import Base

# Загружаем переменные окружения
load_dotenv()


class DatabaseConfig:
    """Конфигурация базы данных"""
    
    def __init__(self):
        # ИСПРАВЛЕНИЕ БАГА: Приоритет DATABASE_URL над отдельными параметрами
        self.DATABASE_URL: str = os.getenv('DATABASE_URL', '')
        
        if self.DATABASE_URL:
            # Используем полный URL если он задан
            print(f"📡 Используется DATABASE_URL для подключения")
        else:
            # Собираем URL из отдельных параметров
            self.DB_HOST = os.getenv('DB_HOST', 'localhost')
            self.DB_PORT = os.getenv('DB_PORT', '5432')
            self.DB_NAME = os.getenv('DB_NAME', 'hr_test')
            self.DB_USER = os.getenv('DB_USER', 'test_user')
            self.DB_PASSWORD = os.getenv('DB_PASSWORD', '')
            
            # Формируем URL подключения
            self.DATABASE_URL = f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
            print(f"📡 Сформирован DATABASE_URL из параметров: {self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}")
        
        # ИСПРАВЛЕНИЕ БАГА: SSL настройки для production
        self.environment = os.getenv('ENVIRONMENT', 'development')
        self.ssl_config = self._get_ssl_config()
        
        # Настройки пула соединений
        self.POOL_SIZE = int(os.getenv('DB_POOL_SIZE', '5'))
        self.MAX_OVERFLOW = int(os.getenv('DB_MAX_OVERFLOW', '10'))
        self.POOL_TIMEOUT = int(os.getenv('DB_POOL_TIMEOUT', '30'))
        self.POOL_RECYCLE = int(os.getenv('DB_POOL_RECYCLE', '3600'))
        self.ECHO = os.getenv('DB_ECHO', 'False').lower() == 'true'
    
    def _get_ssl_config(self) -> dict:
        """Получение SSL конфигурации в зависимости от окружения"""
        
        # Принудительно отключаем SSL для development и localhost
        if (self.environment == 'development' or 
            'localhost' in self.DATABASE_URL or 
            '127.0.0.1' in self.DATABASE_URL):
            print(f"🔓 SSL отключен для development окружения")
            return {'sslmode': 'disable'}
        
        if self.environment == 'production':
            ssl_mode = os.getenv('DB_SSL_MODE', 'require')
            ssl_config = {'sslmode': ssl_mode}
            
            # Дополнительные SSL параметры для production
            if ssl_cert := os.getenv('DB_SSL_CERT'):
                ssl_config['sslcert'] = ssl_cert
            if ssl_key := os.getenv('DB_SSL_KEY'):
                ssl_config['sslkey'] = ssl_key
            if ssl_rootcert := os.getenv('DB_SSL_ROOTCERT'):
                ssl_config['sslrootcert'] = ssl_rootcert
                
            print(f"🔒 SSL включен для production: {ssl_mode}")
            return ssl_config
        else:
            # Для любых других окружений - тоже отключаем SSL
            print(f"🔓 SSL отключен для {self.environment} окружения")
            return {'sslmode': 'disable'}


class Database:
    """Класс для работы с базой данных"""
    
    def __init__(self):
        self.config = DatabaseConfig()
        self.engine = None
        self.SessionLocal = None
        self._setup_database()
    
    def _setup_database(self):
        """Настройка подключения к базе данных"""
        try:
            # Формируем параметры подключения с SSL
            connect_args = self.config.ssl_config if self.config.ssl_config else {}
            
            self.engine = create_engine(
                self.config.DATABASE_URL,
                pool_size=self.config.POOL_SIZE,
                max_overflow=self.config.MAX_OVERFLOW,
                pool_timeout=self.config.POOL_TIMEOUT,
                pool_recycle=self.config.POOL_RECYCLE,
                echo=self.config.ECHO,
                connect_args=connect_args  # ИСПРАВЛЕНИЕ: добавляем SSL конфигурацию
            )
            
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )
            print("✅ Подключение к базе данных настроено успешно")
        except Exception as e:
            print(f"❌ Ошибка при настройке подключения к базе данных: {e}")
            raise
    
    def create_tables(self):
        """Создание всех таблиц в базе данных"""
        try:
            Base.metadata.create_all(bind=self.engine)
            print("✅ Таблицы созданы успешно")
            return True
        except Exception as e:
            print(f"❌ Ошибка при создании таблиц: {e}")
            return False
    
    def drop_tables(self):
        """Удаление всех таблиц из базы данных"""
        try:
            Base.metadata.drop_all(bind=self.engine)
            print("🗑️ Таблицы удалены успешно")
            return True
        except Exception as e:
            print(f"❌ Ошибка при удалении таблиц: {e}")
            return False
    
    def get_session(self):
        """Получение сессии для работы с базой данных"""
        if self.SessionLocal is None:
            raise Exception("База данных не инициализирована")
        return self.SessionLocal()
    
    def close_connection(self):
        """Закрытие соединения с базой данных"""
        if self.engine:
            self.engine.dispose()
            print("🔌 Соединение с базой данных закрыто")
    
    def test_connection(self):
        """Тестирование подключения к базе данных"""
        try:
            session = self.get_session()
            # Простой запрос для проверки соединения (ИСПРАВЛЕНИЕ: используем text())
            session.execute(text("SELECT 1"))
            session.close()
            print("✅ Подключение к базе данных работает корректно")
            return True
        except Exception as e:
            print(f"❌ Ошибка подключения к базе данных: {e}")
            return False


# Глобальный экземпляр базы данных
database = Database()


def get_db():
    """
    Dependency для получения сессии базы данных
    Используется в FastAPI или других фреймворках
    """
    session = database.get_session()
    try:
        yield session
    finally:
        session.close()


def init_database():
    """Инициализация базы данных - создание таблиц"""
    return database.create_tables()


def test_database_connection():
    """Тестирование подключения к базе данных"""
    return database.test_connection()


if __name__ == "__main__":
    print("🔍 Система оценки кандидатов - Инициализация базы данных")
    print("=" * 60)
    
    # Проверяем подключение
    if test_database_connection():
        print("\n🚀 Создание таблиц...")
        if init_database():
            print("\n✅ База данных готова к работе!")
            print("\n📋 Созданные таблицы:")
            print("  • candidates - кандидаты")
            print("  • submissions - заявки")
            print("  • salary_expectations - зарплатные ожидания")
            print("  • addresses - адреса")
            print("  • education - образование")
            print("  • competencies - компетенции")
            print("  • roles - роли")
            print("  • industries - отрасли")
            print("  • locations - локации")
            print("  • связующие таблицы для many-to-many отношений")
        else:
            print("\n❌ Ошибка при создании таблиц")
    else:
        print("\n❌ Не удалось подключиться к базе данных")
        print("📝 Проверьте настройки в файле .env:")
        print("  • DB_HOST")
        print("  • DB_PORT") 
        print("  • DB_NAME")
        print("  • DB_USER")
        print("  • DB_PASSWORD")
