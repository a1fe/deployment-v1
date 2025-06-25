#!/usr/bin/env python3
"""
Скрипт для инициализации базы данных PostgreSQL
Создает пользователя, базу данных и таблицы при первом запуске
"""

import os
import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

# Определяем пути к файлам проекта
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
COMMON_DIR = os.path.join(SCRIPT_DIR, '..', '..')
ROOT_DIR = os.path.join(COMMON_DIR, '..')

# Добавляем пути к модулям
sys.path.insert(0, COMMON_DIR)
sys.path.insert(0, ROOT_DIR)

# Ищем .env файл в нескольких возможных местах
env_paths = [
    os.path.join(COMMON_DIR, '.env'),
    os.path.join(ROOT_DIR, '.env'),
    os.path.join(os.getcwd(), '.env')
]

env_loaded = False
for env_path in env_paths:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"📄 Загружен файл окружения: {env_path}")
        env_loaded = True
        break

if not env_loaded:
    print("⚠️  Файл .env не найден, используются переменные системы")

def get_db_params():
    """Получение параметров базы данных из переменных окружения"""
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', '5432')),
        'user': os.getenv('DB_USER', 'test_user'),
        'password': os.getenv('DB_PASSWORD', ''),
        'database': os.getenv('DB_NAME', 'hr_test'),
        'superuser': os.getenv('DB_SUPERUSER', 'postgres'),
        'superuser_password': os.getenv('DB_SUPERUSER_PASSWORD', '')
    }

def check_postgres_connection(params):
    """Проверка подключения к PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=params['host'],
            port=params['port'],
            user=params['superuser'],
            password=params['superuser_password'],
            database='postgres'
        )
        conn.close()
        print("✅ Подключение к PostgreSQL установлено")
        return True
    except Exception as e:
        print(f"❌ Ошибка подключения к PostgreSQL: {e}")
        return False

def user_exists(cursor, username):
    """Проверка существования пользователя"""
    cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s;", (username,))
    return cursor.fetchone() is not None

def database_exists(cursor, database_name):
    """Проверка существования базы данных"""
    cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (database_name,))
    return cursor.fetchone() is not None

def create_user_and_database(params):
    """Создание пользователя и базы данных"""
    try:
        # Подключаемся как суперпользователь к системной БД postgres
        conn = psycopg2.connect(
            host=params['host'],
            port=params['port'],
            user=params['superuser'],
            password=params['superuser_password'],
            database='postgres'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Проверяем и создаем пользователя
        if not user_exists(cursor, params['user']):
            print(f"👤 Создание пользователя {params['user']}...")
            cursor.execute(
                f"CREATE USER {params['user']} WITH PASSWORD %s;",
                (params['password'],)
            )
            cursor.execute(
                f"ALTER USER {params['user']} CREATEDB;"
            )
            print(f"✅ Пользователь {params['user']} создан")
        else:
            print(f"👤 Пользователь {params['user']} уже существует")
        
        # Проверяем и создаем базу данных
        if not database_exists(cursor, params['database']):
            print(f"🗄️ Создание базы данных {params['database']}...")
            cursor.execute(
                f"CREATE DATABASE {params['database']} OWNER {params['user']};"
            )
            print(f"✅ База данных {params['database']} создана")
        else:
            print(f"🗄️ База данных {params['database']} уже существует")
        
        # Предоставляем права
        cursor.execute(
            f"GRANT ALL PRIVILEGES ON DATABASE {params['database']} TO {params['user']};"
        )
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при создании пользователя/БД: {e}")
        return False

def check_user_database_connection(params):
    """Проверка подключения пользователя к созданной БД"""
    try:
        conn = psycopg2.connect(
            host=params['host'],
            port=params['port'],
            user=params['user'],
            password=params['password'],
            database=params['database']
        )
        conn.close()
        print(f"✅ Пользователь {params['user']} может подключиться к БД {params['database']}")
        return True
    except Exception as e:
        print(f"❌ Ошибка подключения пользователя к БД: {e}")
        return False

def create_tables():
    """Создание таблиц через SQLAlchemy"""
    try:
        from common.database.config import database
        if database.create_tables():
            print("✅ Таблицы созданы успешно")
            
            # Выполняем миграции после создания таблиц
            if apply_migrations():
                print("✅ Миграции применены успешно")
            else:
                print("⚠️ Некоторые миграции не применились, но это может быть нормально")
            
            return True
        else:
            print("❌ Ошибка при создании таблиц")
            return False
    except Exception as e:
        print(f"❌ Ошибка при создании таблиц: {e}")
        return False


def apply_migrations():
    """Применение миграций к базе данных"""
    try:
        from common.database.config import database
        from sqlalchemy import text
        db = database.get_session()
        
        print("🔧 Применение миграций...")
        
        try:
            # Миграция 1: Увеличение размера полей в таблице education
            print("📝 Миграция 1: Обновление таблицы education...")
            db.execute(text("""
                DO $$ 
                BEGIN
                    -- Проверяем, нужна ли миграция
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'education' 
                        AND column_name = 'field_of_study' 
                        AND character_maximum_length = 100
                    ) THEN
                        -- Увеличиваем размер полей
                        ALTER TABLE education 
                        ALTER COLUMN degree_level TYPE VARCHAR(255),
                        ALTER COLUMN field_of_study TYPE VARCHAR(500);
                        
                        RAISE NOTICE 'Миграция education: размеры полей обновлены';
                    ELSE
                        RAISE NOTICE 'Миграция education: не требуется или уже применена';
                    END IF;
                END $$;
            """))
            
            # Миграция 2: Создание таблицы education_fields (если не существует)
            print("📝 Миграция 2: Создание таблицы education_fields...")
            db.execute(text("""
                CREATE TABLE IF NOT EXISTS education_fields (
                    field_id SERIAL PRIMARY KEY,
                    education_id INTEGER NOT NULL,
                    field_name VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (education_id) REFERENCES education(education_id) ON DELETE CASCADE
                );
            """))
            
            # Создаем индексы отдельно
            db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_education_fields_education_id 
                ON education_fields(education_id);
            """))
            
            db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_education_fields_field_name 
                ON education_fields(field_name);
            """))
            
            # Миграция 3: Обновление других таблиц при необходимости
            print("📝 Миграция 3: Проверка и обновление других таблиц...")
            db.execute(text("""
                DO $$ 
                BEGIN
                    -- Проверяем таблицу submissions и увеличиваем размеры полей при необходимости
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'submissions' 
                        AND column_name = 'work_preference' 
                        AND character_maximum_length < 100
                    ) THEN
                        ALTER TABLE submissions 
                        ALTER COLUMN work_preference TYPE VARCHAR(100),
                        ALTER COLUMN willing_to_relocate TYPE VARCHAR(100);
                        
                        RAISE NOTICE 'Миграция submissions: размеры полей обновлены';
                    END IF;
                    
                    -- Проверяем таблицу candidates
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'candidates' 
                        AND column_name = 'current_position' 
                        AND character_maximum_length < 255
                    ) THEN
                        ALTER TABLE candidates 
                        ALTER COLUMN current_position TYPE VARCHAR(255),
                        ALTER COLUMN current_company TYPE VARCHAR(255);
                        
                        RAISE NOTICE 'Миграция candidates: размеры полей обновлены';
                    END IF;
                END $$;
            """))
            
            db.commit()
            print("✅ Все миграции применены успешно")
            return True
            
        finally:
            db.close()
            
    except Exception as e:
        print(f"❌ Ошибка при применении миграций: {e}")
        try:
            db.rollback()
        except:
            pass
        return False

def initialize_dictionaries():
    """Инициализация базовых справочников"""
    try:
        from common.database.init.init_data import initialize_base_dictionaries
        initialize_base_dictionaries()
        return True
    except Exception as e:
        print(f"❌ Ошибка при инициализации справочников: {e}")
        return False

def main():
    """Главная функция инициализации"""
    print("🔍 Инициализация базы данных HR Analysis")
    print("=" * 50)
    
    params = get_db_params()
    
    # Проверяем параметры
    if not params['password']:
        print("❌ Не указан пароль для пользователя БД (DB_PASSWORD)")
        return False
    
    # Шаг 1: Проверяем подключение к PostgreSQL
    if not check_postgres_connection(params):
        print("💡 Убедитесь, что PostgreSQL запущен и доступен")
        return False
    
    # Шаг 2: Создаем пользователя и БД
    if not create_user_and_database(params):
        return False
    
    # Шаг 3: Проверяем подключение пользователя
    if not check_user_database_connection(params):
        return False
    
    # Шаг 4: Создаем таблицы
    print("\n📋 Создание таблиц...")
    if not create_tables():
        return False
    
    # Шаг 5: Инициализируем справочники
    print("\n📊 Инициализация базовых справочников...")
    if not initialize_dictionaries():
        print("⚠️ Справочники не инициализированы, но это не критично")
    
    print("\n✅ Инициализация базы данных завершена успешно!")
    print(f"🗄️ База данных: {params['database']}")
    print(f"👤 Пользователь: {params['user']}")
    print(f"🌐 Хост: {params['host']}:{params['port']}")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
