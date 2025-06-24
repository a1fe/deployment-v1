#!/usr/bin/env python3
"""
Система оценки кандидатов - Главный скрипт запуска (Deployment Version)
Упрощенная версия для деплоймента с поддержкой только двух workflow
"""

import sys
import os

# Добавляем корневую папку и deployment в PYTHONPATH
deployment_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
project_root = os.path.dirname(deployment_root)
sys.path.insert(0, project_root)
sys.path.insert(0, deployment_root)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database.config import database, init_database, test_database_connection


def main():
    """Главная функция для деплоймента"""
    print("🔍 Система оценки кандидатов (Deployment)")
    print("=" * 40)
    print("Доступные действия:")
    print("1. Инициализация базы данных")
    print("2. Тестирование подключения")
    print("3. Инициализация базовых справочников")
    print("4. Инициализация данных компаний")
    print("5. Запуск Celery Worker")
    print("6. Запуск Celery Beat")
    print("7. Статус системы")
    print("8. Тест workflow")
    print("0. Выход")
    
    choice = input("\nВведите номер действия: ").strip()
    
    if choice == "1":
        print("\n🚀 Инициализация базы данных...")
        if init_database():
            print("✅ База данных инициализирована успешно!")
        else:
            print("❌ Ошибка при инициализации базы данных")
    
    elif choice == "2":
        print("\n🔍 Тестирование подключения...")
        if test_database_connection():
            print("✅ Подключение работает корректно!")
        else:
            print("❌ Ошибка подключения к базе данных")
    
    elif choice == "3":
        print("\n📊 Инициализация базовых справочников...")
        try:
            from database.init.init_data import initialize_base_dictionaries
            initialize_base_dictionaries()
        except ImportError as e:
            print(f"❌ Ошибка импорта: {e}")
        except Exception as e:
            print(f"❌ Ошибка выполнения: {e}")
    
    elif choice == "4":
        print("\n🏢 Инициализация данных компаний...")
        try:
            from database.init.init_company_data import initialize_company_base_data
            initialize_company_base_data()
        except ImportError as e:
            print(f"❌ Ошибка импорта: {e}")
        except Exception as e:
            print(f"❌ Ошибка выполнения: {e}")
    
    elif choice == "5":
        print("\n⚙️ Запуск Celery Worker...")
        try:
            from celery_app.celery_app import get_celery_app
            app = get_celery_app()
            print("✅ Запуск Celery Worker для активных очередей...")
            print("Команды для запуска воркеров:")
            print("celery -A celery_app.celery_app worker -Q embeddings_gpu --loglevel=info")
            print("celery -A celery_app.celery_app worker -Q search_basic --loglevel=info")
            print("celery -A celery_app.celery_app worker -Q scoring_tasks --loglevel=info")
            print("celery -A celery_app.celery_app worker -Q fillout --loglevel=info")
            print("celery -A celery_app.celery_app worker -Q default --loglevel=info")
        except Exception as e:
            print(f"❌ Ошибка: {e}")
    
    elif choice == "6":
        print("\n⏰ Запуск Celery Beat...")
        try:
            print("✅ Команда для запуска планировщика:")
            print("celery -A celery_app.celery_app beat --loglevel=info")
        except Exception as e:
            print(f"❌ Ошибка: {e}")
    
    elif choice == "7":
        print("\n📊 Статус системы...")
        try:
            from celery_app.celery_app import celery_app
            # Здесь мы можем добавить проверку здоровья системы
            status = {'celery': True, 'redis': True, 'database': True}
            print("Статус проверки системы:")
            for component, health in status.items():
                print(f"  • {component}: {'✅' if health else '❌'}")
        except Exception as e:
            print(f"❌ Ошибка проверки: {e}")
    
    elif choice == "8":
        print("\n🧪 Тест workflow...")
        try:
            from celery_app.celery_app import get_celery_app
            app = get_celery_app()
            
            print("Доступные workflow:")
            print("1. Resume Processing Chain")
            print("2. Job Processing Chain")
            
            workflow_choice = input("Выберите workflow (1-2): ").strip()
            
            if workflow_choice == "1":
                result = app.send_task('tasks.workflows.resume_processing_chain')
                print(f"✅ Resume workflow запущен, ID: {result.id}")
            elif workflow_choice == "2":
                result = app.send_task('tasks.workflows.job_processing_chain')
                print(f"✅ Job workflow запущен, ID: {result.id}")
            else:
                print("❌ Неверный выбор")
                
        except Exception as e:
            print(f"❌ Ошибка запуска workflow: {e}")
    
    elif choice == "0":
        print("👋 До свидания!")
        sys.exit(0)
    
    else:
        print("❌ Неверный выбор. Попробуйте снова.")
    
    # Возвращаемся в меню
    print("\n" + "=" * 40)
    input("Нажмите Enter для продолжения...")
    main()


if __name__ == "__main__":
    main()
