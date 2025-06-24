#!/usr/bin/env python3
"""
Простая проверка подключения к базе данных
"""

import os
import sys
from dotenv import load_dotenv

# Добавляем путь к модулям
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

# Загружаем переменные окружения
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

def main():
    """Проверка подключения к БД"""
    try:
        from database.config import database
        
        print("🔍 Проверка подключения к базе данных...")
        
        if database.test_connection():
            print("✅ Подключение к базе данных работает!")
            return True
        else:
            print("❌ Ошибка подключения к базе данных")
            return False
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
