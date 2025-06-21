"""
Скрипт для инициализации базовых справочников системы
"""

import sys
import os
sys.path.append('../..')
from database.config import database
from .helpers import create_base_competencies, create_base_industries


def initialize_base_dictionaries():
    """Инициализация всех базовых справочников"""
    print("📊 Инициализация базовых справочников системы")
    print("=" * 50)
    
    db = database.get_session()
    
    try:
        create_base_competencies(db)
        create_base_industries(db)
        
        print("\n✅ Инициализация справочников завершена успешно!")
        
    except Exception as e:
        print(f"❌ Ошибка при инициализации справочников: {e}")
        db.rollback()
    finally:
        db.close()


if __name__ == "__main__":
    initialize_base_dictionaries()
