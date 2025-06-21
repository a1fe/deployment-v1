"""
Скрипт для инициализации базовых данных компаний
"""

import sys
import os
sys.path.append('../..')
from database.config import database
from .helpers import create_base_competencies, create_base_industries, create_default_hiring_stages


def initialize_company_base_data():
    """Инициализация базовых данных для компаний"""
    print("🏢 Инициализация базовых данных компаний")
    print("=" * 50)
    
    db = database.get_session()
    
    try:
        create_base_industries(db)
        create_base_competencies(db)
        create_default_hiring_stages(db)
        
        print("\n✅ Инициализация данных компаний завершена успешно!")
        
    except Exception as e:
        print(f"❌ Ошибка при инициализации данных компаний: {e}")
        db.rollback()
    finally:
        db.close()


if __name__ == "__main__":
    initialize_company_base_data()
