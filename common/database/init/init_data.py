#!/usr/bin/env python3
"""
Инициализация базовых справочников
"""

import os
import sys

# Добавляем пути к модулям
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def initialize_base_dictionaries():
    """Инициализация базовых справочников"""
    try:
        from database.config import database
        
        # Проверяем, есть ли модели справочников
        try:
            from models.dictionaries import Industry, Competency, Role, Location
            models_available = True
        except ImportError:
            print("⚠️ Модели справочников не найдены, пропускаем инициализацию")
            return True
        
        with database.get_session() as db:
            # Проверяем, есть ли уже данные
            try:
                if db.query(Industry).count() > 0:
                    print("📊 Справочники уже инициализированы")
                    return True
            except Exception:
                print("⚠️ Таблицы справочников не созданы, пропускаем инициализацию")
                return True
            
            print("📊 Инициализация базовых справочников...")
            
            # Отрасли
            industries = [
                {"name": "Technology", "is_primary": True},
                {"name": "Finance", "is_primary": True},
                {"name": "Healthcare", "is_primary": True},
                {"name": "Education", "is_primary": True},
                {"name": "Retail", "is_primary": True},
                {"name": "Manufacturing", "is_primary": True},
                {"name": "Consulting", "is_primary": True},
                {"name": "Media & Entertainment", "is_primary": True},
                {"name": "Government", "is_primary": True},
                {"name": "Non-profit", "is_primary": True}
            ]
            
            for industry_data in industries:
                industry = Industry(**industry_data)
                db.add(industry)
            
            # Компетенции
            competencies = [
                {"name": "Python", "is_primary": True},
                {"name": "JavaScript", "is_primary": True},
                {"name": "Java", "is_primary": True},
                {"name": "React", "is_primary": True},
                {"name": "Django", "is_primary": True},
                {"name": "PostgreSQL", "is_primary": True},
                {"name": "Docker", "is_primary": True},
                {"name": "AWS", "is_primary": True},
                {"name": "Machine Learning", "is_primary": True},
                {"name": "Data Analysis", "is_primary": True},
                {"name": "Project Management", "is_primary": True},
                {"name": "Agile", "is_primary": True}
            ]
            
            for comp_data in competencies:
                comp = Competency(**comp_data)
                db.add(comp)
            
            # Роли
            roles = [
                {"name": "Software Engineer"},
                {"name": "Frontend Developer"},
                {"name": "Backend Developer"},
                {"name": "Full Stack Developer"},
                {"name": "DevOps Engineer"},
                {"name": "Data Scientist"},
                {"name": "Data Engineer"},
                {"name": "Product Manager"},
                {"name": "Project Manager"},
                {"name": "QA Engineer"}
            ]
            
            for role_data in roles:
                role = Role(**role_data)
                db.add(role)
            
            # Локации
            locations = [
                {"name": "Remote"},
                {"name": "Moscow"},
                {"name": "St. Petersburg"},
                {"name": "Kiev"},
                {"name": "Minsk"},
                {"name": "London"},
                {"name": "Berlin"},
                {"name": "New York"}
            ]
            
            for loc_data in locations:
                location = Location(**loc_data)
                db.add(location)
            
            db.commit()
            print(f"✅ Инициализированы справочники:")
            print(f"   - Отрасли: {len(industries)}")
            print(f"   - Компетенции: {len(competencies)}")
            print(f"   - Роли: {len(roles)}")
            print(f"   - Локации: {len(locations)}")
            
            return True
            
    except Exception as e:
        print(f"❌ Ошибка при инициализации справочников: {e}")
        return False

if __name__ == "__main__":
    success = initialize_base_dictionaries()
    sys.exit(0 if success else 1)
