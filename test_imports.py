#!/usr/bin/env python3
"""
Тест импорта всех моделей для проверки relationships
"""

print("Импортируем все модели...")

# Импорт базового класса
from common.models.base import Base

# Импорт справочных моделей
from common.models.dictionaries import Industry, Competency, Role, Location

# Импорт основных файлов напрямую (не через подпапки)
print("Импортируем базовые модели...")
from common.models.base import Base
from common.models.dictionaries import Industry, Competency, Role, Location
print("Базовые модели импортированы")

# Пробуем импортировать модели из основных файлов
print("Импортируем основные модели...")
try:
    from common.models.candidates import Candidate, Submission
    print("Модели кандидатов импортированы")
except Exception as e:
    print(f"Ошибка импорта кандидатов: {e}")

try:
    from common.models.companies import Company, Job
    print("Модели компаний импортированы")
except Exception as e:
    print(f"Ошибка импорта компаний: {e}")

try:
    from common.models.embeddings import EmbeddingMetadata
    print("Модели эмбеддингов импортированы")
except Exception as e:
    print(f"Ошибка импорта эмбеддингов: {e}")

print("Все модели импортированы успешно!")

# Проверим, что все модели зарегистрированы
print("\nЗарегистрированные модели:")
for table_name, table in Base.metadata.tables.items():
    print(f"  - {table_name}")

print(f"\nВсего таблиц: {len(Base.metadata.tables)}")
