"""
Миграция для добавления индекса полнотекстового поиска для компаний
"""

from sqlalchemy import text
from database.config import database


def create_fulltext_search_index():
    """Создание индекса полнотекстового поиска для таблицы companies"""
    
    db = database.get_session()
    
    try:
        print("📊 Создание индекса полнотекстового поиска...")
        
        # Создаем индекс GIN для полнотекстового поиска
        create_index_sql = text("""
            CREATE INDEX IF NOT EXISTS idx_companies_fulltext_search 
            ON companies 
            USING GIN (to_tsvector('english', name || ' ' || COALESCE(description, '')))
        """)
        
        db.execute(create_index_sql)
        db.commit()
        
        print("✅ Индекс полнотекстового поиска создан успешно")
        
        # Проверяем, что индекс создался
        check_index_sql = text("""
            SELECT indexname FROM pg_indexes 
            WHERE tablename = 'companies' AND indexname = 'idx_companies_fulltext_search'
        """)
        
        result = db.execute(check_index_sql)
        if result.fetchone():
            print("✅ Индекс подтвержден в базе данных")
        else:
            print("⚠️ Индекс не найден после создания")
            
    except Exception as e:
        print(f"❌ Ошибка при создании индекса: {e}")
        db.rollback()
    finally:
        db.close()


def drop_fulltext_search_index():
    """Удаление индекса полнотекстового поиска"""
    
    db = database.get_session()
    
    try:
        print("🗑️ Удаление индекса полнотекстового поиска...")
        
        drop_index_sql = text("DROP INDEX IF EXISTS idx_companies_fulltext_search")
        db.execute(drop_index_sql)
        db.commit()
        
        print("✅ Индекс удален успешно")
        
    except Exception as e:
        print(f"❌ Ошибка при удалении индекса: {e}")
        db.rollback()
    finally:
        db.close()


if __name__ == "__main__":
    create_fulltext_search_index()
