"""
Миграция для создания таблиц анализа BGE Reranker

Создание таблиц для хранения результатов анализа сравнения резюме и вакансий
"""

from sqlalchemy import text
from database.config import database


def create_reranker_analysis_tables():
    """Создание таблиц для анализа BGE Reranker"""
    
    db = database.get_session()
    
    try:
        print("📊 Создание таблиц анализа BGE Reranker...")
        
        # Создаем таблицу сессий анализа
        create_sessions_table_sql = text("""
            CREATE TABLE IF NOT EXISTS reranker_analysis_sessions (
                session_id SERIAL PRIMARY KEY,
                session_uuid UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
                job_id INTEGER NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
                company_id INTEGER NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
                total_results INTEGER NOT NULL DEFAULT 0,
                search_params JSONB NOT NULL,
                reranker_model VARCHAR(100) NOT NULL,
                session_stats JSONB NOT NULL,
                started_at TIMESTAMP WITH TIME ZONE NOT NULL,
                completed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        db.execute(create_sessions_table_sql)
        print("✅ Таблица reranker_analysis_sessions создана")
        
        # Создаем таблицу результатов анализа
        create_results_table_sql = text("""
            CREATE TABLE IF NOT EXISTS reranker_analysis_results (
                analysis_id SERIAL PRIMARY KEY,
                job_id INTEGER NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
                submission_id UUID NOT NULL REFERENCES submissions(submission_id) ON DELETE CASCADE,
                original_similarity DECIMAL(10, 6) NOT NULL,
                rerank_score DECIMAL(10, 6) NOT NULL,
                final_score DECIMAL(10, 6) NOT NULL,
                score_improvement DECIMAL(10, 6) NOT NULL,
                rank_position INTEGER NOT NULL,
                search_params JSONB NOT NULL,
                reranker_model VARCHAR(100) NOT NULL,
                workflow_stats JSONB NOT NULL,
                job_title VARCHAR(255) NOT NULL,
                company_id INTEGER NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
                candidate_name VARCHAR(255) NOT NULL,
                candidate_email VARCHAR(255) NOT NULL,
                total_candidates_found INTEGER NOT NULL,
                analysis_type VARCHAR(50) NOT NULL DEFAULT 'enhanced_resume_search',
                processed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                quality_metrics JSONB,
                
                -- Constraints
                CONSTRAINT ck_original_similarity_range CHECK (original_similarity >= 0 AND original_similarity <= 1),
                CONSTRAINT ck_rank_position_positive CHECK (rank_position > 0),
                CONSTRAINT ck_total_candidates_positive CHECK (total_candidates_found >= 0),
                CONSTRAINT ck_final_score_positive CHECK (final_score >= 0),
                CONSTRAINT uq_analysis_job_submission_time UNIQUE (job_id, submission_id, processed_at)
            )
        """)
        
        db.execute(create_results_table_sql)
        print("✅ Таблица reranker_analysis_results создана")
        
        # Создаем индексы для сессий
        create_sessions_indexes_sql = text("""
            CREATE INDEX IF NOT EXISTS idx_analysis_session_job_id ON reranker_analysis_sessions(job_id);
            CREATE INDEX IF NOT EXISTS idx_analysis_session_uuid ON reranker_analysis_sessions(session_uuid);
            CREATE INDEX IF NOT EXISTS idx_analysis_session_completed ON reranker_analysis_sessions(completed_at);
            CREATE INDEX IF NOT EXISTS idx_analysis_session_job_completed ON reranker_analysis_sessions(job_id, completed_at);
        """)
        
        db.execute(create_sessions_indexes_sql)
        print("✅ Индексы для сессий анализа созданы")
        
        # Создаем индексы для результатов
        create_results_indexes_sql = text("""
            CREATE INDEX IF NOT EXISTS idx_reranker_analysis_job_id ON reranker_analysis_results(job_id);
            CREATE INDEX IF NOT EXISTS idx_reranker_analysis_submission_id ON reranker_analysis_results(submission_id);
            CREATE INDEX IF NOT EXISTS idx_reranker_analysis_company_id ON reranker_analysis_results(company_id);
            CREATE INDEX IF NOT EXISTS idx_reranker_analysis_processed_at ON reranker_analysis_results(processed_at);
            CREATE INDEX IF NOT EXISTS idx_reranker_analysis_rerank_score ON reranker_analysis_results(rerank_score);
            CREATE INDEX IF NOT EXISTS idx_reranker_analysis_rank_position ON reranker_analysis_results(rank_position);
            CREATE INDEX IF NOT EXISTS idx_reranker_analysis_job_processed ON reranker_analysis_results(job_id, processed_at);
            CREATE INDEX IF NOT EXISTS idx_reranker_analysis_quality ON reranker_analysis_results(rerank_score, final_score, rank_position);
        """)
        
        db.execute(create_results_indexes_sql)
        print("✅ Индексы для результатов анализа созданы")
        
        db.commit()
        print("✅ Все таблицы анализа BGE Reranker созданы успешно")
        
        # Проверяем, что таблицы созданы
        check_tables_sql = text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('reranker_analysis_sessions', 'reranker_analysis_results')
            ORDER BY table_name
        """)
        
        result = db.execute(check_tables_sql)
        tables = [row[0] for row in result.fetchall()]
        
        print(f"📋 Созданные таблицы: {', '.join(tables)}")
        
        if len(tables) == 2:
            print("🎉 Миграция анализа BGE Reranker выполнена успешно!")
            return True
        else:
            print(f"⚠️ Предупреждение: ожидалось 2 таблицы, создано {len(tables)}")
            return False
            
    except Exception as e:
        print(f"❌ Ошибка при создании таблиц анализа: {e}")
        db.rollback()
        raise
    finally:
        db.close()


def drop_reranker_analysis_tables():
    """Удаление таблиц анализа BGE Reranker (для отката миграции)"""
    
    db = database.get_session()
    
    try:
        print("🗑️ Удаление таблиц анализа BGE Reranker...")
        
        # Удаляем таблицы в правильном порядке (сначала зависимые)
        drop_tables_sql = text("""
            DROP TABLE IF EXISTS reranker_analysis_results CASCADE;
            DROP TABLE IF EXISTS reranker_analysis_sessions CASCADE;
        """)
        
        db.execute(drop_tables_sql)
        db.commit()
        
        print("✅ Таблицы анализа BGE Reranker удалены успешно")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при удалении таблиц анализа: {e}")
        db.rollback()
        raise
    finally:
        db.close()


def run_migration():
    """Запуск миграции"""
    print("🚀 Запуск миграции анализа BGE Reranker")
    print("=" * 50)
    
    return create_reranker_analysis_tables()


def rollback_migration():
    """Откат миграции"""
    print("🔄 Откат миграции анализа BGE Reranker")
    print("=" * 50)
    
    return drop_reranker_analysis_tables()


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'rollback':
        rollback_migration()
    else:
        run_migration()
