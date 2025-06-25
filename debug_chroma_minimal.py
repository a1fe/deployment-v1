#!/usr/bin/env python3
"""
Простая диагностика ChromaDB без сложных relationships
"""

import sys
sys.path.append('common')

from common.utils.chroma_config import chroma_client, ChromaConfig
from common.database.config import database

# Импортируем только необходимые модели
from sqlalchemy import Column, Integer, String, Text, UUID, TIMESTAMP, Boolean, JSON
from sqlalchemy.orm import declarative_base

# Создаем простую базу без relationships
SimpleBase = declarative_base()

class SimpleEmbeddingMetadata(SimpleBase):
    """Упрощенная модель EmbeddingMetadata с правильными именами полей"""
    __tablename__ = 'embedding_metadata'
    
    embedding_id = Column(UUID(as_uuid=True), primary_key=True)
    source_type = Column(String(50), nullable=False)
    source_id = Column(String(255), nullable=False)
    chroma_document_id = Column(String(255), nullable=False, unique=True)
    collection_name = Column(String(100), nullable=False)
    text_content = Column(Text)
    model_name = Column(String(100), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True))
    updated_at = Column(TIMESTAMP(timezone=True))
    additional_metadata = Column(JSON)

def debug_simple():
    print("=== Простая диагностика ChromaDB ===")
    
    # Проверяем ChromaDB коллекции
    try:
        print(f"ChromaDB health check: {chroma_client.health_check()}")
        
        job_collection = chroma_client.get_job_collection()
        resume_collection = chroma_client.get_resume_collection()
        
        job_count = job_collection.count()
        resume_count = resume_collection.count()
        
        print(f"Коллекция вакансий '{ChromaConfig.JOB_COLLECTION}': {job_count} документов")
        print(f"Коллекция резюме '{ChromaConfig.RESUME_COLLECTION}': {resume_count} документов")
        
    except Exception as e:
        print(f"Ошибка при работе с ChromaDB: {e}")
    
    # Проверяем БД
    print("\n=== Проверка embedding_metadata ===")
    db = database.get_session()
    try:
        # Простой запрос без relationships
        job_records = db.query(SimpleEmbeddingMetadata).filter(
            SimpleEmbeddingMetadata.collection_name == ChromaConfig.JOB_COLLECTION
        ).all()
        
        resume_records = db.query(SimpleEmbeddingMetadata).filter(
            SimpleEmbeddingMetadata.collection_name == ChromaConfig.RESUME_COLLECTION
        ).all()
        
        print(f"Записей о вакансиях в БД: {len(job_records)}")
        print(f"Записей о резюме в БД: {len(resume_records)}")
        
        if job_records:
            print("\nПример записи о вакансии:")
            job = job_records[0]
            print(f"  Source ID: {job.source_id}")
            print(f"  Chroma ID: {job.chroma_document_id}")
            print(f"  Model: {job.model_name}")
            print(f"  Text preview: {(job.text_content or '')[:100]}...")
            
        if resume_records:
            print("\nПример записи о резюме:")
            resume = resume_records[0]
            print(f"  Source ID: {resume.source_id}")
            print(f"  Chroma ID: {resume.chroma_document_id}")
            print(f"  Model: {resume.model_name}")
            print(f"  Text preview: {(resume.text_content or '')[:100]}...")
            
        # Проверим, есть ли эти документы в ChromaDB
        print("\n=== Проверка синхронизации ===")
        if job_records:
            first_job_chroma_id = job_records[0].chroma_document_id
            try:
                job_collection = chroma_client.get_job_collection()
                found = job_collection.get(ids=[first_job_chroma_id])
                if found['ids']:
                    print(f"✅ Вакансия {first_job_chroma_id} найдена в ChromaDB")
                else:
                    print(f"❌ Вакансия {first_job_chroma_id} НЕ найдена в ChromaDB")
            except Exception as e:
                print(f"❌ Ошибка при поиске вакансии в ChromaDB: {e}")
                
        if resume_records:
            first_resume_chroma_id = resume_records[0].chroma_document_id
            try:
                resume_collection = chroma_client.get_resume_collection()
                found = resume_collection.get(ids=[first_resume_chroma_id])
                if found['ids']:
                    print(f"✅ Резюме {first_resume_chroma_id} найдено в ChromaDB")
                else:
                    print(f"❌ Резюме {first_resume_chroma_id} НЕ найдено в ChromaDB")
            except Exception as e:
                print(f"❌ Ошибка при поиске резюме в ChromaDB: {e}")
        
    except Exception as e:
        print(f"Ошибка при работе с БД: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    debug_simple()
