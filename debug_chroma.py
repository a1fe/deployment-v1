#!/usr/bin/env python3

import sys
sys.path.append('common')

from common.utils.chroma_config import chroma_client, ChromaConfig
from common.database.config import database

# Импортируем все модели, чтобы SQLAlchemy мог разрешить relationships
from common.models.base import Base
from common.models.dictionaries import Industry, Competency, Role, Location
from common.models.companies import Company, Job, JobCompetency, CompanyIndustry
from common.models.embeddings import EmbeddingMetadata

def debug_chroma():
    print("=== Проверка ChromaDB ===")
    
    # Проверяем соединение
    print(f"ChromaDB health check: {chroma_client.health_check()}")
    
    # Получаем все коллекции
    try:
        collections = chroma_client.client.list_collections()
        print(f"Доступные коллекции: {[c.name for c in collections]}")
    except Exception as e:
        print(f"Ошибка при получении коллекций: {e}")
    
    # Проверяем коллекцию вакансий
    try:
        job_collection = chroma_client.get_job_collection()
        job_count = job_collection.count()
        print(f"Коллекция вакансий '{ChromaConfig.JOB_COLLECTION}': {job_count} документов")
        
        if job_count > 0:
            # Получаем первые несколько документов
            sample = job_collection.get(limit=3)
            print(f"Пример документов в коллекции вакансий:")
            for i, (doc_id, metadata) in enumerate(zip(sample['ids'], sample['metadatas'])):
                print(f"  {i+1}. ID: {doc_id}, Metadata: {metadata}")
    except Exception as e:
        print(f"Ошибка при работе с коллекцией вакансий: {e}")
    
    # Проверяем коллекцию резюме
    try:
        resume_collection = chroma_client.get_resume_collection()
        resume_count = resume_collection.count()
        print(f"Коллекция резюме '{ChromaConfig.RESUME_COLLECTION}': {resume_count} документов")
    except Exception as e:
        print(f"Ошибка при работе с коллекцией резюме: {e}")
    
    print("\n=== Проверка таблицы embedding_metadata ===")
    
    # Проверяем данные в таблице embedding_metadata
    db = database.get_session()
    try:
        job_embeddings = db.query(EmbeddingMetadata).filter(
            EmbeddingMetadata.source_type == 'job_description'
        ).all()
        
        print(f"Записей о вакансиях в embedding_metadata: {len(job_embeddings)}")
        
        if job_embeddings:
            print("Примеры записей о вакансиях:")
            for i, embedding in enumerate(job_embeddings[:3]):
                print(f"  {i+1}. Source ID: {embedding.source_id}")
                print(f"      Collection: {embedding.collection_name}")
                print(f"      Chroma ID: {embedding.chroma_document_id}")
                print(f"      Model: {embedding.model_name}")
                print(f"      Text preview: {embedding.text_content[:100]}...")
                print()
        
        resume_embeddings = db.query(EmbeddingMetadata).filter(
            EmbeddingMetadata.source_type == 'resume'
        ).all()
        
        print(f"Записей о резюме в embedding_metadata: {len(resume_embeddings)}")
        
    except Exception as e:
        print(f"Ошибка при работе с таблицей embedding_metadata: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    debug_chroma()
