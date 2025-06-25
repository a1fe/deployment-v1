#!/usr/bin/env python3

import sys
sys.path.append('common')

from common.utils.chroma_config import chroma_client, ChromaConfig
from common.database.config import database

# Импортируем только базовые модели без relationships
from common.models.base import Base
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
            # Получаем первые несколько документов для примера
            sample_docs = job_collection.get(limit=3, include=['documents', 'metadatas'])
            print("Примеры документов из коллекции вакансий:")
            for i, (doc, meta) in enumerate(zip(sample_docs['documents'], sample_docs['metadatas'])):
                print(f"  {i+1}. ID: {sample_docs['ids'][i]}")
                print(f"      Metadata: {meta}")
                print(f"      Text preview: {doc[:100]}...")
    except Exception as e:
        print(f"Ошибка при работе с коллекцией вакансий: {e}")
    
    # Проверяем коллекцию резюме
    try:
        resume_collection = chroma_client.get_resume_collection()
        resume_count = resume_collection.count()
        print(f"Коллекция резюме '{ChromaConfig.RESUME_COLLECTION}': {resume_count} документов")
        
        if resume_count > 0:
            # Получаем первые несколько документов для примера
            sample_docs = resume_collection.get(limit=3, include=['documents', 'metadatas'])
            print("Примеры документов из коллекции резюме:")
            for i, (doc, meta) in enumerate(zip(sample_docs['documents'], sample_docs['metadatas'])):
                print(f"  {i+1}. ID: {sample_docs['ids'][i]}")
                print(f"      Metadata: {meta}")
                print(f"      Text preview: {doc[:100]}...")
    except Exception as e:
        print(f"Ошибка при работе с коллекцией резюме: {e}")

def debug_embedding_metadata():
    print("\n=== Проверка таблицы embedding_metadata ===")
    
    db = database.get_session()
    try:
        # Проверяем записи о вакансиях
        job_embeddings = db.query(EmbeddingMetadata).filter(
            EmbeddingMetadata.collection_name == ChromaConfig.JOB_COLLECTION
        ).all()
        
        print(f"Записей о вакансиях в embedding_metadata: {len(job_embeddings)}")
        if job_embeddings:
            print("Примеры записей о вакансиях:")
            for i, embed in enumerate(job_embeddings[:3], 1):
                print(f"  {i}. Source ID: {embed.source_id}")
                print(f"      Collection: {embed.collection_name}")
                print(f"      Chroma ID: {embed.chroma_id}")
                print(f"      Model: {embed.model_version}")
                print(f"      Text preview: {embed.text_content[:100] if embed.text_content else 'No text'}...")
                print()
        
        # Проверяем записи о резюме
        resume_embeddings = db.query(EmbeddingMetadata).filter(
            EmbeddingMetadata.collection_name == ChromaConfig.RESUME_COLLECTION
        ).all()
        
        print(f"Записей о резюме в embedding_metadata: {len(resume_embeddings)}")
        if resume_embeddings:
            print("Примеры записей о резюме:")
            for i, embed in enumerate(resume_embeddings[:3], 1):
                print(f"  {i}. Source ID: {embed.source_id}")
                print(f"      Collection: {embed.collection_name}")
                print(f"      Chroma ID: {embed.chroma_id}")
                print(f"      Model: {embed.model_version}")
                print(f"      Text preview: {embed.text_content[:100] if embed.text_content else 'No text'}...")
                print()
                
    except Exception as e:
        print(f"Ошибка при работе с таблицей embedding_metadata: {e}")
    finally:
        db.close()

def check_sync_issue():
    print("\n=== Диагностика проблемы синхронизации ===")
    
    db = database.get_session()
    try:
        # Получаем все записи из embedding_metadata
        all_embeddings = db.query(EmbeddingMetadata).all()
        
        print(f"Общее количество записей в embedding_metadata: {len(all_embeddings)}")
        
        job_records = [e for e in all_embeddings if e.collection_name == ChromaConfig.JOB_COLLECTION]
        resume_records = [e for e in all_embeddings if e.collection_name == ChromaConfig.RESUME_COLLECTION]
        
        print(f"  - Записи для коллекции вакансий: {len(job_records)}")
        print(f"  - Записи для коллекции резюме: {len(resume_records)}")
        
        # Проверяем соответствие chroma_id в ChromaDB
        job_collection = chroma_client.get_job_collection()
        resume_collection = chroma_client.get_resume_collection()
        
        print(f"\nРазмеры коллекций в ChromaDB:")
        print(f"  - Коллекция вакансий: {job_collection.count()} документов")
        print(f"  - Коллекция резюме: {resume_collection.count()} документов")
        
        # Выборочная проверка существования документов
        if job_records:
            print(f"\nПроверяем первую запись вакансии:")
            first_job = job_records[0]
            print(f"  Chroma ID: {first_job.chroma_id}")
            try:
                chroma_doc = job_collection.get(ids=[first_job.chroma_id], include=['documents'])
                if chroma_doc['ids']:
                    print(f"  ✅ Документ найден в ChromaDB")
                else:
                    print(f"  ❌ Документ НЕ найден в ChromaDB")
            except Exception as e:
                print(f"  ❌ Ошибка при поиске в ChromaDB: {e}")
                
        if resume_records:
            print(f"\nПроверяем первую запись резюме:")
            first_resume = resume_records[0]
            print(f"  Chroma ID: {first_resume.chroma_id}")
            try:
                chroma_doc = resume_collection.get(ids=[first_resume.chroma_id], include=['documents'])
                if chroma_doc['ids']:
                    print(f"  ✅ Документ найден в ChromaDB")
                else:
                    print(f"  ❌ Документ НЕ найден в ChromaDB")
            except Exception as e:
                print(f"  ❌ Ошибка при поиске в ChromaDB: {e}")
        
    except Exception as e:
        print(f"Ошибка при диагностике: {e}")
    finally:
        db.close()

def main():
    debug_chroma()
    debug_embedding_metadata()
    check_sync_issue()
    
    print("\n=== ЗАКЛЮЧЕНИЕ ===")
    print("Проблема: Данные есть в embedding_metadata, но нет в ChromaDB коллекциях.")
    print("Это значит, что эмбеддинги были сгенерированы, но не загружены в ChromaDB.")
    print("Возможные причины:")
    print("1. Процесс загрузки эмбеддингов в ChromaDB не был запущен")
    print("2. Была ошибка при загрузке, но она не была замечена")
    print("3. ChromaDB коллекции были очищены после загрузки эмбеддингов")
    print("4. Конфигурация ChromaDB изменилась (путь к базе, имена коллекций)")

if __name__ == "__main__":
    main()
