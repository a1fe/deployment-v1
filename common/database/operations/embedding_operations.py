"""
CRUD операции для работы с эмбеддингами
"""

import uuid
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_

from .base_crud import BaseCRUD
from common.models.embeddings import EmbeddingMetadata


class EmbeddingCRUD(BaseCRUD):
    """CRUD операции для эмбеддингов"""
    
    def __init__(self):
        super().__init__(EmbeddingMetadata)
    
    def get_by_source(self, db: Session, source_type: str, source_id: str) -> Optional[EmbeddingMetadata]:
        """Получить эмбеддинг по источнику"""
        return db.query(self.model).filter(
            and_(
                self.model.source_type == source_type,
                self.model.source_id == source_id
            )
        ).first()
    
    def get_by_chroma_id(self, db: Session, chroma_document_id: str) -> Optional[EmbeddingMetadata]:
        """Получить эмбеддинг по ID документа в ChromaDB"""
        return db.query(self.model).filter(
            self.model.chroma_document_id == chroma_document_id
        ).first()
    
    def get_by_collection(self, db: Session, collection_name: str) -> List[EmbeddingMetadata]:
        """Получить все эмбеддинги из коллекции"""
        return db.query(self.model).filter(
            self.model.collection_name == collection_name
        ).all()
    
    def create_embedding_metadata(
        self,
        db: Session,
        source_type: str,
        source_id: str,
        chroma_document_id: str,
        collection_name: str,
        text_content: str,
        model_name: str = 'nomic-embed-text:latest',
        additional_metadata: Optional[Dict[str, Any]] = None
    ) -> EmbeddingMetadata:
        """Создать метаданные эмбеддинга"""
        
        # Проверяем, существует ли уже эмбеддинг для этого источника
        existing = self.get_by_source(db, source_type, source_id)
        if existing:
            # Обновляем существующий
            existing.chroma_document_id = chroma_document_id
            existing.collection_name = collection_name
            existing.text_content = text_content
            existing.model_name = model_name
            existing.additional_metadata = additional_metadata
            db.commit()
            db.refresh(existing)
            return existing
        
        # Создаем новый
        embedding_metadata = EmbeddingMetadata(
            source_type=source_type,
            source_id=source_id,
            chroma_document_id=chroma_document_id,
            collection_name=collection_name,
            text_content=text_content,
            model_name=model_name,
            additional_metadata=additional_metadata or {}
        )
        
        db.add(embedding_metadata)
        db.commit()
        db.refresh(embedding_metadata)
        return embedding_metadata
    
    def delete_by_source(self, db: Session, source_type: str, source_id: str) -> bool:
        """Удалить эмбеддинг по источнику"""
        embedding = self.get_by_source(db, source_type, source_id)
        if embedding:
            db.delete(embedding)
            db.commit()
            return True
        return False
    
    def get_sources_without_embeddings(
        self, 
        db: Session, 
        source_type: str, 
        source_ids: List[str]
    ) -> List[str]:
        """Получить источники, для которых еще нет эмбеддингов"""
        existing_embeddings = db.query(self.model.source_id).filter(
            and_(
                self.model.source_type == source_type,
                self.model.source_id.in_(source_ids)
            )
        ).all()
        
        existing_source_ids = [emb.source_id for emb in existing_embeddings]
        return [source_id for source_id in source_ids if source_id not in existing_source_ids]


# Глобальный экземпляр
embedding_crud = EmbeddingCRUD()
