"""
Модели для хранения метаданных эмбеддингов
"""

from sqlalchemy import JSON, UniqueConstraint
from .base import *

class EmbeddingMetadata(Base):
    """Модель для хранения метаданных эмбеддингов"""
    __tablename__ = 'embedding_metadata'
    
    embedding_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_type = Column(String(50), nullable=False)  # 'resume' или 'job_description'
    source_id = Column(String(255), nullable=False)  # ID записи в источнике
    chroma_document_id = Column(String(255), nullable=False, unique=True)  # ID документа в ChromaDB
    collection_name = Column(String(100), nullable=False)  # Имя коллекции в ChromaDB
    text_content = Column(Text, nullable=False)  # Сырой текст
    model_name = Column(String(100), nullable=False, default='nomic-embed-text:latest')
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp())
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    # Дополнительные метаданные в JSON формате
    additional_metadata = Column(JSON)
    
    # Составной индекс для быстрого поиска по источнику
    __table_args__ = (
        Index('idx_embedding_source', 'source_type', 'source_id'),
        Index('idx_embedding_chroma_id', 'chroma_document_id'),
        Index('idx_embedding_collection', 'collection_name'),
        UniqueConstraint('source_type', 'source_id', name='uq_embedding_source')
    )
    
    def __repr__(self):
        return f"<EmbeddingMetadata(id={self.embedding_id}, source={self.source_type}:{self.source_id})>"
