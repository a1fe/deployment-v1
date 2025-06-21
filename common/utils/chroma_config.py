"""
Конфигурация и клиент для ChromaDB
"""

import os
import chromadb
from chromadb.config import Settings
from chromadb.utils import embedding_functions
from typing import Optional

class ChromaConfig:
    """Конфигурация ChromaDB"""
    
    # Настройки по умолчанию
    CHROMA_PERSIST_DIRECTORY = os.getenv('CHROMA_PERSIST_DIRECTORY', './chroma_db')
    CHROMA_HOST = os.getenv('CHROMA_HOST', 'localhost')
    CHROMA_PORT = int(os.getenv('CHROMA_PORT', '8000'))
    
    # Имена коллекций
    RESUME_COLLECTION = 'resume_embeddings'
    JOB_COLLECTION = 'job_embeddings'
    
    # Модель для эмбеддингов
    EMBEDDING_MODEL = 'nomic-embed-text:latest'


class ChromaDBClient:
    """Клиент для работы с ChromaDB"""
    
    def __init__(self):
        self._client: Optional[chromadb.Client] = None
        self._embedding_function = None
    
    @property
    def client(self) -> chromadb.Client:
        """Lazy initialization клиента ChromaDB"""
        if self._client is None:
            try:
                # Пробуем подключиться к удаленному серверу
                self._client = chromadb.HttpClient(
                    host=ChromaConfig.CHROMA_HOST,
                    port=ChromaConfig.CHROMA_PORT,
                    settings=Settings(allow_reset=True)
                )
                # Проверяем соединение
                self._client.heartbeat()
            except Exception:
                # Если не получается подключиться к удаленному серверу, используем локальный
                self._client = chromadb.PersistentClient(
                    path=ChromaConfig.CHROMA_PERSIST_DIRECTORY,
                    settings=Settings(allow_reset=True)
                )
        return self._client
    
    @property
    def embedding_function(self):
        """Функция для создания эмбеддингов через Ollama"""
        if self._embedding_function is None:
            self._embedding_function = embedding_functions.OllamaEmbeddingFunction(
                model_name=ChromaConfig.EMBEDDING_MODEL,
                url="http://localhost:11434"
            )
        return self._embedding_function
    
    def get_or_create_collection(self, collection_name: str):
        """Получить или создать коллекцию"""
        try:
            collection = self.client.get_collection(
                name=collection_name,
                embedding_function=self.embedding_function
            )
        except Exception:
            # Коллекция не существует, создаем новую
            collection = self.client.create_collection(
                name=collection_name,
                embedding_function=self.embedding_function,
                metadata={"hnsw:space": "cosine"}  # Используем косинусное расстояние
            )
        return collection
    
    def get_resume_collection(self):
        """Получить коллекцию для резюме"""
        return self.get_or_create_collection(ChromaConfig.RESUME_COLLECTION)
    
    def get_job_collection(self):
        """Получить коллекцию для вакансий"""
        return self.get_or_create_collection(ChromaConfig.JOB_COLLECTION)
    
    def health_check(self) -> bool:
        """Проверка здоровья ChromaDB"""
        try:
            self.client.heartbeat()
            return True
        except Exception:
            return False


# Глобальный экземпляр клиента
chroma_client = ChromaDBClient()
