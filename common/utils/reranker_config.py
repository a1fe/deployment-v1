"""
Конфигурация для BGE Reranker модели

Настройки для переоценки сходства между вакансиями и резюме
"""

import os
import logging
from typing import Optional, List, Tuple
from FlagEmbedding import FlagReranker

logger = logging.getLogger(__name__)


class RerankerConfig:
    """Конфигурация для BGE Reranker"""
    
    # Модель по умолчанию (BGE-M3 с максимальной длиной токенов)
    DEFAULT_MODEL = "BAAI/bge-reranker-v2-m3"
    
    # Альтернативные модели
    ALTERNATIVE_MODELS = [
        "BAAI/bge-reranker-large",
        "BAAI/bge-reranker-base",
        "ms-marco-MiniLM-L-6-v2"
    ]
    
    # Максимальная длина токенов для BGE-M3 (8192 токена ≈ 32,000-35,000 символов)
    MAX_TOKEN_LENGTH = 8192
    MAX_CHAR_LENGTH = 32000  # Приблизительное ограничение по символам
    
    # Размер batch для обработки
    BATCH_SIZE = 8
    
    # Минимальный score для валидного результата
    MIN_RERANK_SCORE = -10.0  # BGE reranker может возвращать отрицательные значения


class RerankerClient:
    """Клиент для работы с BGE Reranker"""
    
    def __init__(self, model_name: str = RerankerConfig.DEFAULT_MODEL):
        """
        Инициализация клиента reranker
        
        Args:
            model_name: Название модели для reranking
        """
        self.model_name = model_name
        self._reranker: Optional[FlagReranker] = None
        self._initialized = False
    
    def _initialize_reranker(self) -> bool:
        """
        Ленивая инициализация reranker модели
        
        Returns:
            True если модель успешно загружена
        """
        if self._initialized:
            return True
        
        try:
            logger.info(f"🔄 Загрузка BGE Reranker модели: {self.model_name}")
            self._reranker = FlagReranker(
                self.model_name,
                use_fp16=True  # Используем FP16 для экономии памяти
            )
            self._initialized = True
            logger.info(f"✅ BGE Reranker модель {self.model_name} успешно загружена")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки BGE Reranker модели {self.model_name}: {e}")
            
            # Пробуем альтернативные модели
            for alt_model in RerankerConfig.ALTERNATIVE_MODELS:
                if alt_model != self.model_name:
                    try:
                        logger.info(f"🔄 Пробуем альтернативную модель: {alt_model}")
                        self._reranker = FlagReranker(alt_model, use_fp16=True)
                        self.model_name = alt_model
                        self._initialized = True
                        logger.info(f"✅ Альтернативная модель {alt_model} успешно загружена")
                        return True
                    except Exception as alt_e:
                        logger.warning(f"⚠️ Альтернативная модель {alt_model} также не загрузилась: {alt_e}")
                        continue
            
            logger.error("❌ Не удалось загрузить ни одну reranker модель")
            return False
    
    def health_check(self) -> bool:
        """
        Проверка доступности reranker
        
        Returns:
            True если reranker доступен
        """
        return self._initialize_reranker()
    
    def rerank_texts(self, query: str, texts: List[str]) -> List[Tuple[int, float]]:
        """
        Переоценка релевантности текстов относительно запроса
        
        Args:
            query: Запрос (описание вакансии или резюме)
            texts: Список текстов для переоценки
            
        Returns:
            Список кортежей (индекс, score) отсортированный по убыванию score
        """
        if not self._initialize_reranker():
            logger.error("❌ Reranker не инициализирован")
            return []
        
        if not texts:
            logger.warning("⚠️ Пустой список текстов для reranking")
            return []
        
        try:
            # Обрезаем тексты до максимальной длины (BGE-M3 поддерживает до 8192 токенов)
            truncated_query = query[:RerankerConfig.MAX_CHAR_LENGTH]
            truncated_texts = [text[:RerankerConfig.MAX_CHAR_LENGTH] for text in texts]
            
            # Формируем пары для reranking (кортежи вместо списков)
            pairs = [(truncated_query, text) for text in truncated_texts]
            
            logger.info(f"🔍 Reranking {len(pairs)} текстов с помощью {self.model_name}")
            
            # Получаем scores
            if self._reranker is None:
                logger.error("❌ Reranker не инициализирован")
                return []
            
            scores = self._reranker.compute_score(pairs)
            
            # Безопасная обработка scores
            if scores is None:
                logger.warning("⚠️ Reranker вернул None scores")
                return []
            
            # Преобразуем в список float
            if hasattr(scores, '__iter__') and not isinstance(scores, str):
                # scores это массив или список
                try:
                    scores_list = [float(score) for score in scores]
                except (ValueError, TypeError):
                    logger.error("❌ Не удалось преобразовать scores в float")
                    return []
            else:
                # scores это одно значение
                try:
                    scores_list = [float(scores)]
                except (ValueError, TypeError):
                    logger.error("❌ Не удалось преобразовать score в float")
                    return []
            
            # Создаем список (индекс, score) и сортируем по убыванию score
            indexed_scores = [(i, score) for i, score in enumerate(scores_list)]
            indexed_scores.sort(key=lambda x: x[1], reverse=True)
            
            # Фильтруем по минимальному score
            filtered_scores = [
                (idx, score) for idx, score in indexed_scores 
                if score >= RerankerConfig.MIN_RERANK_SCORE
            ]
            
            logger.info(f"📊 Reranking завершен: {len(filtered_scores)}/{len(texts)} текстов прошли фильтр")
            return filtered_scores
            
        except Exception as e:
            logger.error(f"❌ Ошибка при reranking: {e}")
            return []
    
    def rerank_matches(self, query: str, matches: List[dict]) -> List[dict]:
        """
        Переоценка найденных совпадений с добавлением rerank_score
        
        Args:
            query: Запрос (описание вакансии)
            matches: Список найденных совпадений с полем 'snippet' или 'document'
            
        Returns:
            Обновленный список совпадений с добавленным rerank_score, отсортированный по rerank_score
        """
        if not matches:
            return []
        
        # Извлекаем тексты для reranking
        texts = []
        for match in matches:
            # Пробуем разные поля с текстом
            text = match.get('snippet') or match.get('document') or match.get('text') or ''
            texts.append(text)
        
        # Выполняем reranking
        reranked_results = self.rerank_texts(query, texts)
        
        if not reranked_results:
            logger.warning("⚠️ Reranking не вернул результатов")
            return matches
        
        # Обновляем matches с rerank_score
        updated_matches = []
        for idx, rerank_score in reranked_results:
            if idx < len(matches):
                match = matches[idx].copy()
                match['rerank_score'] = round(rerank_score, 4)
                updated_matches.append(match)
        
        logger.info(f"✅ Добавлен rerank_score к {len(updated_matches)} совпадениям")
        return updated_matches
    
    def rerank_from_chroma_embeddings(self, query_embedding: List[float], 
                                     candidate_embeddings: List[List[float]], 
                                     candidate_texts: Optional[List[str]] = None) -> List[Tuple[int, float]]:
        """
        Переоценка релевантности на основе эмбеддингов из ChromaDB
        
        Args:
            query_embedding: Эмбеддинг запроса (вакансии или резюме)
            candidate_embeddings: Список эмбеддингов кандидатов
            candidate_texts: Опциональные тексты кандидатов для дополнительного анализа
            
        Returns:
            Список кортежей (индекс, score) отсортированный по убыванию score
        """
        if not self._initialize_reranker():
            logger.error("❌ Reranker не инициализирован")
            return []
        
        if not candidate_embeddings:
            logger.warning("⚠️ Пустой список эмбеддингов для reranking")
            return []
        
        try:
            # BGE-M3 может работать как с текстом, так и с эмбеддингами
            # Если у нас есть тексты, используем их с ограничением по длине
            if candidate_texts and len(candidate_texts) == len(candidate_embeddings):
                logger.info(f"🔍 Reranking {len(candidate_texts)} текстов с помощью BGE-M3")
                
                # Создаем фиктивный запрос, так как у нас есть только эмбеддинг запроса
                # В реальности BGE-M3 лучше работает с парами текст-текст
                query_text = "Search query based on embeddings"
                
                # Обрезаем тексты до максимальной длины
                truncated_texts = [text[:RerankerConfig.MAX_CHAR_LENGTH] if text else "" 
                                  for text in candidate_texts]
                
                # Формируем пары для reranking
                pairs = [(query_text, text) for text in truncated_texts]
                
                # Получаем scores через стандартный метод
                if self._reranker is None:
                    logger.error("❌ Reranker не инициализирован для BGE-M3")
                    return []
                
                scores = self._reranker.compute_score(pairs)
                
            else:
                # Если текстов нет, используем только эмбеддинги
                # Вычисляем косинусное сходство между query_embedding и candidate_embeddings
                logger.info(f"🔍 Reranking {len(candidate_embeddings)} эмбеддингов через косинусное сходство")
                
                import numpy as np
                from sklearn.metrics.pairwise import cosine_similarity
                
                # Преобразуем в numpy массивы
                query_array = np.array(query_embedding).reshape(1, -1)
                candidates_array = np.array(candidate_embeddings)
                
                # Вычисляем косинусное сходство
                similarities = cosine_similarity(query_array, candidates_array)[0]
                scores = similarities.tolist()
            
            # Безопасная обработка scores
            if scores is None:
                logger.warning("⚠️ Reranker вернул None scores")
                return []
            
            # Преобразуем в список float
            if hasattr(scores, '__iter__') and not isinstance(scores, str):
                # scores это массив или список
                try:
                    scores_list = [float(score) for score in scores]
                except (ValueError, TypeError):
                    logger.error("❌ Не удалось преобразовать scores в float")
                    return []
            else:
                # scores это одно значение
                try:
                    scores_list = [float(scores)]
                except (ValueError, TypeError):
                    logger.error("❌ Не удалось преобразовать score в float")
                    return []
            
            # Создаем список (индекс, score) и сортируем по убыванию score
            indexed_scores = [(i, score) for i, score in enumerate(scores_list)]
            indexed_scores.sort(key=lambda x: x[1], reverse=True)
            
            # Фильтруем по минимальному score
            filtered_scores = [
                (idx, score) for idx, score in indexed_scores 
                if score >= RerankerConfig.MIN_RERANK_SCORE
            ]
            
            logger.info(f"📊 Reranking завершен: {len(filtered_scores)}/{len(candidate_embeddings)} результатов прошли фильтр")
            return filtered_scores
            
        except Exception as e:
            logger.error(f"❌ Ошибка при reranking эмбеддингов: {e}")
            return []

    def rerank_chroma_matches(self, job_embedding: List[float], 
                             resume_matches: List[dict]) -> List[dict]:
        """
        Переоценка результатов поиска из ChromaDB с использованием эмбеддингов
        
        Args:
            job_embedding: Эмбеддинг вакансии
            resume_matches: Результаты поиска из ChromaDB с эмбеддингами и текстами
            
        Returns:
            Обновленный список совпадений с добавленным rerank_score
        """
        if not resume_matches:
            return []
        
        # Извлекаем эмбеддинги и тексты из результатов ChromaDB
        candidate_embeddings = []
        candidate_texts = []
        
        for match in resume_matches:
            # Пробуем извлечь эмбеддинг из метаданных или документа
            embedding = match.get('embedding') or match.get('vector')
            if embedding:
                candidate_embeddings.append(embedding)
            else:
                # Если эмбеддинга нет, создаем пустой список нужной размерности
                # (это не должно происходить в ChromaDB, но добавляем для безопасности)
                candidate_embeddings.append([0.0] * len(job_embedding))
            
            # Извлекаем текст для дополнительного анализа
            text = match.get('snippet') or match.get('document') or match.get('text') or ''
            candidate_texts.append(text)
        
        # Выполняем reranking
        reranked_results = self.rerank_from_chroma_embeddings(
            job_embedding, 
            candidate_embeddings, 
            candidate_texts
        )
        
        if not reranked_results:
            logger.warning("⚠️ Reranking не вернул результатов")
            return resume_matches
        
        # Обновляем matches с rerank_score
        updated_matches = []
        for idx, rerank_score in reranked_results:
            if idx < len(resume_matches):
                match = resume_matches[idx].copy()
                match['rerank_score'] = round(rerank_score, 4)
                # Добавляем информацию о том, что результат прошел reranking
                match['reranked_with_bge_m3'] = True
                updated_matches.append(match)
        
        logger.info(f"✅ Добавлен rerank_score к {len(updated_matches)} совпадениям через BGE-M3")
        return updated_matches


# Глобальный экземпляр клиента
reranker_client = RerankerClient()


def get_reranker_client() -> RerankerClient:
    """Получить глобальный экземпляр reranker клиента"""
    return reranker_client
