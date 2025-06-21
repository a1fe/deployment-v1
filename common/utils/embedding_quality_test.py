"""
Проверка качества эмбеддингов после старта GPU воркера
Включает обработку ошибок, логирование и мониторинг производительности
"""

import logging
import time
import traceback
from typing import List, Tuple, Optional, Dict, Any
import numpy as np

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EmbeddingQualityChecker:
    """Класс для проверки качества эмбеддингов с обработкой ошибок"""
    
    def __init__(self, timeout: int = 60):
        self.timeout = timeout
        self.test_texts = [
            "Python разработчик с опытом работы в ML",
            "Senior Software Engineer with experience in backend development",
            "Data Scientist specializing in machine learning algorithms",
            "Frontend developer skilled in React and TypeScript",
            "DevOps engineer with expertise in cloud platforms"
        ]
    
    def check_embedding_quality(self) -> Dict[str, Any]:
        """
        Основная функция проверки качества эмбеддингов
        
        Returns:
            Dict с результатами проверки и метриками
        """
        start_time = time.time()
        result = {
            'success': False,
            'error': None,
            'metrics': {},
            'duration': 0,
            'timestamp': time.time()
        }
        
        try:
            logger.info("🔍 Начинаем проверку качества эмбеддингов...")
            
            # Проверка доступности модели
            if not self._check_model_availability():
                raise RuntimeError("Модель эмбеддингов недоступна")
            
            # Генерация тестовых эмбеддингов
            embeddings = self._generate_test_embeddings()
            if embeddings is None:
                raise RuntimeError("Не удалось сгенерировать тестовые эмбеддинги")
            
            # Проверка качества эмбеддингов
            quality_metrics = self._check_embeddings_quality(embeddings)
            
            # Проверка производительности
            performance_metrics = self._check_performance()
            
            # Объединение метрик
            result['metrics'] = {
                **quality_metrics,
                **performance_metrics
            }
            
            result['success'] = True
            logger.info("✅ Проверка качества эмбеддингов завершена успешно")
            
        except Exception as e:
            error_msg = f"Ошибка при проверке качества эмбеддингов: {str(e)}"
            logger.error(f"❌ {error_msg}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            result['error'] = error_msg
        
        finally:
            result['duration'] = time.time() - start_time
            logger.info(f"⏱️ Время выполнения проверки: {result['duration']:.2f} секунд")
        
        return result
    
    def _check_model_availability(self) -> bool:
        """Проверка доступности модели эмбеддингов"""
        try:
            # Попытка импорта и инициализации модели
            from sentence_transformers import SentenceTransformer
            
            # Проверяем доступность CUDA
            import torch
            if torch.cuda.is_available():
                device = 'cuda'
                logger.info(f"🎯 GPU доступен: {torch.cuda.get_device_name()}")
            else:
                device = 'cpu'
                logger.warning("⚠️ GPU недоступен, используется CPU")
            
            # Загружаем модель
            model = SentenceTransformer('all-MiniLM-L6-v2', device=device)
            logger.info(f"✅ Модель загружена на устройство: {device}")
            
            return True
            
        except ImportError as e:
            logger.error(f"❌ Не удалось импортировать sentence_transformers: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке модели: {e}")
            return False
    
    def _generate_test_embeddings(self) -> Optional[np.ndarray]:
        """Генерация тестовых эмбеддингов"""
        try:
            from sentence_transformers import SentenceTransformer
            import torch
            
            device = 'cuda' if torch.cuda.is_available() else 'cpu'
            model = SentenceTransformer('all-MiniLM-L6-v2', device=device)
            
            logger.info(f"🔄 Генерация эмбеддингов для {len(self.test_texts)} текстов...")
            
            start_time = time.time()
            embeddings = model.encode(self.test_texts, convert_to_numpy=True)
            generation_time = time.time() - start_time
            
            logger.info(f"✅ Эмбеддинги сгенерированы за {generation_time:.2f} секунд")
            logger.info(f"📊 Размерность эмбеддингов: {embeddings.shape}")
            
            return embeddings
            
        except Exception as e:
            logger.error(f"❌ Ошибка при генерации эмбеддингов: {e}")
            return None
    
    def _check_embeddings_quality(self, embeddings: np.ndarray) -> Dict[str, Any]:
        """Проверка качества сгенерированных эмбеддингов"""
        try:
            metrics = {}
            
            # Проверка размерности
            metrics['dimension'] = embeddings.shape[1]
            metrics['num_samples'] = embeddings.shape[0]
            
            # Проверка на NaN и бесконечность
            nan_count = np.isnan(embeddings).sum()
            inf_count = np.isinf(embeddings).sum()
            
            metrics['nan_count'] = int(nan_count)
            metrics['inf_count'] = int(inf_count)
            metrics['has_invalid_values'] = nan_count > 0 or inf_count > 0
            
            # Статистические метрики
            metrics['mean_magnitude'] = float(np.linalg.norm(embeddings, axis=1).mean())
            metrics['std_magnitude'] = float(np.linalg.norm(embeddings, axis=1).std())
            
            # Проверка разнообразия эмбеддингов
            similarity_matrix = np.dot(embeddings, embeddings.T)
            np.fill_diagonal(similarity_matrix, 0)  # Убираем самоподобие
            
            metrics['max_similarity'] = float(similarity_matrix.max())
            metrics['avg_similarity'] = float(similarity_matrix.mean())
            metrics['min_similarity'] = float(similarity_matrix.min())
            
            # Проверка качества
            quality_score = self._calculate_quality_score(metrics)
            metrics['quality_score'] = quality_score
            
            if quality_score > 0.7:
                logger.info(f"✅ Качество эмбеддингов хорошее: {quality_score:.3f}")
            elif quality_score > 0.5:
                logger.warning(f"⚠️ Качество эмбеддингов среднее: {quality_score:.3f}")
            else:
                logger.error(f"❌ Качество эмбеддингов низкое: {quality_score:.3f}")
            
            return metrics
            
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке качества эмбеддингов: {e}")
            return {'error': str(e)}
    
    def _calculate_quality_score(self, metrics: Dict[str, float]) -> float:
        """Расчет общего балла качества эмбеддингов"""
        try:
            score = 1.0
            
            # Штраф за невалидные значения
            if metrics.get('has_invalid_values', False):
                score *= 0.1
            
            # Штраф за слишком низкую или высокую величину
            mean_mag = metrics.get('mean_magnitude', 0)
            if mean_mag < 0.1 or mean_mag > 100:
                score *= 0.5
            
            # Штраф за слишком высокое сходство (недостаток разнообразия)
            max_sim = metrics.get('max_similarity', 0)
            if max_sim > 0.95:
                score *= 0.7
            
            # Штраф за слишком низкое среднее сходство
            avg_sim = metrics.get('avg_similarity', 0)
            if avg_sim < 0.1:
                score *= 0.8
            
            return max(0.0, min(1.0, score))
            
        except Exception as e:
            logger.error(f"❌ Ошибка при расчете качества: {e}")
            return 0.0
    
    def _check_performance(self) -> Dict[str, Any]:
        """Проверка производительности генерации эмбеддингов"""
        try:
            from sentence_transformers import SentenceTransformer
            import torch
            
            device = 'cuda' if torch.cuda.is_available() else 'cpu'
            model = SentenceTransformer('all-MiniLM-L6-v2', device=device)
            
            # Тест производительности
            test_sizes = [1, 10, 50, 100]
            performance_metrics = {}
            
            for size in test_sizes:
                test_texts = self.test_texts[:min(size, len(self.test_texts))]
                if len(test_texts) < size:
                    test_texts = test_texts * (size // len(test_texts) + 1)
                    test_texts = test_texts[:size]
                
                start_time = time.time()
                embeddings = model.encode(test_texts, convert_to_numpy=True)
                end_time = time.time()
                
                duration = end_time - start_time
                throughput = size / duration if duration > 0 else 0
                
                performance_metrics[f'throughput_{size}_texts'] = throughput
                performance_metrics[f'latency_{size}_texts'] = duration
            
            logger.info(f"📈 Производительность: {performance_metrics}")
            return performance_metrics
            
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке производительности: {e}")
            return {'performance_error': str(e)}


def check_embedding_quality(timeout: int = 60) -> Dict[str, Any]:
    """
    Публичная функция для проверки качества эмбеддингов
    
    Args:
        timeout: Таймаут в секундах
        
    Returns:
        Результат проверки качества
    """
    checker = EmbeddingQualityChecker(timeout=timeout)
    return checker.check_embedding_quality()


if __name__ == "__main__":
    # Тестирование при прямом запуске
    logger.info("🚀 Запуск проверки качества эмбеддингов...")
    result = check_embedding_quality()
    
    if result['success']:
        print("✅ Проверка прошла успешно")
        print(f"📊 Метрики: {result['metrics']}")
    else:
        print(f"❌ Проверка завершилась с ошибкой: {result['error']}")
    
    print(f"⏱️ Время выполнения: {result['duration']:.2f} секунд")
