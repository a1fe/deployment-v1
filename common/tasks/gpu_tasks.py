"""
GPU-related tasks for HR analysis system
"""

import os
import time
import requests
import psutil
from typing import Dict, Any, Optional
from celery import Celery
from celery.utils.log import get_task_logger

# Создаем экземпляр Celery
# Безопасное подключение к Redis через Secret Manager
from deployment.common.utils.secret_manager import get_redis_url_with_auth
redis_url = get_redis_url_with_auth()
app = Celery('hr_analysis', broker=redis_url, backend=redis_url)
logger = get_task_logger(__name__)


@app.task(
    bind=True,
    name='tasks.gpu_tasks.check_and_start_gpu_server',
    soft_time_limit=300,  # 5 минут
    time_limit=360,       # 6 минут
    max_retries=3
)
def check_and_start_gpu_server(self, required_for: str = 'ai_analysis') -> Dict[str, Any]:
    """
    Проверяет доступность GPU сервера и запускает его при необходимости
    
    Args:
        required_for: Для какой задачи требуется GPU ('ai_analysis', 'embedding', etc.)
    
    Returns:
        Dict с информацией о статусе GPU сервера
    """
    logger.info(f"🔍 Проверка статуса GPU сервера для задачи: {required_for}")
    
    try:
        gpu_server_url = os.getenv('GPU_SERVER_URL', 'http://localhost:8001')
        health_endpoint = f"{gpu_server_url}/health"
        
        # Обновляем прогресс
        self.update_state(
            state='PROGRESS',
            meta={
                'progress': 10,
                'status': 'Проверка доступности GPU сервера',
                'required_for': required_for
            }
        )
        
        # Проверяем доступность GPU сервера
        try:
            response = requests.get(health_endpoint, timeout=10)
            if response.status_code == 200:
                gpu_info = response.json()
                logger.info(f"✅ GPU сервер доступен: {gpu_info}")
                
                self.update_state(
                    state='SUCCESS',
                    meta={
                        'progress': 100,
                        'status': 'GPU сервер доступен',
                        'gpu_info': gpu_info
                    }
                )
                
                return {
                    'status': 'available',
                    'server_url': gpu_server_url,
                    'gpu_info': gpu_info,
                    'action': 'none',
                    'required_for': required_for
                }
        except requests.RequestException:
            logger.warning("⚠️ GPU сервер недоступен, попытка запуска")
        
        # Попытка запуска GPU сервера
        startup_script = os.getenv('GPU_STARTUP_SCRIPT', '/opt/gpu-server/start.sh')
        if os.path.exists(startup_script):
            logger.info(f"🚀 Запуск GPU сервера: {startup_script}")
            
            # Безопасный запуск через process_executor
            from deployment.common.utils.process_executor import start_background_service
            
            result = start_background_service(['bash', startup_script])
            if not result.get('success', False):
                logger.error(f"❌ Ошибка запуска GPU сервера: {result.get('error', 'Unknown error')}")
                return {
                    'status': 'error',
                    'error': f"Failed to start GPU server: {result.get('error', 'Unknown error')}"
                }
            
            logger.info(f"✅ GPU сервер запущен, PID: {result.get('pid')}")
            
            # Ждем запуска (до 60 секунд)
            for i in range(12):  # 12 попыток по 5 секунд
                time.sleep(5)
                try:
                    response = requests.get(health_endpoint, timeout=5)
                    if response.status_code == 200:
                        gpu_info = response.json()
                        logger.info(f"✅ GPU сервер успешно запущен: {gpu_info}")
                        return {
                            'status': 'started',
                            'server_url': gpu_server_url,
                            'gpu_info': gpu_info,
                            'action': 'started',
                            'startup_time': (i + 1) * 5
                        }
                except requests.RequestException:
                    continue
            
            logger.error("❌ GPU сервер не запустился в течение 60 секунд")
            return {
                'status': 'failed',
                'server_url': gpu_server_url,
                'action': 'start_failed',
                'error': 'Timeout waiting for GPU server startup'
            }
        else:
            logger.warning(f"⚠️ Скрипт запуска GPU не найден: {startup_script}")
            return {
                'status': 'unavailable',
                'server_url': gpu_server_url,
                'action': 'script_not_found',
                'error': f'Startup script not found: {startup_script}'
            }
    
    except Exception as e:
        logger.error(f"❌ Ошибка проверки GPU сервера: {e}")
        return {
            'status': 'error',
            'action': 'check_failed',
            'error': str(e)
        }


@app.task(
    bind=True,
    name='tasks.gpu_tasks.ai_analysis_task',
    soft_time_limit=600,  # 10 минут
    time_limit=720,       # 12 минут
    max_retries=2
)
def ai_analysis_task(self, analysis_data: Dict[str, Any], gpu_info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Выполняет AI-анализ на GPU сервере
    
    Args:
        analysis_data: Данные для анализа (результаты реранкинга, матчинга и т.д.)
        gpu_info: Информация о GPU сервере (опционально)
        
    Returns:
        Dict с результатами AI-анализа
    """
    logger.info("🧠 Запуск AI-анализа на GPU сервере")
    
    try:
        # Проверяем, что анализ должен выполняться на GPU
        if gpu_info and gpu_info.get('status') != 'available':
            logger.warning("⚠️ GPU сервер недоступен, пропускаем AI-анализ")
            return {
                'status': 'skipped',
                'reason': 'GPU server unavailable',
                'gpu_info': gpu_info
            }
        
        gpu_server_url = os.getenv('GPU_SERVER_URL', 'http://localhost:8001')
        analysis_endpoint = f"{gpu_server_url}/analyze"
        
        # Подготавливаем данные для анализа
        analysis_request = {
            'analysis_type': analysis_data.get('analysis_type', 'reranking_analysis'),
            'data': analysis_data,
            'model_config': {
                'use_gpu': True,
                'batch_size': int(os.getenv('AI_BATCH_SIZE', '8')),
                'max_length': int(os.getenv('AI_MAX_LENGTH', '512'))
            }
        }
        
        # Выполняем запрос к GPU серверу
        logger.info(f"📡 Отправка данных на GPU сервер: {analysis_endpoint}")
        response = requests.post(
            analysis_endpoint,
            json=analysis_request,
            timeout=300,  # 5 минут на анализ
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code == 200:
            analysis_result = response.json()
            logger.info("✅ AI-анализ выполнен успешно")
            return {
                'status': 'completed',
                'analysis_result': analysis_result,
                'gpu_info': gpu_info,
                'processing_time': analysis_result.get('processing_time', 0)
            }
        else:
            logger.error(f"❌ Ошибка AI-анализа: HTTP {response.status_code}")
            return {
                'status': 'error',
                'error': f'HTTP {response.status_code}: {response.text}',
                'gpu_info': gpu_info
            }
    
    except requests.RequestException as e:
        logger.error(f"❌ Ошибка соединения с GPU сервером: {e}")
        return {
            'status': 'connection_error',
            'error': str(e),
            'gpu_info': gpu_info
        }
    except Exception as e:
        logger.error(f"❌ Ошибка AI-анализа: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'gpu_info': gpu_info
        }


@app.task(
    bind=True,
    name='tasks.gpu_tasks.get_gpu_status',
    soft_time_limit=30,   # 30 секунд
    time_limit=60         # 1 минута
)
def get_gpu_status(self) -> Dict[str, Any]:
    """
    Получает текущий статус GPU ресурсов
    
    Returns:
        Dict с информацией о статусе GPU
    """
    logger.info("📊 Получение статуса GPU")
    
    try:
        gpu_server_url = os.getenv('GPU_SERVER_URL', 'http://localhost:8001')
        status_endpoint = f"{gpu_server_url}/status"
        
        response = requests.get(status_endpoint, timeout=10)
        if response.status_code == 200:
            status_info = response.json()
            logger.info(f"📊 Статус GPU получен: {status_info}")
            return {
                'status': 'available',
                'gpu_status': status_info,
                'server_url': gpu_server_url
            }
        else:
            logger.warning(f"⚠️ GPU сервер вернул статус {response.status_code}")
            return {
                'status': 'unavailable',
                'error': f'HTTP {response.status_code}',
                'server_url': gpu_server_url
            }
    
    except requests.RequestException as e:
        logger.warning(f"⚠️ GPU сервер недоступен: {e}")
        return {
            'status': 'unavailable',
            'error': str(e),
            'server_url': gpu_server_url
        }
    except Exception as e:
        logger.error(f"❌ Ошибка получения статуса GPU: {e}")
        return {
            'status': 'error',
            'error': str(e)
        }


@app.task(
    bind=True,
    name='tasks.gpu_tasks.ai_analysis',
    soft_time_limit=900,  # 15 минут
    time_limit=1080,      # 18 минут
    max_retries=2
)
def ai_analysis(self, documents_data: Dict[str, Any], analysis_type: str = 'match_scoring') -> Dict[str, Any]:
    """
    AI анализ документов на GPU сервере
    
    Args:
        documents_data: Данные документов для анализа
        analysis_type: Тип анализа ('match_scoring', 'skills_extraction', 'sentiment_analysis')
        
    Returns:
        Dict с результатами AI анализа
    """
    from datetime import datetime
    
    logger.info(f"🤖 Запуск AI анализа типа '{analysis_type}' для {len(documents_data.get('documents', []))} документов")
    
    try:
        gpu_server_url = os.getenv('GPU_SERVER_URL', 'http://localhost:8001')
        analysis_endpoint = f"{gpu_server_url}/ai/analyze"
        
        # Обновляем прогресс
        self.update_state(
            state='PROGRESS',
            meta={
                'progress': 10,
                'status': f'Подготовка данных для AI анализа типа {analysis_type}',
                'analysis_type': analysis_type
            }
        )
        
        # Подготавливаем данные для анализа
        analysis_request = {
            'documents': documents_data.get('documents', []),
            'analysis_type': analysis_type,
            'options': {
                'include_confidence': True,
                'detailed_results': True,
                'batch_size': 10  # Обрабатываем по 10 документов
            }
        }
        
        # Обновляем прогресс
        self.update_state(
            state='PROGRESS',
            meta={
                'progress': 30,
                'status': 'Отправка данных на GPU сервер для анализа',
                'documents_count': len(documents_data.get('documents', []))
            }
        )
        
        # Отправляем запрос на GPU сервер
        response = requests.post(
            analysis_endpoint,
            json=analysis_request,
            timeout=900,  # 15 минут
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code == 200:
            analysis_results = response.json()
            
            # Обновляем прогресс
            self.update_state(
                state='PROGRESS',
                meta={
                    'progress': 80,
                    'status': 'Обработка результатов AI анализа',
                    'results_received': True
                }
            )
            
            # Обрабатываем результаты
            processed_results = _process_ai_results(analysis_results, analysis_type)
            
            # Финальное обновление прогресса
            self.update_state(
                state='SUCCESS',
                meta={
                    'progress': 100,
                    'status': 'AI анализ завершен успешно',
                    'analysis_type': analysis_type,
                    'processed_count': len(processed_results.get('results', []))
                }
            )
            
            result = {
                'status': 'completed',
                'analysis_type': analysis_type,
                'results': processed_results,
                'stats': {
                    'total_documents': len(documents_data.get('documents', [])),
                    'processed_documents': len(processed_results.get('results', [])),
                    'processing_time': processed_results.get('processing_time'),
                    'confidence_scores': processed_results.get('confidence_stats')
                },
                'processed_at': datetime.utcnow().isoformat()
            }
            
            logger.info(f"✅ AI анализ завершен: обработано {len(processed_results.get('results', []))} документов")
            return result
            
        else:
            error_msg = f"GPU сервер вернул ошибку: {response.status_code} - {response.text}"
            logger.error(f"❌ {error_msg}")
            return {
                'status': 'error',
                'error': error_msg,
                'analysis_type': analysis_type,
                'processed_at': datetime.utcnow().isoformat()
            }
    
    except requests.exceptions.Timeout:
        error_msg = "Таймаут при обращении к GPU серверу"
        logger.error(f"❌ {error_msg}")
        return {
            'status': 'error',
            'error': error_msg,
            'analysis_type': analysis_type,
            'processed_at': datetime.utcnow().isoformat()
        }
    
    except Exception as e:
        logger.error(f"❌ Критическая ошибка AI анализа: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'analysis_type': analysis_type,
            'processed_at': datetime.utcnow().isoformat()
        }


@app.task(
    bind=True,
    name='tasks.gpu_tasks.gpu_health_check',
    soft_time_limit=60,
    time_limit=90,
    max_retries=3
)
def gpu_health_check(self) -> Dict[str, Any]:
    """
    Расширенная проверка состояния GPU сервера и его ресурсов
    
    Returns:
        Dict с информацией о состоянии GPU
    """
    from datetime import datetime
    
    logger.info("🔧 Расширенная проверка состояния GPU сервера")
    
    try:
        gpu_server_url = os.getenv('GPU_SERVER_URL', 'http://localhost:8001')
        status_endpoint = f"{gpu_server_url}/gpu/status"
        
        response = requests.get(status_endpoint, timeout=30)
        
        if response.status_code == 200:
            gpu_status = response.json()
            logger.info(f"✅ GPU статус получен: {gpu_status}")
            
            return {
                'status': 'healthy',
                'gpu_info': gpu_status,
                'server_url': gpu_server_url,
                'checked_at': datetime.utcnow().isoformat()
            }
        else:
            error_msg = f"Не удалось получить статус GPU: {response.status_code}"
            logger.error(f"❌ {error_msg}")
            return {
                'status': 'unhealthy',
                'error': error_msg,
                'server_url': gpu_server_url,
                'checked_at': datetime.utcnow().isoformat()
            }
    
    except Exception as e:
        logger.error(f"❌ Ошибка проверки GPU: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'checked_at': datetime.utcnow().isoformat()
        }


def _process_ai_results(raw_results: Dict[str, Any], analysis_type: str) -> Dict[str, Any]:
    """
    Обработка результатов AI анализа
    
    Args:
        raw_results: Сырые результаты от GPU сервера
        analysis_type: Тип анализа
        
    Returns:
        Обработанные результаты
    """
    from datetime import datetime
    
    try:
        processed_results = {
            'results': [],
            'processing_time': raw_results.get('processing_time'),
            'confidence_stats': {
                'avg_confidence': 0.0,
                'min_confidence': 1.0,
                'max_confidence': 0.0
            }
        }
        
        confidences = []
        
        for result in raw_results.get('results', []):
            processed_result = {
                'document_id': result.get('document_id'),
                'analysis_type': analysis_type,
                'confidence': result.get('confidence', 0.0),
                'processed_at': datetime.utcnow().isoformat()
            }
            
            # Специфичная обработка по типу анализа
            if analysis_type == 'match_scoring':
                processed_result.update({
                    'match_score': result.get('match_score', 0.0),
                    'matching_skills': result.get('matching_skills', []),
                    'missing_skills': result.get('missing_skills', []),
                    'overall_fit': result.get('overall_fit', 'unknown')
                })
            elif analysis_type == 'skills_extraction':
                processed_result.update({
                    'extracted_skills': result.get('skills', []),
                    'skill_categories': result.get('categories', {}),
                    'experience_level': result.get('experience_level', 'unknown')
                })
            elif analysis_type == 'sentiment_analysis':
                processed_result.update({
                    'sentiment': result.get('sentiment', 'neutral'),
                    'sentiment_score': result.get('sentiment_score', 0.0),
                    'key_phrases': result.get('key_phrases', [])
                })
            
            processed_results['results'].append(processed_result)
            confidences.append(result.get('confidence', 0.0))
        
        # Вычисляем статистику по уверенности
        if confidences:
            processed_results['confidence_stats'] = {
                'avg_confidence': sum(confidences) / len(confidences),
                'min_confidence': min(confidences),
                'max_confidence': max(confidences)
            }
        
        return processed_results
    
    except Exception as e:
        logger.error(f"❌ Ошибка обработки результатов AI анализа: {e}")
        return {
            'results': [],
            'error': str(e)
        }
