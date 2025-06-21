"""
Мониторинг GPU задач и автоматическое управление инстансом

Отслеживает очереди Redis и управляет запуском/остановкой GPU-сервера
"""

import os
import time
import threading
import logging
from typing import List, Optional
from celery_app.redis_manager import redis_manager
from common.gcp_instance_manager import gcp_manager

logger = logging.getLogger(__name__)


class GPUTaskMonitor:
    """Мониторинг GPU задач и автоматическое управление инстансом"""
    
    def __init__(self):
        # Параметры мониторинга
        self.check_interval = int(os.environ.get('GPU_CHECK_INTERVAL', '60'))  # секунды
        self.idle_timeout = int(os.environ.get('GPU_IDLE_TIMEOUT', '300'))  # 5 минут простоя
        
        # GPU очереди
        self.gpu_queues = ['embeddings_gpu', 'scoring_tasks']
        
        # Состояние мониторинга
        self._monitoring_thread: Optional[threading.Thread] = None
        self._stop_monitoring = False
        self._last_activity_time = 0
        
    def check_gpu_tasks_in_queues(self) -> int:
        """
        Проверяет наличие GPU задач в очередях Redis
        
        Returns:
            Общее количество задач в GPU очередях
        """
        total_tasks = 0
        
        try:
            redis_conn = redis_manager._get_connection()
            for queue in self.gpu_queues:
                queue_key = f'celery:{queue}'
                queue_length = int(redis_conn.llen(queue_key) or 0)
                total_tasks += queue_length
                
                if queue_length > 0:
                    logger.info(f"Очередь {queue}: {queue_length} задач")
            
            return total_tasks
            
        except Exception as e:
            logger.error(f"Ошибка проверки GPU очередей: {e}")
            return 0
    
    def update_activity_timestamp(self):
        """Обновляет метку времени последней активности"""
        self._last_activity_time = time.time()
        
        # Сохраняем в Redis для синхронизации между процессами
        try:
            redis_conn = redis_manager._get_connection()
            redis_conn.set(
                'gpu_last_activity', 
                self._last_activity_time,
                ex=3600  # TTL 1 час
            )
        except Exception as e:
            logger.error(f"Ошибка сохранения метки активности: {e}")
    
    def get_last_activity_time(self) -> float:
        """Получает время последней активности"""
        try:
            redis_conn = redis_manager._get_connection()
            stored_time = redis_conn.get('gpu_last_activity')
            if stored_time:
                return float(str(stored_time))
        except Exception:
            pass
        
        return self._last_activity_time
    
    def should_start_gpu_instance(self) -> bool:
        """Определяет, нужно ли запускать GPU-инстанс"""
        if not gcp_manager.enabled:
            return False
        
        # Проверяем наличие задач
        task_count = self.check_gpu_tasks_in_queues()
        if task_count == 0:
            return False
        
        # Проверяем, не запущен ли уже инстанс
        if gcp_manager.is_running():
            return False
        
        return True
    
    def should_stop_gpu_instance(self) -> bool:
        """Определяет, нужно ли останавливать GPU-инстанс"""
        if not gcp_manager.enabled:
            return False
        
        # Проверяем, запущен ли инстанс
        if not gcp_manager.is_running():
            return False
        
        # Проверяем наличие задач
        task_count = self.check_gpu_tasks_in_queues()
        if task_count > 0:
            # Есть задачи - обновляем время активности
            self.update_activity_timestamp()
            return False
        
        # Проверяем время простоя
        last_activity = self.get_last_activity_time()
        current_time = time.time()
        idle_time = current_time - last_activity
        
        return idle_time > self.idle_timeout
    
    def handle_gpu_instance_lifecycle(self):
        """Основная логика управления жизненным циклом GPU-инстанса"""
        try:
            if self.should_start_gpu_instance():
                logger.info("Обнаружены GPU задачи. Запуск GPU-инстанса...")
                if gcp_manager.start_instance():
                    self.update_activity_timestamp()
                    logger.info("GPU-инстанс успешно запущен")
                else:
                    logger.error("Не удалось запустить GPU-инстанс")
            
            elif self.should_stop_gpu_instance():
                idle_time = time.time() - self.get_last_activity_time()
                logger.info(f"GPU-инстанс простаивает {idle_time:.0f} секунд. Остановка...")
                if gcp_manager.stop_instance():
                    logger.info("GPU-инстанс остановлен")
                else:
                    logger.error("Не удалось остановить GPU-инстанс")
            
        except Exception as e:
            logger.error(f"Ошибка в управлении GPU-инстансом: {e}")
    
    def start_monitoring(self):
        """Запускает мониторинг в отдельном потоке"""
        if self._monitoring_thread and self._monitoring_thread.is_alive():
            logger.info("Мониторинг GPU задач уже запущен")
            return
        
        if not gcp_manager.enabled:
            logger.info("GCP управление отключено. Мониторинг GPU не запущен.")
            return
        
        logger.info("Запуск мониторинга GPU задач...")
        self._stop_monitoring = False
        self._monitoring_thread = threading.Thread(
            target=self._monitor_loop,
            name="gpu-task-monitor",
            daemon=True
        )
        self._monitoring_thread.start()
    
    def stop_monitoring(self):
        """Останавливает мониторинг"""
        if not self._monitoring_thread:
            return
        
        logger.info("Остановка мониторинга GPU задач...")
        self._stop_monitoring = True
        
        if self._monitoring_thread.is_alive():
            self._monitoring_thread.join(timeout=5.0)
    
    def _monitor_loop(self):
        """Основной цикл мониторинга"""
        while not self._stop_monitoring:
            try:
                self.handle_gpu_instance_lifecycle()
            except Exception as e:
                logger.error(f"Ошибка в цикле мониторинга GPU: {e}")
            
            # Спим до следующей проверки
            time.sleep(self.check_interval)


# Глобальный экземпляр монитора
gpu_monitor = GPUTaskMonitor()
