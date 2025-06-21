"""
Управление GPU-инстансом в Google Cloud Platform

Автоматический запуск/остановка GPU-сервера по требованию
"""

import os
import time
import logging
from typing import Optional, Dict, Any
from google.cloud import compute_v1

logger = logging.getLogger(__name__)


class GCPInstanceManager:
    """Управление GPU-инстансом в Google Cloud"""
    
    def __init__(self):
        self.project_id = os.environ.get('GCP_PROJECT_ID')
        self.zone = os.environ.get('GCP_ZONE', 'us-central1-a')
        self.gpu_instance_name = os.environ.get('GPU_INSTANCE_NAME')
        
        if not self.project_id or not self.gpu_instance_name:
            logger.warning("GCP конфигурация не найдена. GPU автоматическое управление отключено.")
            self._enabled = False
        else:
            self._enabled = True
            self.instance_client = compute_v1.InstancesClient()
            self.operation_client = compute_v1.ZoneOperationsClient()
    
    @property
    def enabled(self) -> bool:
        """Проверяет, включено ли управление GPU-инстансом"""
        return self._enabled
    
    def get_instance_status(self) -> Optional[str]:
        """Получает статус GPU-инстанса"""
        if not self.enabled:
            return None
        
        try:
            instance = self.instance_client.get(
                project=self.project_id,
                zone=self.zone,
                instance=self.gpu_instance_name
            )
            return instance.status
        except Exception as e:
            logger.error(f"Ошибка получения статуса GPU-инстанса: {e}")
            return None
    
    def is_running(self) -> bool:
        """Проверяет, запущен ли GPU-инстанс"""
        status = self.get_instance_status()
        return status == "RUNNING"
    
    def is_stopped(self) -> bool:
        """Проверяет, остановлен ли GPU-инстанс"""
        status = self.get_instance_status()
        return status in ["STOPPED", "TERMINATED"]
    
    def start_instance(self, wait_timeout: int = 300) -> bool:
        """
        Запускает GPU-инстанс
        
        Args:
            wait_timeout: Максимальное время ожидания запуска (секунды)
            
        Returns:
            True если инстанс успешно запущен
        """
        if not self.enabled:
            logger.warning("GCP управление отключено")
            return False
        
        if self.is_running():
            logger.info(f"GPU-инстанс {self.gpu_instance_name} уже запущен")
            return True
        
        try:
            logger.info(f"Запуск GPU-инстанса {self.gpu_instance_name}...")
            operation = self.instance_client.start(
                project=self.project_id,
                zone=self.zone,
                instance=self.gpu_instance_name
            )
            
            # Ожидание завершения операции
            start_time = time.time()
            while time.time() - start_time < wait_timeout:
                operation = self.operation_client.get(
                    project=self.project_id,
                    zone=self.zone,
                    operation=operation.name
                )
                
                if operation.status == compute_v1.Operation.Status.DONE:
                    if operation.error:
                        logger.error(f"Ошибка запуска GPU-инстанса: {operation.error.errors}")
                        return False
                    
                    # Дополнительная проверка статуса инстанса
                    if self.is_running():
                        logger.info(f"GPU-инстанс {self.gpu_instance_name} успешно запущен")
                        return True
                    break
                
                time.sleep(5)
            
            logger.error(f"Таймаут запуска GPU-инстанса ({wait_timeout} сек)")
            return False
            
        except Exception as e:
            logger.error(f"Ошибка при запуске GPU-инстанса: {e}")
            return False
    
    def stop_instance(self) -> bool:
        """
        Останавливает GPU-инстанс
        
        Returns:
            True если инстанс успешно остановлен
        """
        if not self.enabled:
            logger.warning("GCP управление отключено")
            return False
        
        if self.is_stopped():
            logger.info(f"GPU-инстанс {self.gpu_instance_name} уже остановлен")
            return True
        
        try:
            logger.info(f"Остановка GPU-инстанса {self.gpu_instance_name}...")
            operation = self.instance_client.stop(
                project=self.project_id,
                zone=self.zone,
                instance=self.gpu_instance_name
            )
            
            logger.info(f"Команда остановки GPU-инстанса отправлена")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при остановке GPU-инстанса: {e}")
            return False


# Глобальный экземпляр менеджера
gcp_manager = GCPInstanceManager()
