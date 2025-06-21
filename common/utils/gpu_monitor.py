"""
Мониторинг GPU задач и автоматическое управление GPU инстансом
Отслеживает очереди GPU задач и включает/выключает GPU сервер по требованию
"""

import os
import time
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import asyncio
from dataclasses import dataclass

try:
    from celery import Celery
    from .gcloud_manager import GCloudGPUManager
except ImportError:
    # Для прямого запуска
    import sys
    sys.path.append('.')
    from celery_app.celery_app import app as celery_app
    from gcloud_manager import GCloudGPUManager


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class QueueStats:
    """Статистика очереди задач"""
    name: str
    active_tasks: int
    pending_tasks: int
    failed_tasks: int
    last_update: datetime


@dataclass
class GPUMonitorConfig:
    """Конфигурация мониторинга GPU"""
    # Очереди GPU задач
    gpu_queues: List[str]
    
    # Пороги для включения GPU сервера
    min_pending_tasks: int = 1  # Минимум задач в очереди для включения
    max_idle_time: int = 600   # Максимальное время простоя перед выключением (секунды)
    
    # Интервалы проверки
    check_interval: int = 30   # Интервал проверки очередей (секунды)
    startup_delay: int = 120   # Задержка после запуска GPU сервера (секунды)
    
    # Настройки безопасности
    max_startup_attempts: int = 3  # Максимум попыток запуска
    min_uptime: int = 300      # Минимальное время работы перед выключением (секунды)


class GPUTaskMonitor:
    """Монитор GPU задач с автоматическим управлением инстансом"""
    
    def __init__(self, config: Optional[GPUMonitorConfig] = None):
        # Конфигурация по умолчанию
        if config is None:
            config = GPUMonitorConfig(
                gpu_queues=['embeddings_gpu', 'scoring_tasks'],
                min_pending_tasks=1,
                max_idle_time=600,  # 10 минут
                check_interval=30,
                startup_delay=120,
                max_startup_attempts=3,
                min_uptime=300  # 5 минут
            )
        
        self.config = config
        self.gpu_manager = None
        self.last_activity_time = datetime.now()
        self.gpu_started_at: Optional[datetime] = None
        self.startup_attempts = 0
        
        # Инициализируем GPU менеджер если настроен
        try:
            if os.getenv('GPU_INSTANCE_NAME'):
                self.gpu_manager = GCloudGPUManager()
                logger.info("GPU менеджер инициализирован")
            else:
                logger.info("GPU_INSTANCE_NAME не настроен, автоматическое управление отключено")
        except Exception as e:
            logger.warning(f"Не удалось инициализировать GPU менеджер: {e}")
        
        # Подключение к Celery
        try:
            from celery_app.celery_app import app as celery_app
            self.celery_app = celery_app
            logger.info("Подключение к Celery установлено")
        except Exception as e:
            logger.error(f"Ошибка подключения к Celery: {e}")
            raise
    
    def get_queue_stats(self) -> Dict[str, QueueStats]:
        """Получить статистику очередей"""
        stats = {}
        
        try:
            # Получаем информацию об активных задачах
            inspect = self.celery_app.control.inspect()
            
            # Активные задачи
            active_tasks = inspect.active() or {}
            
            # Зарезервированные задачи (pending)
            reserved_tasks = inspect.reserved() or {}
            
            # Статистика по воркерам
            worker_stats = inspect.stats() or {}
            
            for queue_name in self.config.gpu_queues:
                active_count = 0
                pending_count = 0
                
                # Подсчитываем задачи по воркерам
                for worker_name, tasks in active_tasks.items():
                    for task in tasks:
                        if task.get('delivery_info', {}).get('routing_key') == queue_name:
                            active_count += 1
                
                for worker_name, tasks in reserved_tasks.items():
                    for task in tasks:
                        if task.get('delivery_info', {}).get('routing_key') == queue_name:
                            pending_count += 1
                
                stats[queue_name] = QueueStats(
                    name=queue_name,
                    active_tasks=active_count,
                    pending_tasks=pending_count,
                    failed_tasks=0,  # TODO: получать из Redis
                    last_update=datetime.now()
                )
                
        except Exception as e:
            logger.error(f"Ошибка получения статистики очередей: {e}")
            # Создаем пустую статистику
            for queue_name in self.config.gpu_queues:
                stats[queue_name] = QueueStats(
                    name=queue_name,
                    active_tasks=0,
                    pending_tasks=0,
                    failed_tasks=0,
                    last_update=datetime.now()
                )
        
        return stats
    
    def has_gpu_tasks(self, stats: Dict[str, QueueStats]) -> bool:
        """Проверить наличие GPU задач"""
        total_tasks = sum(
            stat.active_tasks + stat.pending_tasks 
            for stat in stats.values()
        )
        return total_tasks >= self.config.min_pending_tasks
    
    def should_start_gpu(self, stats: Dict[str, QueueStats]) -> bool:
        """Определить, нужно ли запускать GPU сервер"""
        if not self.gpu_manager:
            return False
        
        # Проверяем количество задач
        if not self.has_gpu_tasks(stats):
            return False
        
        # Проверяем, не превышено ли количество попыток запуска
        if self.startup_attempts >= self.config.max_startup_attempts:
            logger.warning("Превышено максимальное количество попыток запуска GPU")
            return False
        
        # Проверяем, не запущен ли уже GPU сервер
        if self.gpu_manager.is_instance_running():
            return False
        
        return True
    
    def should_stop_gpu(self, stats: Dict[str, QueueStats]) -> bool:
        """Определить, нужно ли остановить GPU сервер"""
        if not self.gpu_manager:
            return False
        
        # Проверяем, запущен ли GPU сервер
        if not self.gpu_manager.is_instance_running():
            return False
        
        # Проверяем минимальное время работы
        if self.gpu_started_at:
            uptime = (datetime.now() - self.gpu_started_at).total_seconds()
            if uptime < self.config.min_uptime:
                logger.info(f"GPU сервер работает {uptime:.0f}с, минимум {self.config.min_uptime}с")
                return False
        
        # Проверяем наличие задач
        if self.has_gpu_tasks(stats):
            self.last_activity_time = datetime.now()
            return False
        
        # Проверяем время простоя
        idle_time = (datetime.now() - self.last_activity_time).total_seconds()
        return idle_time >= self.config.max_idle_time
    
    def wait_for_gpu_worker_ready(self, timeout: int = 120) -> bool:
        """Ждать готовности GPU воркера через Celery inspect ping"""
        logger.info("Ожидание готовности GPU воркера...")
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                inspect = self.celery_app.control.inspect()
                active = inspect.active() or {}
                # Проверяем наличие хотя бы одного GPU воркера
                for worker_name in (active.keys() if active else []):
                    if any(q in worker_name for q in self.config.gpu_queues):
                        logger.info(f"GPU воркер {worker_name} готов")
                        return True
                logger.info("GPU воркер не готов, ожидание...")
            except Exception as e:
                logger.info(f"Ошибка при ping воркера: {e}")
            time.sleep(5)
        logger.error("Таймаут ожидания GPU воркера")
        return False

    def start_gpu_server(self) -> bool:
        """Запустить GPU сервер и дождаться готовности воркера"""
        if not self.gpu_manager:
            return False
        
        logger.info("Запуск GPU сервера...")
        self.startup_attempts += 1
        
        try:
            success = self.gpu_manager.start_instance(wait_for_startup=True)
            if success:
                self.gpu_started_at = datetime.now()
                self.last_activity_time = datetime.now()
                logger.info("✅ GPU сервер успешно запущен")
                
                # Ждем готовности SSH
                if self.gpu_manager.wait_for_ssh_ready():
                    logger.info("✅ SSH подключение готово")
                
                # Задержка для запуска воркеров
                logger.info(f"Ожидание {self.config.startup_delay}с для запуска воркеров...")
                time.sleep(self.config.startup_delay)
                
                # Ждем готовности GPU воркера
                if self.wait_for_gpu_worker_ready():
                    logger.info("✅ GPU воркер готов к обработке задач")
                else:
                    logger.warning("⚠️ GPU воркер не готов, задачи могут быть в очереди")
                
                return True
            else:
                logger.error("❌ Ошибка запуска GPU сервера")
                return False
                
        except Exception as e:
            logger.error(f"❌ Исключение при запуске GPU сервера: {e}")
            return False
    
    def stop_gpu_server(self) -> bool:
        """Остановить GPU сервер"""
        if not self.gpu_manager:
            return False
        
        logger.info("Остановка GPU сервера...")
        
        try:
            success = self.gpu_manager.stop_instance(wait_for_shutdown=True)
            if success:
                self.gpu_started_at = None
                self.startup_attempts = 0  # Сбрасываем счетчик попыток
                logger.info("✅ GPU сервер успешно остановлен")
                return True
            else:
                logger.error("❌ Ошибка остановки GPU сервера")
                return False
                
        except Exception as e:
            logger.error(f"❌ Исключение при остановке GPU сервера: {e}")
            return False
    
    def log_status(self, stats: Dict[str, QueueStats]):
        """Вывести статус мониторинга"""
        gpu_status = "UNKNOWN"
        if self.gpu_manager:
            try:
                gpu_status = "RUNNING" if self.gpu_manager.is_instance_running() else "STOPPED"
            except:
                gpu_status = "ERROR"
        
        logger.info("=" * 50)
        logger.info(f"📊 Статус мониторинга GPU - {datetime.now().strftime('%H:%M:%S')}")
        logger.info(f"🖥️  GPU сервер: {gpu_status}")
        
        if self.gpu_started_at:
            uptime = (datetime.now() - self.gpu_started_at).total_seconds()
            logger.info(f"⏱️  Время работы: {uptime:.0f}с")
        
        total_active = sum(stat.active_tasks for stat in stats.values())
        total_pending = sum(stat.pending_tasks for stat in stats.values())
        
        logger.info(f"📋 Всего GPU задач: {total_active} активных, {total_pending} в очереди")
        
        for queue_name, stat in stats.items():
            logger.info(f"   {queue_name}: {stat.active_tasks} активных, {stat.pending_tasks} в очереди")
        
        idle_time = (datetime.now() - self.last_activity_time).total_seconds()
        logger.info(f"💤 Время простоя: {idle_time:.0f}с")
        logger.info(f"🔄 Попыток запуска: {self.startup_attempts}/{self.config.max_startup_attempts}")
    
    async def monitor_loop(self):
        """Основной цикл мониторинга"""
        logger.info("🚀 Запуск мониторинга GPU задач")
        logger.info(f"📋 Отслеживаемые очереди: {', '.join(self.config.gpu_queues)}")
        logger.info(f"⚙️  Интервал проверки: {self.config.check_interval}с")
        logger.info(f"💤 Максимальное время простоя: {self.config.max_idle_time}с")
        
        while True:
            try:
                # Получаем статистику очередей
                stats = self.get_queue_stats()
                
                # Логируем статус каждые 5 циклов (2.5 минуты при интервале 30с)
                if int(time.time()) % (self.config.check_interval * 5) == 0:
                    self.log_status(stats)
                
                # Принимаем решение о запуске/остановке GPU
                if self.should_start_gpu(stats):
                    logger.info("🚀 Обнаружены GPU задачи, запуск GPU сервера...")
                    self.start_gpu_server()
                
                elif self.should_stop_gpu(stats):
                    idle_time = (datetime.now() - self.last_activity_time).total_seconds()
                    logger.info(f"💤 Нет GPU задач {idle_time:.0f}с, остановка GPU сервера...")
                    self.stop_gpu_server()
                
                # Ждем следующую проверку
                await asyncio.sleep(self.config.check_interval)
                
            except KeyboardInterrupt:
                logger.info("👋 Получен сигнал остановки")
                break
            except Exception as e:
                logger.error(f"❌ Ошибка в цикле мониторинга: {e}")
                await asyncio.sleep(self.config.check_interval)
        
        logger.info("🏁 Мониторинг остановлен")
    
    def run(self):
        """Запустить мониторинг"""
        try:
            asyncio.run(self.monitor_loop())
        except KeyboardInterrupt:
            logger.info("👋 Мониторинг остановлен пользователем")


def main():
    """Основная функция для CLI использования"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Мониторинг GPU задач')
    parser.add_argument('--queues', nargs='+', 
                       default=['embeddings_gpu', 'scoring_tasks'],
                       help='Список GPU очередей для мониторинга')
    parser.add_argument('--min-tasks', type=int, default=1,
                       help='Минимум задач для запуска GPU')
    parser.add_argument('--idle-time', type=int, default=600,
                       help='Максимальное время простоя в секундах')
    parser.add_argument('--check-interval', type=int, default=30,
                       help='Интервал проверки в секундах')
    
    args = parser.parse_args()
    
    # Создаем конфигурацию
    config = GPUMonitorConfig(
        gpu_queues=args.queues,
        min_pending_tasks=args.min_tasks,
        max_idle_time=args.idle_time,
        check_interval=args.check_interval
    )
    
    # Запускаем мониторинг
    monitor = GPUTaskMonitor(config)
    monitor.run()


if __name__ == '__main__':
    main()
