"""
Система мониторинга здоровья Celery воркеров с алертами
Отслеживает состояние воркеров, очередей и производительность
"""

import asyncio
import json
import time
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from celery import Celery
from deployment.common.utils.logging_config import get_logger
from deployment.common.utils.secret_manager import get_secret

logger = get_logger('worker_monitor')


@dataclass
class WorkerStatus:
    """Статус воркера"""
    name: str
    hostname: str
    queues: List[str]
    active_tasks: int
    processed_tasks: int
    load_avg: List[float]
    last_heartbeat: datetime
    is_alive: bool
    memory_usage: Optional[float] = None
    cpu_usage: Optional[float] = None


@dataclass
class QueueStatus:
    """Статус очереди"""
    name: str
    pending_tasks: int
    active_tasks: int
    completed_tasks: int
    failed_tasks: int
    avg_processing_time: float
    workers_count: int


@dataclass
class Alert:
    """Алерт о проблеме"""
    type: str
    severity: str  # 'low', 'medium', 'high', 'critical'
    message: str
    timestamp: datetime
    component: str
    details: Dict[str, Any]


class WorkerHealthMonitor:
    """Мониторинг здоровья воркеров"""
    
    def __init__(self, celery_app: Optional[Celery] = None):
        self.celery_app = celery_app or self._get_celery_app()
        self.alerts: List[Alert] = []
        self.last_check_time = datetime.now()
        self.check_interval = 30  # секунд
        self.alert_thresholds = self._load_alert_thresholds()
        self.email_config = self._load_email_config()
        
    def _get_celery_app(self) -> Celery:
        """Получение экземпляра Celery приложения"""
        try:
            from deployment.common.celery_app.celery_app import get_celery_app
            return get_celery_app()
        except ImportError:
            logger.error("❌ Не удалось импортировать Celery приложение")
            raise
    
    def _load_alert_thresholds(self) -> Dict[str, Any]:
        """Загрузка порогов для алертов"""
        return {
            'worker_down_timeout': 60,  # секунд без heartbeat
            'high_queue_threshold': 100,  # задач в очереди
            'critical_queue_threshold': 500,
            'memory_threshold': 85,  # процент использования памяти
            'cpu_threshold': 90,  # процент использования CPU
            'processing_time_threshold': 300,  # секунд на задачу
            'failed_tasks_threshold': 10,  # процент неудачных задач
        }
    
    def _load_email_config(self) -> Dict[str, Any]:
        """Загрузка конфигурации email для алертов"""
        recipients_str = get_secret('ALERT_RECIPIENTS', '') or ''
        return {
            'smtp_server': get_secret('SMTP_SERVER', 'smtp.gmail.com') or 'smtp.gmail.com',
            'smtp_port': get_secret('SMTP_PORT', '587') or '587',
            'smtp_username': get_secret('SMTP_USERNAME', '') or '',
            'smtp_password': get_secret('SMTP_PASSWORD', '') or '',
            'alert_recipients': [r.strip() for r in recipients_str.split(',') if r.strip()],
            'from_email': get_secret('ALERT_FROM_EMAIL', '') or '',
        }
    
    async def monitor_loop(self):
        """Основной цикл мониторинга"""
        logger.info("🔄 Запуск цикла мониторинга воркеров...")
        
        while True:
            try:
                # Проверка воркеров
                workers_status = await self.check_workers_health()
                
                # Проверка очередей
                queues_status = await self.check_queues_health()
                
                # Анализ и генерация алертов
                await self.analyze_and_alert(workers_status, queues_status)
                
                # Сохранение метрик
                await self.save_metrics(workers_status, queues_status)
                
                self.last_check_time = datetime.now()
                
            except Exception as e:
                logger.error(f"❌ Ошибка в цикле мониторинга: {e}")
                # Создаем алерт о проблеме самого мониторинга
                await self.create_alert(
                    'monitor_error',
                    'high',
                    f"Ошибка в системе мониторинга: {str(e)}",
                    'monitor',
                    {'error': str(e)}
                )
            
            await asyncio.sleep(self.check_interval)
    
    async def check_workers_health(self) -> List[WorkerStatus]:
        """Проверка здоровья всех воркеров"""
        workers_status = []
        
        try:
            # Получаем информацию о воркерах
            inspect = self.celery_app.control.inspect()
            
            # Активные воркеры
            active_workers = inspect.active() or {}
            
            # Статистика воркеров
            stats = inspect.stats() or {}
            
            # Зарегистрированные воркеры
            registered = inspect.registered() or {}
            
            current_time = datetime.now()
            
            for worker_name, worker_stats in stats.items():
                # Извлекаем информацию о воркере
                hostname = worker_stats.get('hostname', worker_name)
                
                # Активные задачи
                active_tasks = len(active_workers.get(worker_name, []))
                
                # Обработанные задачи
                processed_tasks = worker_stats.get('total', {}).get('tasks.processed', 0)
                
                # Load average
                load_avg = worker_stats.get('rusage', {}).get('load_avg', [0, 0, 0])
                
                # Последний heartbeat
                last_heartbeat = current_time  # Упрощенная версия
                
                # Очереди воркера
                worker_queues = []
                if worker_name in registered:
                    for task_name, task_info in registered[worker_name].items():
                        # Извлекаем информацию об очередях из конфигурации
                        pass
                
                # Статус воркера
                worker_status = WorkerStatus(
                    name=worker_name,
                    hostname=hostname,
                    queues=worker_queues,
                    active_tasks=active_tasks,
                    processed_tasks=processed_tasks,
                    load_avg=load_avg,
                    last_heartbeat=last_heartbeat,
                    is_alive=True,
                )
                
                workers_status.append(worker_status)
            
            logger.info(f"✅ Проверено {len(workers_status)} воркеров")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке воркеров: {e}")
        
        return workers_status
    
    async def check_queues_health(self) -> List[QueueStatus]:
        """Проверка здоровья очередей"""
        queues_status = []
        
        try:
            inspect = self.celery_app.control.inspect()
            
            # Получаем информацию об очередях
            active_queues = inspect.active_queues() or {}
            reserved_tasks = inspect.reserved() or {}
            
            # Список всех очередей
            all_queues = set()
            for worker_queues in active_queues.values():
                for queue_info in worker_queues:
                    all_queues.add(queue_info['name'])
            
            for queue_name in all_queues:
                # Подсчет задач в очереди
                pending_tasks = 0
                active_tasks = 0
                workers_count = 0
                
                for worker_name, worker_queues in active_queues.items():
                    for queue_info in worker_queues:
                        if queue_info['name'] == queue_name:
                            workers_count += 1
                            # Здесь можно добавить подсчет задач
                
                # Для Redis broker можем получить длину очереди
                try:
                    with self.celery_app.connection() as connection:
                        # Упрощенная версия - в реальности нужно подключиться к Redis
                        pass
                except:
                    pass
                
                queue_status = QueueStatus(
                    name=queue_name,
                    pending_tasks=pending_tasks,
                    active_tasks=active_tasks,
                    completed_tasks=0,  # Требует дополнительной логики
                    failed_tasks=0,     # Требует дополнительной логики
                    avg_processing_time=0.0,  # Требует дополнительной логики
                    workers_count=workers_count
                )
                
                queues_status.append(queue_status)
            
            logger.info(f"✅ Проверено {len(queues_status)} очередей")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке очередей: {e}")
        
        return queues_status
    
    async def analyze_and_alert(self, workers: List[WorkerStatus], queues: List[QueueStatus]):
        """Анализ состояния и генерация алертов"""
        current_time = datetime.now()
        
        # Проверка воркеров
        for worker in workers:
            # Проверка на недоступность воркера
            time_since_heartbeat = (current_time - worker.last_heartbeat).total_seconds()
            if time_since_heartbeat > self.alert_thresholds['worker_down_timeout']:
                await self.create_alert(
                    'worker_down',
                    'critical',
                    f"Воркер {worker.name} не отвечает {time_since_heartbeat:.1f} секунд",
                    worker.name,
                    {'last_heartbeat': worker.last_heartbeat.isoformat()}
                )
            
            # Проверка использования памяти
            if worker.memory_usage and worker.memory_usage > self.alert_thresholds['memory_threshold']:
                await self.create_alert(
                    'high_memory',
                    'medium',
                    f"Высокое использование памяти воркером {worker.name}: {worker.memory_usage:.1f}%",
                    worker.name,
                    {'memory_usage': worker.memory_usage}
                )
            
            # Проверка использования CPU
            if worker.cpu_usage and worker.cpu_usage > self.alert_thresholds['cpu_threshold']:
                await self.create_alert(
                    'high_cpu',
                    'medium',
                    f"Высокое использование CPU воркером {worker.name}: {worker.cpu_usage:.1f}%",
                    worker.name,
                    {'cpu_usage': worker.cpu_usage}
                )
        
        # Проверка очередей
        for queue in queues:
            # Проверка переполнения очереди
            if queue.pending_tasks > self.alert_thresholds['critical_queue_threshold']:
                await self.create_alert(
                    'queue_overflow',
                    'critical',
                    f"Критическое переполнение очереди {queue.name}: {queue.pending_tasks} задач",
                    queue.name,
                    {'pending_tasks': queue.pending_tasks}
                )
            elif queue.pending_tasks > self.alert_thresholds['high_queue_threshold']:
                await self.create_alert(
                    'queue_high',
                    'medium',
                    f"Высокая загрузка очереди {queue.name}: {queue.pending_tasks} задач",
                    queue.name,
                    {'pending_tasks': queue.pending_tasks}
                )
            
            # Проверка отсутствия воркеров для очереди
            if queue.workers_count == 0 and queue.pending_tasks > 0:
                await self.create_alert(
                    'no_workers',
                    'high',
                    f"Нет воркеров для очереди {queue.name} с {queue.pending_tasks} задачами",
                    queue.name,
                    {'pending_tasks': queue.pending_tasks}
                )
    
    async def create_alert(self, alert_type: str, severity: str, message: str, component: str, details: Dict[str, Any]):
        """Создание алерта"""
        alert = Alert(
            type=alert_type,
            severity=severity,
            message=message,
            timestamp=datetime.now(),
            component=component,
            details=details
        )
        
        self.alerts.append(alert)
        logger.warning(f"🚨 Алерт [{severity.upper()}] {alert_type}: {message}")
        
        # Отправка уведомления
        if severity in ['high', 'critical']:
            await self.send_alert_notification(alert)
    
    async def send_alert_notification(self, alert: Alert):
        """Отправка уведомления об алерте"""
        try:
            if not self.email_config['smtp_username'] or not self.email_config['alert_recipients']:
                logger.warning("⚠️ Email конфигурация не настроена, пропускаем отправку алерта")
                return
            
            # Формируем email
            subject = f"[HR Analysis] Алерт {alert.severity.upper()}: {alert.type}"
            
            body = f"""
Время: {alert.timestamp.isoformat()}
Тип: {alert.type}
Серьезность: {alert.severity.upper()}
Компонент: {alert.component}
Сообщение: {alert.message}

Детали:
{json.dumps(alert.details, indent=2, ensure_ascii=False)}

---
Система мониторинга HR Analysis
            """.strip()
            
            # Отправляем email
            msg = MIMEMultipart()
            msg['From'] = self.email_config['from_email'] or self.email_config['smtp_username']
            msg['To'] = ', '.join(self.email_config['alert_recipients'])
            msg['Subject'] = subject
            
            msg.attach(MIMEText(body, 'plain', 'utf-8'))
            
            with smtplib.SMTP(self.email_config['smtp_server'], int(self.email_config['smtp_port'])) as server:
                server.starttls()
                server.login(self.email_config['smtp_username'], self.email_config['smtp_password'])
                server.send_message(msg)
            
            logger.info(f"📧 Алерт отправлен на {len(self.email_config['alert_recipients'])} получателей")
            
        except Exception as e:
            logger.error(f"❌ Ошибка отправки алерта: {e}")
    
    async def save_metrics(self, workers: List[WorkerStatus], queues: List[QueueStatus]):
        """Сохранение метрик для истории"""
        try:
            metrics_dir = Path('./logs/metrics')
            metrics_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().isoformat()
            
            # Сохраняем метрики воркеров
            workers_data = {
                'timestamp': timestamp,
                'workers': [asdict(worker) for worker in workers]
            }
            
            with open(metrics_dir / f'workers_{datetime.now().strftime("%Y%m%d")}.jsonl', 'a') as f:
                f.write(json.dumps(workers_data, ensure_ascii=False, default=str) + '\n')
            
            # Сохраняем метрики очередей
            queues_data = {
                'timestamp': timestamp,
                'queues': [asdict(queue) for queue in queues]
            }
            
            with open(metrics_dir / f'queues_{datetime.now().strftime("%Y%m%d")}.jsonl', 'a') as f:
                f.write(json.dumps(queues_data, ensure_ascii=False, default=str) + '\n')
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения метрик: {e}")
    
    def get_recent_alerts(self, hours: int = 24) -> List[Alert]:
        """Получение недавних алертов"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        return [alert for alert in self.alerts if alert.timestamp > cutoff_time]
    
    def get_health_summary(self) -> Dict[str, Any]:
        """Получение сводки о здоровье системы"""
        recent_alerts = self.get_recent_alerts()
        
        return {
            'monitoring_active': True,
            'last_check': self.last_check_time.isoformat(),
            'total_alerts_24h': len(recent_alerts),
            'critical_alerts_24h': len([a for a in recent_alerts if a.severity == 'critical']),
            'high_alerts_24h': len([a for a in recent_alerts if a.severity == 'high']),
            'recent_alerts': [asdict(alert) for alert in recent_alerts[-10:]]  # Последние 10
        }


async def main():
    """Основная функция для запуска мониторинга"""
    logger.info("🚀 Запуск системы мониторинга воркеров...")
    
    monitor = WorkerHealthMonitor()
    
    try:
        await monitor.monitor_loop()
    except KeyboardInterrupt:
        logger.info("⏹️ Мониторинг остановлен пользователем")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка мониторинга: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
