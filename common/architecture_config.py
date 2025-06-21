"""
Главный конфигурационный файл для CPU/GPU архитектуры
Определяет поведение системы в зависимости от типа сервера
"""

import os
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass


@dataclass
class ServerConfig:
    """Конфигурация сервера"""
    server_type: str  # 'cpu' или 'gpu'
    is_gpu_enabled: bool
    queues: List[str]
    worker_configs: Dict[str, Dict]
    services: List[str]


class ArchitectureManager:
    """Менеджер архитектуры CPU/GPU"""
    
    def __init__(self):
        self.gpu_instance_name = os.getenv('GPU_INSTANCE_NAME')
        self.is_gpu_server = bool(self.gpu_instance_name)
        self.environment = os.getenv('ENVIRONMENT', 'development')
        
    def get_server_type(self) -> str:
        """Определить тип сервера"""
        return 'gpu' if self.is_gpu_server else 'cpu'
    
    def get_server_config(self) -> ServerConfig:
        """Получить конфигурацию текущего сервера"""
        server_type = self.get_server_type()
        
        if server_type == 'gpu':
            return self._get_gpu_server_config()
        else:
            return self._get_cpu_server_config()
    
    def _get_cpu_server_config(self) -> ServerConfig:
        """Конфигурация CPU сервера"""
        # Очереди для CPU сервера
        queues = ['default', 'fillout', 'search_basic']
        
        # Если GPU не настроен, добавляем GPU задачи в CPU очереди
        if not self.gpu_instance_name:
            queues.extend(['embeddings_cpu', 'scoring_cpu'])
        
        # Конфигурации воркеров
        worker_configs = {
            'default': {
                'concurrency': 2,
                'prefetch_multiplier': 2,
                'max_tasks_per_child': 500,
                'time_limit': 300,
                'soft_time_limit': 240,
                'queues': ['default']
            },
            'fillout': {
                'concurrency': 2,
                'prefetch_multiplier': 1,
                'max_tasks_per_child': 100,
                'time_limit': 180,
                'soft_time_limit': 150,
                'queues': ['fillout']
            },
            'search_basic': {
                'concurrency': 2,
                'prefetch_multiplier': 1,
                'max_tasks_per_child': 100,
                'time_limit': 300,
                'soft_time_limit': 240,
                'queues': ['search_basic']
            }
        }
        
        # Если GPU не настроен, добавляем CPU воркеры для GPU задач
        if not self.gpu_instance_name:
            worker_configs.update({
                'embeddings_cpu': {
                    'concurrency': 1,
                    'prefetch_multiplier': 1,
                    'max_tasks_per_child': 50,
                    'time_limit': 600,
                    'soft_time_limit': 540,
                    'queues': ['embeddings_cpu']
                },
                'scoring_cpu': {
                    'concurrency': 1,
                    'prefetch_multiplier': 1,
                    'max_tasks_per_child': 50,
                    'time_limit': 300,
                    'soft_time_limit': 240,
                    'queues': ['scoring_cpu']
                }
            })
        
        # Сервисы CPU сервера
        services = ['redis', 'postgresql', 'celery-beat']
        if not self.gpu_instance_name:
            services.append('gpu-monitor')  # Только если GPU не настроен
        
        return ServerConfig(
            server_type='cpu',
            is_gpu_enabled=bool(self.gpu_instance_name),
            queues=queues,
            worker_configs=worker_configs,
            services=services
        )
    
    def _get_gpu_server_config(self) -> ServerConfig:
        """Конфигурация GPU сервера"""
        queues = ['embeddings_gpu', 'scoring_tasks', 'default']
        
        worker_configs = {
            'embeddings_gpu': {
                'concurrency': 1,
                'prefetch_multiplier': 1,
                'max_tasks_per_child': 50,
                'time_limit': 600,
                'soft_time_limit': 540,
                'queues': ['embeddings_gpu']
            },
            'scoring_tasks': {
                'concurrency': 1,
                'prefetch_multiplier': 1,
                'max_tasks_per_child': 50,
                'time_limit': 300,
                'soft_time_limit': 240,
                'queues': ['scoring_tasks']
            },
            'default_gpu': {
                'concurrency': 1,
                'prefetch_multiplier': 1,
                'max_tasks_per_child': 100,
                'time_limit': 300,
                'soft_time_limit': 240,
                'queues': ['default']
            }
        }
        
        services = ['celery-workers']
        
        return ServerConfig(
            server_type='gpu',
            is_gpu_enabled=True,
            queues=queues,
            worker_configs=worker_configs,
            services=services
        )
    
    def get_recommended_workers(self) -> List[Tuple[str, Dict]]:
        """Получить рекомендуемые воркеры для запуска"""
        config = self.get_server_config()
        
        workers = []
        for worker_name, worker_config in config.worker_configs.items():
            workers.append((worker_name, worker_config))
        
        return workers
    
    def get_systemd_services(self) -> List[str]:
        """Получить список systemd сервисов для установки"""
        config = self.get_server_config()
        
        service_mapping = {
            'redis': 'redis-server',
            'postgresql': 'postgresql',
            'celery-beat': 'hr-celery-beat',
            'celery-workers': 'hr-celery-workers',
            'gpu-monitor': 'hr-gpu-monitor'
        }
        
        return [service_mapping[service] for service in config.services if service in service_mapping]
    
    def validate_environment(self) -> Tuple[bool, List[str]]:
        """Проверить корректность настройки окружения"""
        errors = []
        
        # Общие проверки
        required_vars = ['REDIS_URL', 'DATABASE_URL']
        for var in required_vars:
            if not os.getenv(var):
                errors.append(f"Отсутствует переменная окружения: {var}")
        
        # Проверки для GPU сервера
        if self.is_gpu_server:
            if not os.getenv('CUDA_VISIBLE_DEVICES'):
                errors.append("GPU сервер: отсутствует CUDA_VISIBLE_DEVICES")
                
            # Проверка доступности GPU пакетов
            try:
                import torch
                if not torch.cuda.is_available():
                    errors.append("GPU сервер: CUDA недоступна в PyTorch")
            except ImportError:
                errors.append("GPU сервер: PyTorch не установлен")
                
            try:
                import sentence_transformers
            except ImportError:
                errors.append("GPU сервер: sentence-transformers не установлен")
        
        # Проверки для CPU сервера
        else:
            if os.getenv('CUDA_VISIBLE_DEVICES'):
                errors.append("CPU сервер: обнаружена CUDA_VISIBLE_DEVICES (должна быть только на GPU сервере)")
        
        return len(errors) == 0, errors
    
    def get_startup_command(self) -> str:
        """Получить команду запуска для текущего сервера"""
        server_type = self.get_server_type()
        
        if server_type == 'gpu':
            return "./deployment/gpu-server/start_gpu_celery.sh"
        else:
            return "./deployment/cpu-server/start_cpu_celery.sh"
    
    def get_monitoring_config(self) -> Dict:
        """Получить конфигурацию мониторинга"""
        if self.is_gpu_server:
            return {
                'enabled': False,
                'reason': 'GPU сервер не запускает мониторинг'
            }
        
        if not self.gpu_instance_name:
            return {
                'enabled': False,
                'reason': 'GPU инстанс не настроен, автоматическое управление отключено'
            }
        
        return {
            'enabled': True,
            'gpu_queues': ['embeddings_gpu', 'scoring_tasks'],
            'check_interval': 30,
            'max_idle_time': 600,
            'min_pending_tasks': 1
        }
    
    def print_configuration_summary(self):
        """Вывести сводку конфигурации"""
        config = self.get_server_config()
        is_valid, errors = self.validate_environment()
        
        print("=" * 60)
        print("🏗️  КОНФИГУРАЦИЯ АРХИТЕКТУРЫ CPU/GPU")
        print("=" * 60)
        print(f"🖥️  Тип сервера: {config.server_type.upper()}")
        print(f"🎯 GPU включен: {'✅ Да' if config.is_gpu_enabled else '❌ Нет'}")
        print(f"🌍 Окружение: {self.environment}")
        
        if self.gpu_instance_name:
            print(f"🚀 GPU инстанс: {self.gpu_instance_name}")
        
        print(f"\n📋 Очереди для обработки:")
        for queue in config.queues:
            print(f"   - {queue}")
        
        print(f"\n⚙️  Воркеры для запуска:")
        for worker_name, worker_config in config.worker_configs.items():
            queues_str = ', '.join(worker_config['queues'])
            print(f"   - {worker_name}: {queues_str} (concurrency: {worker_config['concurrency']})")
        
        print(f"\n🔧 Сервисы:")
        for service in config.services:
            print(f"   - {service}")
        
        startup_cmd = self.get_startup_command()
        print(f"\n🚀 Команда запуска:")
        print(f"   {startup_cmd}")
        
        monitoring = self.get_monitoring_config()
        print(f"\n📊 Мониторинг GPU:")
        if monitoring['enabled']:
            print("   ✅ Включен")
            print(f"   📋 Очереди: {', '.join(monitoring['gpu_queues'])}")
            print(f"   ⏱️  Интервал: {monitoring['check_interval']}с")
            print(f"   💤 Время простоя: {monitoring['max_idle_time']}с")
        else:
            print(f"   ❌ Отключен: {monitoring['reason']}")
        
        print(f"\n✅ Статус конфигурации:")
        if is_valid:
            print("   🟢 Конфигурация корректна")
        else:
            print("   🔴 Обнаружены ошибки:")
            for error in errors:
                print(f"      - {error}")
        
        print("=" * 60)


def main():
    """Главная функция для CLI использования"""
    import sys
    
    manager = ArchitectureManager()
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == 'config':
            manager.print_configuration_summary()
        
        elif command == 'validate':
            is_valid, errors = manager.validate_environment()
            if is_valid:
                print("✅ Конфигурация корректна")
                sys.exit(0)
            else:
                print("❌ Ошибки конфигурации:")
                for error in errors:
                    print(f"  - {error}")
                sys.exit(1)
        
        elif command == 'workers':
            workers = manager.get_recommended_workers()
            print("Рекомендуемые воркеры:")
            for worker_name, config in workers:
                queues = ', '.join(config['queues'])
                print(f"  {worker_name}: {queues} (concurrency: {config['concurrency']})")
        
        elif command == 'startup':
            print(manager.get_startup_command())
        
        else:
            print(f"Неизвестная команда: {command}")
            print("Доступные команды: config, validate, workers, startup")
            sys.exit(1)
    else:
        manager.print_configuration_summary()


if __name__ == '__main__':
    main()
