#!/usr/bin/env python3
"""
Скрипт проверки готовности системы после деплоя
Выполняет комплексную диагностику всех компонентов
"""

import sys
import os
import asyncio
import time
import json
from typing import Dict, Any, List
from datetime import datetime

# Добавляем путь к проекту
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Импорты проверяем с обработкой ошибок
try:
    from deployment.common.utils.embedding_quality_test import check_embedding_quality
except ImportError as e:
    print(f"⚠️ Не удалось импортировать embedding_quality_test: {e}")
    check_embedding_quality = None

try:
    from deployment.common.utils.worker_monitor import WorkerHealthMonitor
except ImportError as e:
    print(f"⚠️ Не удалось импортировать worker_monitor: {e}")
    WorkerHealthMonitor = None

try:
    from deployment.common.utils.secret_manager import secret_manager, get_secret
except ImportError as e:
    print(f"⚠️ Не удалось импортировать secret_manager: {e}")
    secret_manager = None
    get_secret = lambda key, default=None: os.getenv(key, default)


class HealthChecker:
    """Класс для проверки здоровья системы"""
    
    def __init__(self):
        self.results = {
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'unknown',
            'checks': {},
            'summary': {
                'total': 0,
                'passed': 0,
                'warnings': 0,
                'failed': 0
            }
        }
    
    async def run_all_checks(self) -> Dict[str, Any]:
        """Выполнение всех проверок здоровья системы"""
        checks = [
            ('secrets', self.check_secrets),
            ('database', self.check_database),
            ('redis', self.check_redis),
            ('workers', self.check_workers),
            ('gpu_quality', self.check_gpu_quality),
            ('queues', self.check_queues),
            ('disk_space', self.check_disk_space),
            ('memory', self.check_memory),
            ('services', self.check_systemd_services),
        ]
        
        print("🔍 Запуск проверки готовности системы...")
        print("=" * 50)
        
        for check_name, check_func in checks:
            try:
                print(f"🔍 Проверка {check_name}...", end=' ')
                result = await check_func()
                self.results['checks'][check_name] = result
                self.results['summary']['total'] += 1
                
                status = result.get('status', 'unknown')
                if status == 'ok':
                    print("✅ OK")
                    self.results['summary']['passed'] += 1
                elif status == 'warning':
                    print("⚠️ WARNING")
                    self.results['summary']['warnings'] += 1
                elif status == 'skip':
                    print("⏭️ SKIP")
                else:
                    print("❌ FAILED")
                    self.results['summary']['failed'] += 1
                
                if result.get('message'):
                    print(f"   💬 {result['message']}")
                    
            except Exception as e:
                print(f"❌ ERROR")
                print(f"   💬 {str(e)}")
                self.results['checks'][check_name] = {
                    'status': 'error',
                    'message': str(e)
                }
                self.results['summary']['total'] += 1
                self.results['summary']['failed'] += 1
        
        # Определяем общий статус
        self._determine_overall_status()
        
        print("=" * 50)
        self._print_summary()
        
        return self.results
    
    def _determine_overall_status(self):
        """Определение общего статуса системы"""
        total = self.results['summary']['total']
        passed = self.results['summary']['passed']
        warnings = self.results['summary']['warnings']
        failed = self.results['summary']['failed']
        
        if failed == 0 and warnings == 0:
            self.results['overall_status'] = 'healthy'
        elif failed == 0 and warnings <= total * 0.2:
            self.results['overall_status'] = 'mostly_healthy'
        elif failed <= total * 0.2:
            self.results['overall_status'] = 'warning'
        else:
            self.results['overall_status'] = 'critical'
    
    def _print_summary(self):
        """Вывод сводки результатов"""
        summary = self.results['summary']
        status = self.results['overall_status']
        
        print(f"📊 СВОДКА ПРОВЕРКИ:")
        print(f"   Всего проверок: {summary['total']}")
        print(f"   Пройдено: {summary['passed']}")
        print(f"   Предупреждения: {summary['warnings']}")
        print(f"   Ошибки: {summary['failed']}")
        print()
        
        status_icons = {
            'healthy': '✅',
            'mostly_healthy': '🟢',
            'warning': '⚠️',
            'critical': '❌'
        }
        
        status_messages = {
            'healthy': 'Система полностью готова',
            'mostly_healthy': 'Система готова с незначительными замечаниями',
            'warning': 'Система частично готова, есть проблемы',
            'critical': 'Система не готова, критические ошибки'
        }
        
        icon = status_icons.get(status, '❓')
        message = status_messages.get(status, 'Неопределенный статус')
        
        print(f"{icon} ОБЩИЙ СТАТУС: {status.upper()}")
        print(f"   {message}")
    
    async def check_secrets(self) -> Dict[str, Any]:
        """Проверка доступности секретов"""
        try:
            if secret_manager is None:
                return {
                    'status': 'warning',
                    'message': 'Secret Manager недоступен, используются env переменные'
                }
            
            if not secret_manager.validate_required_secrets():
                return {
                    'status': 'error',
                    'message': 'Обязательные секреты недоступны'
                }
            
            return {
                'status': 'ok',
                'message': 'Секреты доступны'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    async def check_database(self) -> Dict[str, Any]:
        """Проверка подключения к базе данных"""
        try:
            from database.config import Database
            db = Database()
            
            with db.engine.connect() as conn:
                result = conn.execute("SELECT 1").fetchone()
                if result and result[0] == 1:
                    # Дополнительные проверки
                    version_result = conn.execute("SELECT version()").fetchone()
                    version = version_result[0] if version_result else 'unknown'
                    
                    return {
                        'status': 'ok',
                        'message': 'База данных доступна',
                        'details': {'version': version[:50] + '...'}
                    }
            
            return {
                'status': 'error',
                'message': 'Не удалось выполнить тестовый запрос'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    async def check_redis(self) -> Dict[str, Any]:
        """Проверка подключения к Redis"""
        try:
            import redis
            
            redis_url = get_secret('REDIS_URL', 'redis://localhost:6379/0')
            r = redis.from_url(redis_url)
            
            # Тестовые операции
            r.ping()
            test_key = f'health_check_{int(time.time())}'
            r.set(test_key, 'ok', ex=10)
            value = r.get(test_key)
            r.delete(test_key)
            
            if value == b'ok':
                info = r.info()
                return {
                    'status': 'ok',
                    'message': 'Redis доступен',
                    'details': {
                        'version': info.get('redis_version', 'unknown'),
                        'memory_usage': info.get('used_memory_human', 'unknown')
                    }
                }
            else:
                return {
                    'status': 'error',
                    'message': 'Тестовая операция Redis не удалась'
                }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    async def check_workers(self) -> Dict[str, Any]:
        """Проверка состояния Celery воркеров"""
        try:
            if WorkerHealthMonitor is None:
                return {
                    'status': 'warning',
                    'message': 'Мониторинг воркеров недоступен'
                }
            
            monitor = WorkerHealthMonitor()
            workers = await monitor.check_workers_health()
            
            if not workers:
                return {
                    'status': 'error',
                    'message': 'Воркеры не найдены'
                }
            
            alive_workers = [w for w in workers if w.is_alive]
            
            if len(alive_workers) == len(workers):
                return {
                    'status': 'ok',
                    'message': f'Все воркеры активны ({len(workers)})',
                    'details': {
                        'total_workers': len(workers),
                        'alive_workers': len(alive_workers),
                        'worker_names': [w.name for w in workers]
                    }
                }
            else:
                return {
                    'status': 'warning',
                    'message': f'Активно {len(alive_workers)}/{len(workers)} воркеров',
                    'details': {
                        'total_workers': len(workers),
                        'alive_workers': len(alive_workers)
                    }
                }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    async def check_gpu_quality(self) -> Dict[str, Any]:
        """Проверка качества GPU эмбеддингов"""
        try:
            if not os.getenv('GPU_INSTANCE_NAME'):
                return {
                    'status': 'skip',
                    'message': 'GPU не настроен'
                }
            
            if check_embedding_quality is None:
                return {
                    'status': 'warning',
                    'message': 'Модуль проверки GPU недоступен'
                }
            
            result = check_embedding_quality(timeout=120)
            
            if result['success']:
                quality_score = result['metrics'].get('quality_score', 0)
                if quality_score > 0.7:
                    return {
                        'status': 'ok',
                        'message': f'Качество GPU эмбеддингов хорошее ({quality_score:.2f})',
                        'details': result['metrics']
                    }
                else:
                    return {
                        'status': 'warning',
                        'message': f'Качество GPU эмбеддингов среднее ({quality_score:.2f})',
                        'details': result['metrics']
                    }
            else:
                return {
                    'status': 'error',
                    'message': f'Ошибка тестирования GPU: {result["error"]}'
                }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    async def check_queues(self) -> Dict[str, Any]:
        """Проверка состояния очередей"""
        try:
            if WorkerHealthMonitor is None:
                return {
                    'status': 'warning',
                    'message': 'Мониторинг очередей недоступен'
                }
            
            monitor = WorkerHealthMonitor()
            queues = await monitor.check_queues_health()
            
            if not queues:
                return {
                    'status': 'warning',
                    'message': 'Очереди не найдены'
                }
            
            overloaded = [q for q in queues if q.pending_tasks > 100]
            no_workers = [q for q in queues if q.workers_count == 0]
            
            if overloaded:
                return {
                    'status': 'warning',
                    'message': f'Перегружены очереди: {[q.name for q in overloaded]}',
                    'details': {'total_queues': len(queues)}
                }
            elif no_workers:
                return {
                    'status': 'warning',
                    'message': f'Нет воркеров для очередей: {[q.name for q in no_workers]}',
                    'details': {'total_queues': len(queues)}
                }
            else:
                return {
                    'status': 'ok',
                    'message': f'Все очереди в порядке ({len(queues)})',
                    'details': {
                        'total_queues': len(queues),
                        'queue_names': [q.name for q in queues]
                    }
                }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    async def check_disk_space(self) -> Dict[str, Any]:
        """Проверка свободного места на диске"""
        try:
            import shutil
            
            # Проверяем основные директории
            paths_to_check = [
                '/',
                '/var/log',
                '/tmp',
                '/home/hr-user/hr-analysis'
            ]
            
            warnings = []
            critical = []
            
            for path in paths_to_check:
                if os.path.exists(path):
                    total, used, free = shutil.disk_usage(path)
                    free_percent = (free / total) * 100
                    
                    if free_percent < 5:
                        critical.append(f"{path}: {free_percent:.1f}% свободно")
                    elif free_percent < 15:
                        warnings.append(f"{path}: {free_percent:.1f}% свободно")
            
            if critical:
                return {
                    'status': 'error',
                    'message': f'Критически мало места: {", ".join(critical)}'
                }
            elif warnings:
                return {
                    'status': 'warning',
                    'message': f'Мало места: {", ".join(warnings)}'
                }
            else:
                return {
                    'status': 'ok',
                    'message': 'Достаточно свободного места на диске'
                }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    async def check_memory(self) -> Dict[str, Any]:
        """Проверка использования памяти"""
        try:
            import psutil
            
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            
            if memory_percent > 90:
                return {
                    'status': 'error',
                    'message': f'Критически высокое использование памяти: {memory_percent:.1f}%'
                }
            elif memory_percent > 80:
                return {
                    'status': 'warning',
                    'message': f'Высокое использование памяти: {memory_percent:.1f}%'
                }
            else:
                return {
                    'status': 'ok',
                    'message': f'Использование памяти в норме: {memory_percent:.1f}%',
                    'details': {
                        'total_gb': round(memory.total / (1024**3), 1),
                        'available_gb': round(memory.available / (1024**3), 1)
                    }
                }
        except ImportError:
            return {
                'status': 'warning',
                'message': 'psutil не установлен, проверка памяти недоступна'
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    async def check_systemd_services(self) -> Dict[str, Any]:
        """Проверка статуса systemd сервисов"""
        try:
            import subprocess
            
            services = [
                'hr-celery-cpu',
                'hr-worker-monitor',
                'redis-server',
                'postgresql'
            ]
            
            # Добавляем GPU сервисы если настроены
            if os.getenv('GPU_INSTANCE_NAME'):
                services.extend(['hr-celery-gpu', 'hr-gpu-monitor'])
            
            failed_services = []
            inactive_services = []
            active_services = []
            
            for service in services:
                try:
                    result = subprocess.run(
                        ['systemctl', 'is-active', service],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    
                    if result.returncode == 0 and result.stdout.strip() == 'active':
                        active_services.append(service)
                    else:
                        inactive_services.append(service)
                        
                except subprocess.TimeoutExpired:
                    failed_services.append(f"{service} (timeout)")
                except Exception as e:
                    failed_services.append(f"{service} ({str(e)})")
            
            if failed_services:
                return {
                    'status': 'error',
                    'message': f'Ошибки сервисов: {", ".join(failed_services)}'
                }
            elif inactive_services:
                return {
                    'status': 'warning',
                    'message': f'Неактивные сервисы: {", ".join(inactive_services)}',
                    'details': {'active_services': active_services}
                }
            else:
                return {
                    'status': 'ok',
                    'message': f'Все сервисы активны ({len(active_services)})',
                    'details': {'active_services': active_services}
                }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}


async def main():
    """Главная функция"""
    checker = HealthChecker()
    results = await checker.run_all_checks()
    
    # Сохраняем результаты в файл
    results_file = f"/tmp/health_check_{int(time.time())}.json"
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\n📄 Результаты сохранены в: {results_file}")
    
    # Определяем код выхода
    status = results['overall_status']
    if status == 'healthy':
        sys.exit(0)
    elif status in ['mostly_healthy', 'warning']:
        sys.exit(1)
    else:
        sys.exit(2)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️ Проверка прервана пользователем")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        sys.exit(2)
