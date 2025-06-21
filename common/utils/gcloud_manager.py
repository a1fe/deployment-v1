"""
Утилита для управления GPU инстансом в Google Cloud
Позволяет включать/выключать GPU сервер по требованию
"""

import os
import subprocess
import logging
import time
from typing import Optional, Dict, Any
import json


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GCloudGPUManager:
    """Менеджер для управления GPU инстансом в Google Cloud"""
    
    def __init__(self):
        self.project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
        self.zone = os.getenv('GOOGLE_CLOUD_ZONE', 'us-central1-a')
        self.instance_name = os.getenv('GPU_INSTANCE_NAME')
        
        if not self.project_id:
            raise ValueError("GOOGLE_CLOUD_PROJECT не установлен")
        if not self.instance_name:
            raise ValueError("GPU_INSTANCE_NAME не установлен")
            
        logger.info(f"GPU Manager инициализирован: {self.instance_name} в {self.zone}")
    
    def _run_gcloud_command(self, command: list) -> Dict[str, Any]:
        """Выполнить команду gcloud и вернуть результат"""
        try:
            cmd = ['gcloud'] + command + ['--format=json', f'--project={self.project_id}']
            logger.debug(f"Выполнение команды: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            if result.stdout:
                return json.loads(result.stdout)
            return {}
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Ошибка выполнения команды gcloud: {e}")
            logger.error(f"Stdout: {e.stdout}")
            logger.error(f"Stderr: {e.stderr}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка парсинга JSON ответа: {e}")
            raise
    
    def get_instance_status(self) -> Optional[str]:
        """Получить статус GPU инстанса"""
        try:
            result = self._run_gcloud_command([
                'compute', 'instances', 'describe',
                self.instance_name,
                f'--zone={self.zone}'
            ])
            
            status = result.get('status', 'UNKNOWN')
            logger.info(f"Статус инстанса {self.instance_name}: {status}")
            return status
            
        except subprocess.CalledProcessError as e:
            if 'was not found' in e.stderr:
                logger.warning(f"Инстанс {self.instance_name} не найден")
                return None
            raise
    
    def is_instance_running(self) -> bool:
        """Проверить, запущен ли GPU инстанс"""
        status = self.get_instance_status()
        return status == 'RUNNING'
    
    def start_instance(self, wait_for_startup: bool = True) -> bool:
        """Запустить GPU инстанс"""
        logger.info(f"Запуск GPU инстанса {self.instance_name}...")
        
        if self.is_instance_running():
            logger.info("Инстанс уже запущен")
            return True
        
        try:
            self._run_gcloud_command([
                'compute', 'instances', 'start',
                self.instance_name,
                f'--zone={self.zone}'
            ])
            
            logger.info("Команда запуска отправлена")
            
            if wait_for_startup:
                return self._wait_for_status('RUNNING', timeout=300)  # 5 минут
            
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Ошибка запуска инстанса: {e}")
            return False
    
    def stop_instance(self, wait_for_shutdown: bool = True) -> bool:
        """Остановить GPU инстанс"""
        logger.info(f"Остановка GPU инстанса {self.instance_name}...")
        
        if not self.is_instance_running():
            logger.info("Инстанс уже остановлен")
            return True
        
        try:
            self._run_gcloud_command([
                'compute', 'instances', 'stop',
                self.instance_name,
                f'--zone={self.zone}'
            ])
            
            logger.info("Команда остановки отправлена")
            
            if wait_for_shutdown:
                return self._wait_for_status('TERMINATED', timeout=120)  # 2 минуты
            
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Ошибка остановки инстанса: {e}")
            return False
    
    def _wait_for_status(self, target_status: str, timeout: int = 300) -> bool:
        """Ждать определенного статуса инстанса"""
        logger.info(f"Ожидание статуса {target_status}...")
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            current_status = self.get_instance_status()
            
            if current_status == target_status:
                logger.info(f"Достигнут статус {target_status}")
                return True
            
            logger.info(f"Текущий статус: {current_status}, ожидание...")
            time.sleep(10)
        
        logger.error(f"Таймаут ожидания статуса {target_status}")
        return False
    
    def get_instance_external_ip(self) -> Optional[str]:
        """Получить внешний IP адрес GPU инстанса"""
        try:
            result = self._run_gcloud_command([
                'compute', 'instances', 'describe',
                self.instance_name,
                f'--zone={self.zone}'
            ])
            
            network_interfaces = result.get('networkInterfaces', [])
            if network_interfaces:
                access_configs = network_interfaces[0].get('accessConfigs', [])
                if access_configs:
                    external_ip = access_configs[0].get('natIP')
                    logger.info(f"Внешний IP: {external_ip}")
                    return external_ip
            
            logger.warning("Внешний IP не найден")
            return None
            
        except Exception as e:
            logger.error(f"Ошибка получения IP адреса: {e}")
            return None
    
    def wait_for_ssh_ready(self, timeout: int = 120) -> bool:
        """Ждать готовности SSH подключения"""
        logger.info("Ожидание готовности SSH...")
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                # Простая проверка SSH подключения
                cmd = [
                    'gcloud', 'compute', 'ssh',
                    f'--zone={self.zone}',
                    f'--project={self.project_id}',
                    self.instance_name,
                    '--command=echo "SSH ready"'
                ]
                subprocess.run(
                    cmd,
                    capture_output=True, 
                    check=True, 
                    timeout=30
                )
                
                logger.info("SSH подключение готово")
                return True
                
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                logger.info("SSH еще не готов, ожидание...")
                time.sleep(10)
        
        logger.error("Таймаут ожидания SSH")
        return False
    
    def get_instance_info(self) -> Dict[str, Any]:
        """Получить полную информацию об инстансе"""
        try:
            result = self._run_gcloud_command([
                'compute', 'instances', 'describe',
                self.instance_name,
                f'--zone={self.zone}'
            ])
            
            return {
                'name': result.get('name'),
                'status': result.get('status'),
                'zone': result.get('zone', '').split('/')[-1],
                'machineType': result.get('machineType', '').split('/')[-1],
                'creationTimestamp': result.get('creationTimestamp'),
                'lastStartTimestamp': result.get('lastStartTimestamp'),
                'lastStopTimestamp': result.get('lastStopTimestamp'),
                'externalIP': self.get_instance_external_ip()
            }
            
        except Exception as e:
            logger.error(f"Ошибка получения информации об инстансе: {e}")
            return {}


def main():
    """Основная функция для CLI использования"""
    import sys
    
    if len(sys.argv) < 2:
        print("Использование: python gcloud_manager.py [start|stop|status|info]")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    try:
        manager = GCloudGPUManager()
        
        if command == 'start':
            success = manager.start_instance()
            if success:
                print("✅ GPU инстанс успешно запущен")
                if manager.wait_for_ssh_ready():
                    print("✅ SSH подключение готово")
                    print(f"🌐 Внешний IP: {manager.get_instance_external_ip()}")
                else:
                    print("⚠️ SSH подключение не готово")
            else:
                print("❌ Ошибка запуска GPU инстанса")
                sys.exit(1)
                
        elif command == 'stop':
            success = manager.stop_instance()
            if success:
                print("✅ GPU инстанс успешно остановлен")
            else:
                print("❌ Ошибка остановки GPU инстанса")
                sys.exit(1)
                
        elif command == 'status':
            status = manager.get_instance_status()
            if status:
                print(f"📊 Статус GPU инстанса: {status}")
                if status == 'RUNNING':
                    print(f"🌐 Внешний IP: {manager.get_instance_external_ip()}")
            else:
                print("❌ GPU инстанс не найден")
                
        elif command == 'info':
            info = manager.get_instance_info()
            if info:
                print("📋 Информация о GPU инстансе:")
                for key, value in info.items():
                    print(f"  {key}: {value}")
            else:
                print("❌ Не удалось получить информацию об инстансе")
                
        else:
            print(f"❌ Неизвестная команда: {command}")
            print("Доступные команды: start, stop, status, info")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Ошибка выполнения команды: {e}")
        print(f"❌ Ошибка: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
