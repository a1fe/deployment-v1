"""
Безопасный запуск процессов и команд
"""

import subprocess
import logging
import os
import shlex
from typing import List, Optional, Dict, Any, Union
from pathlib import Path

logger = logging.getLogger(__name__)


class ProcessExecutor:
    """Безопасный исполнитель процессов"""
    
    def __init__(self):
        self.allowed_commands = {
            'bash', 'sh', 'python', 'python3', 'celery', 'docker', 'docker-compose',
            'systemctl', 'supervisorctl', 'nginx', 'gunicorn', 'redis-cli'
        }
        self.forbidden_patterns = {
            ';', '&&', '||', '|', '>', '<', '`', '$(',
            'rm -rf', 'sudo rm', 'chmod 777', 'chown -R',
            'wget', 'curl -o', 'nc -', 'telnet'
        }
    
    def validate_command(self, command: Union[str, List[str]]) -> bool:
        """
        Валидация команды на безопасность
        
        Args:
            command: Команда для валидации
            
        Returns:
            True если команда безопасна
        """
        if isinstance(command, list):
            command_str = ' '.join(command)
            base_command = command[0] if command else ''
        else:
            command_str = command
            base_command = command.split()[0] if command else ''
        
        # Проверка разрешенных команд
        if base_command not in self.allowed_commands:
            logger.error(f"❌ Команда '{base_command}' не разрешена")
            return False
        
        # Проверка запрещенных паттернов
        for pattern in self.forbidden_patterns:
            if pattern in command_str:
                logger.error(f"❌ Обнаружен запрещенный паттерн '{pattern}' в команде")
                return False
        
        return True
    
    def execute_command(
        self, 
        command: Union[str, List[str]], 
        background: bool = False,
        timeout: int = 300,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Безопасное выполнение команды
        
        Args:
            command: Команда для выполнения
            background: Запустить в фоне
            timeout: Таймаут выполнения в секундах
            cwd: Рабочая директория
            env: Переменные окружения
            
        Returns:
            Результат выполнения команды
        """
        # Валидация команды
        if not self.validate_command(command):
            return {
                'success': False,
                'error': 'Command validation failed',
                'returncode': -1
            }
        
        # Подготовка команды
        if isinstance(command, str):
            # Используем shlex для безопасного разбора строки
            command_list = shlex.split(command)
        else:
            command_list = command
        
        # Подготовка окружения
        process_env = os.environ.copy()
        if env:
            process_env.update(env)
        
        try:
            logger.info(f"🚀 Выполнение команды: {' '.join(command_list)}")
            
            if background:
                # Запуск в фоне
                process = subprocess.Popen(
                    command_list,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=cwd,
                    env=process_env,
                    start_new_session=True  # Отделяем от родительского процесса
                )
                
                return {
                    'success': True,
                    'pid': process.pid,
                    'process': process,
                    'background': True,
                    'command': ' '.join(command_list)
                }
            else:
                # Синхронное выполнение
                result = subprocess.run(
                    command_list,
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                    cwd=cwd,
                    env=process_env
                )
                
                success = result.returncode == 0
                if success:
                    logger.info(f"✅ Команда выполнена успешно")
                else:
                    logger.error(f"❌ Команда завершилась с ошибкой: {result.stderr}")
                
                return {
                    'success': success,
                    'returncode': result.returncode,
                    'stdout': result.stdout,
                    'stderr': result.stderr,
                    'command': ' '.join(command_list)
                }
                
        except subprocess.TimeoutExpired:
            logger.error(f"❌ Таймаут выполнения команды: {timeout}s")
            return {
                'success': False,
                'error': f'Command timeout after {timeout} seconds',
                'returncode': -1
            }
        except Exception as e:
            logger.error(f"❌ Ошибка выполнения команды: {e}")
            return {
                'success': False,
                'error': str(e),
                'returncode': -1
            }
    
    def start_service(self, service_name: str, service_type: str = 'systemctl') -> Dict[str, Any]:
        """
        Безопасный запуск системного сервиса
        
        Args:
            service_name: Имя сервиса
            service_type: Тип сервис-менеджера (systemctl, supervisorctl)
            
        Returns:
            Результат запуска сервиса
        """
        if service_type == 'systemctl':
            command = ['systemctl', 'start', service_name]
        elif service_type == 'supervisorctl':
            command = ['supervisorctl', 'start', service_name]
        else:
            return {
                'success': False,
                'error': f'Unsupported service type: {service_type}'
            }
        
        return self.execute_command(command)
    
    def start_docker_compose(self, compose_file: str, service: Optional[str] = None) -> Dict[str, Any]:
        """
        Безопасный запуск Docker Compose
        
        Args:
            compose_file: Путь к docker-compose.yml
            service: Конкретный сервис для запуска
            
        Returns:
            Результат запуска
        """
        # Валидация пути к файлу
        compose_path = Path(compose_file)
        if not compose_path.exists():
            return {
                'success': False,
                'error': f'Docker compose file not found: {compose_file}'
            }
        
        # Проверка, что файл находится в разрешенной директории
        allowed_dirs = [
            '/opt/hr-analysis',
            str(Path.cwd()),  # Текущая директория
            '/home/hr-user/hr-analysis'
        ]
        
        if not any(str(compose_path).startswith(allowed_dir) for allowed_dir in allowed_dirs):
            return {
                'success': False,
                'error': f'Docker compose file not in allowed directory: {compose_file}'
            }
        
        # Формирование команды
        command = ['docker-compose', '-f', str(compose_path), 'up', '-d']
        if service:
            command.append(service)
        
        return self.execute_command(command, background=True, cwd=str(compose_path.parent))
    
    def check_process_status(self, pid: int) -> Dict[str, Any]:
        """
        Проверка статуса процесса по PID
        
        Args:
            pid: ID процесса
            
        Returns:
            Информация о статусе процесса
        """
        try:
            # Проверяем существование процесса
            os.kill(pid, 0)  # Не убивает процесс, только проверяет существование
            
            # Получаем дополнительную информацию через ps
            result = subprocess.run(
                ['ps', '-p', str(pid), '-o', 'pid,ppid,command'],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                return {
                    'running': True,
                    'pid': pid,
                    'info': result.stdout.strip()
                }
            else:
                return {
                    'running': False,
                    'pid': pid,
                    'error': 'Process not found'
                }
                
        except ProcessLookupError:
            return {
                'running': False,
                'pid': pid,
                'error': 'Process not found'
            }
        except Exception as e:
            return {
                'running': False,
                'pid': pid,
                'error': str(e)
            }


# Глобальный экземпляр исполнителя
process_executor = ProcessExecutor()


def safe_execute(command: Union[str, List[str]], **kwargs) -> Dict[str, Any]:
    """
    Удобная функция для безопасного выполнения команд
    
    Args:
        command: Команда для выполнения
        **kwargs: Дополнительные параметры для execute_command
        
    Returns:
        Результат выполнения
    """
    return process_executor.execute_command(command, **kwargs)


def start_background_service(command: Union[str, List[str]], **kwargs) -> Dict[str, Any]:
    """
    Запуск сервиса в фоне
    
    Args:
        command: Команда для запуска
        **kwargs: Дополнительные параметры
        
    Returns:
        Результат запуска с PID процесса
    """
    return process_executor.execute_command(command, background=True, **kwargs)
