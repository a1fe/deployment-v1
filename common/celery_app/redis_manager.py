"""
Unified Redis management for HR Analysis Celery system
"""

import os
import subprocess
import time
import logging
import re
from pathlib import Path
from typing import Union, Optional, Tuple
from functools import lru_cache
import redis

logger = logging.getLogger(__name__)


class RedisManager:
    """Unified Redis management for Celery system"""
    
    def __init__(self):
        self.host = os.getenv('REDIS_HOST', 'localhost')
        self.port = int(os.getenv('REDIS_PORT', '6379'))
        self.db = int(os.getenv('REDIS_DB', '0'))
        self.password = os.getenv('REDIS_PASSWORD', None)
        self._connection = None
        
        # ИСПРАВЛЕНИЕ БАГА: Проверка пароля для production
        environment = os.getenv('ENVIRONMENT', 'development')
        if environment == 'production' and not self.password:
            logger.warning("⚠️ Redis работает без пароля в production! Это небезопасно.")
            logger.warning("   Установите REDIS_PASSWORD в переменных окружения.")
        elif not self.password:
            logger.warning("⚠️ Redis работает без пароля! Это небезопасно для production.")
            logger.warning("   Установите REDIS_PASSWORD в переменных окружения.")
    
    def _get_connection(self) -> redis.Redis:
        """Get Redis connection instance with improved error handling"""
        if self._connection is None:
            try:
                self._connection = redis.Redis(
                    host=self.host,
                    port=self.port,
                    db=self.db,
                    password=self.password,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    retry_on_timeout=True,
                    health_check_interval=30  # ИСПРАВЛЕНИЕ: health check
                )
                # ИСПРАВЛЕНИЕ БАГА: Тестируем соединение при создании
                self._connection.ping()
                logger.info(f"✅ Redis connection established: {self.host}:{self.port}/{self.db}")
            except redis.AuthenticationError as e:
                logger.error(f"❌ Redis authentication failed: {e}")
                raise ConnectionError(f"Redis authentication failed - check REDIS_PASSWORD")
            except redis.ConnectionError as e:
                logger.error(f"❌ Failed to connect to Redis: {e}")
                raise ConnectionError(f"Cannot connect to Redis at {self.host}:{self.port} - {e}")
            except Exception as e:
                logger.error(f"❌ Unexpected Redis error: {e}")
                raise ConnectionError(f"Unexpected Redis error: {e}")
        return self._connection
    
    def check_connection(self) -> bool:
        """Check Redis connection health with detailed logging"""
        try:
            connection = self._get_connection()
            connection.ping()
            logger.info("✅ Redis connection healthy")
            return True
        except redis.ConnectionError as e:
            logger.error(f"❌ Redis connection failed: {e}")
            return False
        except redis.TimeoutError as e:
            logger.error(f"❌ Redis timeout: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Redis error: {type(e).__name__}: {e}")
            return False
    
    def _run_command(self, command: str, background: bool = False, 
                    cwd: Optional[Union[str, Path]] = None) -> Union[subprocess.Popen, subprocess.CompletedProcess]:
        """Execute shell command with proper error handling"""
        if cwd is None:
            cwd = Path(__file__).parent.parent
        
        logger.debug(f"Executing: {command}")
        
        try:
            if background:
                return subprocess.Popen(
                    command,
                    shell=True,
                    cwd=cwd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    start_new_session=True  # Detach from parent process
                )
            else:
                return subprocess.run(
                    command,
                    shell=True,
                    cwd=cwd,
                    capture_output=True,
                    text=True,
                    timeout=30  # Add timeout to prevent hanging
                )
        except subprocess.TimeoutExpired:
            logger.error(f"Command timed out: {command}")
            raise
        except Exception as e:
            logger.error(f"Command execution failed: {command} - {e}")
            raise
    
    def start_server(self, wait_timeout: int = 30) -> bool:
        """Start Redis server with intelligent waiting and health checks"""
        if self.check_connection():
            logger.info("✅ Redis is already running")
            return True
        
        logger.info("🚀 Starting Redis server...")
        
        try:
            # Start Redis in background
            proc = self._run_command("redis-server", background=True)
            
            # Wait for Redis to be ready with exponential backoff
            wait_intervals = [0.5, 1, 1, 2, 2, 3, 3, 5, 5, 5]  # Total ~27 seconds
            
            for i, interval in enumerate(wait_intervals):
                if i >= wait_timeout:
                    break
                    
                time.sleep(interval)
                if self.check_connection():
                    logger.info(f"✅ Redis started successfully (attempt {i + 1})")
                    return True
            
            logger.error(f"❌ Redis failed to start within {wait_timeout} seconds")
            
            # Try to terminate the process if it's still running (only for Popen objects)
            if isinstance(proc, subprocess.Popen):
                if proc.poll() is None:
                    proc.terminate()
                    time.sleep(2)
                    if proc.poll() is None:
                        proc.kill()
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error starting Redis: {e}")
            return False
    
    def stop_server(self) -> bool:
        """Stop Redis server gracefully"""
        logger.info("🛑 Stopping Redis server...")
        
        if not self.check_connection():
            logger.warning("⚠️ Redis is not running")
            return True
        
        try:
            result = self._run_command("redis-cli shutdown")
            
            if result.returncode == 0:
                # Verify Redis is actually stopped
                time.sleep(1)
                if not self.check_connection():
                    logger.info("✅ Redis stopped successfully")
                    return True
                else:
                    logger.warning("⚠️ Redis shutdown command succeeded but server still responding")
                    return False
            else:
                logger.error(f"❌ Redis shutdown failed: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error stopping Redis: {e}")
            return False
    
    def restart_server(self) -> bool:
        """Restart Redis server"""
        logger.info("🔄 Restarting Redis server...")
        
        if self.stop_server():
            time.sleep(2)  # Give some time for cleanup
            return self.start_server()
        
        logger.error("❌ Failed to restart Redis - stop operation failed")
        return False
    
    def get_server_info(self) -> dict:
        """Get Redis server information"""
        try:
            connection = self._get_connection()
            info = connection.info()
            
            # Handle potential async redis client
            if not isinstance(info, dict):
                logger.warning("Redis info() did not return dict, using basic info")
                return {'status': 'connected', 'db_keys': connection.dbsize()}
            
            return {
                'version': info.get('redis_version', 'unknown'),
                'uptime': info.get('uptime_in_seconds', 0),
                'connected_clients': info.get('connected_clients', 0),
                'used_memory': info.get('used_memory_human', 'unknown'),
                'role': info.get('role', 'unknown'),
                'db_keys': connection.dbsize()
            }
        except Exception as e:
            logger.error(f"❌ Failed to get Redis info: {e}")
            return {}
    
    def flush_db(self, confirm: bool = False) -> bool:
        """Flush Redis database (requires confirmation)"""
        if not confirm:
            logger.warning("⚠️ Database flush requires explicit confirmation")
            return False
        
        try:
            connection = self._get_connection()
            connection.flushdb()
            logger.info("✅ Redis database flushed")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to flush Redis database: {e}")
            return False
    
    def get_connection_string(self, mask_password: bool = True) -> str:
        """Get Redis connection string for logging"""
        if self.password and mask_password:
            return f"redis://***@{self.host}:{self.port}/{self.db}"
        elif self.password:
            return f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
        else:
            return f"redis://{self.host}:{self.port}/{self.db}"
    
    def close_connection(self):
        """Close Redis connection"""
        if self._connection:
            try:
                self._connection.close()
                self._connection = None
                logger.debug("Redis connection closed")
            except Exception as e:
                logger.warning(f"Error closing Redis connection: {e}")
    
    def validate_redis_security(self, environment: Optional[str] = None) -> None:
        """Validate Redis security settings for the given environment"""
        if environment is None:
            environment = os.getenv('ENVIRONMENT', 'development')
        
        if environment == 'production' and not self.password:
            raise RuntimeError("❌ Redis не может работать без пароля в production! Установите REDIS_PASSWORD.")
        
        if not self.password:
            logger.warning("⚠️  Redis работает без пароля! Это небезопасно для production.")
            logger.warning("   Установите REDIS_PASSWORD в переменных окружения.")
    
    def mask_redis_url(self, url: str) -> str:
        """Mask password in Redis URL for safe logging"""
        if ":" in url and "@" in url:
            return re.sub(r'(:)([^@]+)(@)', r'\1***\3', url)
        return url
    
    @lru_cache(maxsize=1)
    def get_redis_urls(self) -> Tuple[str, str]:
        """Get Redis broker and result backend URLs"""
        self.validate_redis_security()
        
        # Get clean URL (handles masking internally)
        broker_url = self.get_connection_string(mask_password=False)
        result_backend = broker_url
        
        # Log safely
        safe_url = self.mask_redis_url(broker_url)
        if self.password:
            logger.info(f"🔐 Redis подключение с аутентификацией: {safe_url}")
        else:
            logger.info(f"🔓 Redis подключение без пароля: {self.host}:{self.port}/{self.db}")
        
        return broker_url, result_backend
    
    def get_redis_connection_info(self) -> str:
        """Get safe Redis connection info for logging (without password)"""
        auth_status = "с паролем" if self.password else "без пароля"
        return f"Redis {self.host}:{self.port}/{self.db} ({auth_status})"


# Global instance for easy access
redis_manager = RedisManager()

# Convenience functions for backward compatibility
check_redis = redis_manager.check_connection
start_redis = redis_manager.start_server
stop_redis = redis_manager.stop_server
restart_redis = redis_manager.restart_server

# New unified functions
get_redis_urls = redis_manager.get_redis_urls
validate_redis_security = redis_manager.validate_redis_security
mask_redis_url = redis_manager.mask_redis_url
check_redis_connection = redis_manager.check_connection
get_redis_connection_info = redis_manager.get_redis_connection_info
