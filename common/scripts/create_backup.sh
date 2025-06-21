#!/bin/bash
"""
Скрипт создания резервных копий для системы HR Analysis
Создает backup базы данных, конфигурации и критичных файлов
"""

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] $1${NC}"
}

error() {
    echo -e "${RED}[ERROR] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[WARNING] $1${NC}"
}

info() {
    echo -e "${BLUE}[INFO] $1${NC}"
}

# Конфигурация
BACKUP_BASE_DIR="/var/backups/hr-analysis"
APP_DIR="/home/hr-user/hr-analysis"
TIMESTAMP="${1:-$(date +%Y%m%d_%H%M%S)}"
BACKUP_DIR="${BACKUP_BASE_DIR}/${TIMESTAMP}"

# Настройки ротации
KEEP_DAILY=7
KEEP_WEEKLY=4
KEEP_MONTHLY=6

create_backup_structure() {
    log "Создание структуры директорий backup..."
    
    sudo mkdir -p "$BACKUP_DIR"
    sudo mkdir -p "$BACKUP_DIR/database"
    sudo mkdir -p "$BACKUP_DIR/config"
    sudo mkdir -p "$BACKUP_DIR/logs"
    sudo mkdir -p "$BACKUP_DIR/metrics"
    sudo mkdir -p "$BACKUP_DIR/code"
    
    log "Структура backup создана: $BACKUP_DIR"
}

backup_database() {
    log "Создание backup базы данных..."
    
    # Загружаем переменные окружения
    if [ -f "$APP_DIR/.env" ]; then
        source "$APP_DIR/.env"
    else
        error "Файл .env не найден: $APP_DIR/.env"
        return 1
    fi
    
    # Извлекаем параметры подключения из DATABASE_URL
    if [ -n "$DATABASE_URL" ]; then
        DB_HOST=$(echo "$DATABASE_URL" | sed -E 's|postgresql://[^@]*@([^:/]+).*|\1|')
        DB_PORT=$(echo "$DATABASE_URL" | sed -E 's|postgresql://[^@]*@[^:]+:([0-9]+).*|\1|')
        DB_NAME=$(echo "$DATABASE_URL" | sed -E 's|postgresql://[^@]*@[^/]*/([^?]*).*|\1|')
        DB_USER=$(echo "$DATABASE_URL" | sed -E 's|postgresql://([^:@]*).*|\1|')
        DB_PASSWORD=$(echo "$DATABASE_URL" | sed -E 's|postgresql://[^:]*:([^@]*)@.*|\1|')
    else
        error "DATABASE_URL не найден в .env"
        return 1
    fi
    
    # Создаем backup базы данных
    backup_file="${BACKUP_DIR}/database/hr_analysis_${TIMESTAMP}.sql"
    
    PGPASSWORD="$DB_PASSWORD" pg_dump \
        -h "$DB_HOST" \
        -p "$DB_PORT" \
        -U "$DB_USER" \
        --format=custom \
        --verbose \
        --file="$backup_file" \
        "$DB_NAME"
    
    # Создаем также plain SQL версию для ручного восстановления
    plain_backup="${BACKUP_DIR}/database/hr_analysis_${TIMESTAMP}_plain.sql"
    PGPASSWORD="$DB_PASSWORD" pg_dump \
        -h "$DB_HOST" \
        -p "$DB_PORT" \
        -U "$DB_USER" \
        --format=plain \
        --file="$plain_backup" \
        "$DB_NAME"
    
    # Проверяем размер backup
    backup_size=$(du -h "$backup_file" | cut -f1)
    log "Backup базы данных создан: $backup_file (размер: $backup_size)"
    
    # Создаем metadata файл
    cat > "${BACKUP_DIR}/database/metadata.json" <<EOF
{
    "timestamp": "$TIMESTAMP",
    "database_host": "$DB_HOST",
    "database_port": "$DB_PORT",
    "database_name": "$DB_NAME",
    "database_user": "$DB_USER",
    "backup_format": "custom",
    "backup_size": "$backup_size",
    "pg_version": "$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT version();" | xargs)"
}
EOF
}

backup_configuration() {
    log "Создание backup конфигурации..."
    
    # Backup .env файла
    if [ -f "$APP_DIR/.env" ]; then
        cp "$APP_DIR/.env" "${BACKUP_DIR}/config/env_${TIMESTAMP}.env"
        log ".env файл сохранен"
    else
        warn ".env файл не найден"
    fi
    
    # Backup systemd сервисов
    systemd_dir="${BACKUP_DIR}/config/systemd"
    mkdir -p "$systemd_dir"
    
    services=(
        "hr-celery-cpu.service"
        "hr-celery-gpu.service"
        "hr-worker-monitor.service"
        "hr-gpu-monitor.service"
    )
    
    for service in "${services[@]}"; do
        if [ -f "/etc/systemd/system/$service" ]; then
            cp "/etc/systemd/system/$service" "$systemd_dir/"
            log "Systemd сервис сохранен: $service"
        fi
    done
    
    # Backup Redis конфигурации
    if [ -f "/etc/redis/redis.conf" ]; then
        cp "/etc/redis/redis.conf" "${BACKUP_DIR}/config/redis_${TIMESTAMP}.conf"
        log "Redis конфигурация сохранена"
    fi
    
    # Backup PostgreSQL конфигурации
    if [ -f "/etc/postgresql/*/main/postgresql.conf" ]; then
        cp /etc/postgresql/*/main/postgresql.conf "${BACKUP_DIR}/config/postgresql_${TIMESTAMP}.conf" 2>/dev/null || true
        log "PostgreSQL конфигурация сохранена"
    fi
    
    # Backup SSL сертификатов
    if [ -d "/etc/postgresql-ssl" ]; then
        cp -r "/etc/postgresql-ssl" "${BACKUP_DIR}/config/ssl_certificates"
        log "SSL сертификаты сохранены"
    fi
}

backup_logs() {
    log "Создание backup логов..."
    
    # Backup логов приложения
    if [ -d "/var/log/hr-analysis" ]; then
        cp -r "/var/log/hr-analysis" "${BACKUP_DIR}/logs/app_logs"
        log "Логи приложения сохранены"
    fi
    
    # Backup системных логов Celery
    journalctl_backup="${BACKUP_DIR}/logs/systemd"
    mkdir -p "$journalctl_backup"
    
    services=(
        "hr-celery-cpu"
        "hr-celery-gpu"
        "hr-worker-monitor"
        "hr-gpu-monitor"
    )
    
    for service in "${services[@]}"; do
        if systemctl is-enabled "$service" >/dev/null 2>&1; then
            journalctl -u "$service" --since "7 days ago" > "${journalctl_backup}/${service}_${TIMESTAMP}.log"
            log "Системные логи сохранены: $service"
        fi
    done
}

backup_metrics() {
    log "Создание backup метрик..."
    
    # Backup метрик мониторинга
    if [ -d "$APP_DIR/logs/metrics" ]; then
        cp -r "$APP_DIR/logs/metrics" "${BACKUP_DIR}/metrics/"
        log "Метрики мониторинга сохранены"
    fi
    
    # Backup статистики Celery
    if [ -f "$APP_DIR/celerybeat-schedule" ]; then
        cp "$APP_DIR/celerybeat-schedule"* "${BACKUP_DIR}/metrics/" 2>/dev/null || true
        log "Celery Beat schedule сохранен"
    fi
}

backup_code() {
    log "Создание backup кода..."
    
    cd "$APP_DIR"
    
    # Информация о текущем коммите
    git_info="${BACKUP_DIR}/code/git_info.json"
    cat > "$git_info" <<EOF
{
    "timestamp": "$TIMESTAMP",
    "commit_hash": "$(git rev-parse HEAD)",
    "branch": "$(git rev-parse --abbrev-ref HEAD)",
    "commit_message": "$(git log -1 --pretty=%B | tr '\n' ' ')",
    "author": "$(git log -1 --pretty=%an)",
    "date": "$(git log -1 --pretty=%ad)",
    "tags": "$(git tag --points-at HEAD | tr '\n' ',')",
    "dirty": $([ -n "$(git status --porcelain)" ] && echo "true" || echo "false")
}
EOF
    
    # Создаем архив кода
    git archive --format=tar.gz --prefix=hr-analysis/ HEAD > "${BACKUP_DIR}/code/source_${TIMESTAMP}.tar.gz"
    
    # Backup requirements
    if [ -f "requirements.txt" ]; then
        cp "requirements.txt" "${BACKUP_DIR}/code/requirements_${TIMESTAMP}.txt"
    fi
    
    log "Исходный код сохранен"
}

create_backup_summary() {
    log "Создание сводки backup..."
    
    summary_file="${BACKUP_DIR}/backup_summary.json"
    backup_size=$(du -sh "$BACKUP_DIR" | cut -f1)
    
    cat > "$summary_file" <<EOF
{
    "timestamp": "$TIMESTAMP",
    "backup_directory": "$BACKUP_DIR",
    "total_size": "$backup_size",
    "components": {
        "database": $([ -f "${BACKUP_DIR}/database/hr_analysis_${TIMESTAMP}.sql" ] && echo "true" || echo "false"),
        "configuration": $([ -f "${BACKUP_DIR}/config/env_${TIMESTAMP}.env" ] && echo "true" || echo "false"),
        "logs": $([ -d "${BACKUP_DIR}/logs" ] && echo "true" || echo "false"),
        "metrics": $([ -d "${BACKUP_DIR}/metrics" ] && echo "true" || echo "false"),
        "code": $([ -f "${BACKUP_DIR}/code/source_${TIMESTAMP}.tar.gz" ] && echo "true" || echo "false")
    },
    "system_info": {
        "hostname": "$(hostname)",
        "uptime": "$(uptime | tr -d '\n')",
        "disk_usage": "$(df -h / | tail -1 | awk '{print $5}')",
        "memory_usage": "$(free -h | grep '^Mem:' | awk '{print $3 "/" $2}')"
    },
    "environment": "$(grep '^ENVIRONMENT=' "$APP_DIR/.env" 2>/dev/null | cut -d'=' -f2 || echo 'unknown')"
}
EOF
    
    log "Сводка backup создана: $summary_file"
}

compress_backup() {
    log "Сжатие backup..."
    
    cd "$BACKUP_BASE_DIR"
    
    # Создаем сжатый архив
    tar -czf "${TIMESTAMP}.tar.gz" "$TIMESTAMP"
    
    # Удаляем несжатую версию
    rm -rf "$TIMESTAMP"
    
    compressed_size=$(du -h "${TIMESTAMP}.tar.gz" | cut -f1)
    log "Backup сжат: ${TIMESTAMP}.tar.gz (размер: $compressed_size)"
}

rotate_backups() {
    log "Ротация старых backup..."
    
    cd "$BACKUP_BASE_DIR"
    
    # Удаляем backup старше указанного срока
    # Ежедневные backup - сохраняем KEEP_DAILY дней
    find . -name "*.tar.gz" -mtime +$KEEP_DAILY -delete 2>/dev/null || true
    
    # Можно добавить более сложную логику для недельных и месячных backup
    
    remaining_backups=$(ls -1 *.tar.gz 2>/dev/null | wc -l)
    log "Ротация завершена, осталось backup: $remaining_backups"
}

verify_backup() {
    log "Проверка целостности backup..."
    
    cd "$BACKUP_BASE_DIR"
    
    # Проверяем архив
    if tar -tzf "${TIMESTAMP}.tar.gz" >/dev/null 2>&1; then
        log "✅ Архив backup корректен"
    else
        error "❌ Архив backup поврежден"
        return 1
    fi
    
    # Проверяем размер
    archive_size=$(du -sh "${TIMESTAMP}.tar.gz" | cut -f1)
    if [[ "$archive_size" == "0"* ]]; then
        error "❌ Размер backup подозрительно мал: $archive_size"
        return 1
    else
        log "✅ Размер backup в норме: $archive_size"
    fi
    
    return 0
}

show_usage() {
    echo "Использование: $0 [TIMESTAMP]"
    echo ""
    echo "Параметры:"
    echo "  TIMESTAMP    Временная метка для backup (по умолчанию: текущее время)"
    echo ""
    echo "Примеры:"
    echo "  $0                    # Создать backup с текущей временной меткой"
    echo "  $0 manual_backup      # Создать backup с пользовательской меткой"
    echo ""
    echo "Backup будет создан в: $BACKUP_BASE_DIR"
}

main() {
    if [[ "$1" == "--help" || "$1" == "-h" ]]; then
        show_usage
        exit 0
    fi
    
    log "🚀 Начинаем создание backup: $TIMESTAMP"
    
    # Проверяем права доступа
    if [[ $EUID -eq 0 ]]; then
        warn "Скрипт запущен от root, рекомендуется запуск от пользователя приложения"
    fi
    
    # Создаем базовую директорию
    sudo mkdir -p "$BACKUP_BASE_DIR"
    sudo chown hr-user:hr-user "$BACKUP_BASE_DIR"
    
    # Выполняем backup компонентов
    create_backup_structure
    backup_database
    backup_configuration
    backup_logs
    backup_metrics
    backup_code
    create_backup_summary
    
    # Сжимаем и проверяем
    compress_backup
    
    if verify_backup; then
        log "✅ Backup создан успешно: ${BACKUP_BASE_DIR}/${TIMESTAMP}.tar.gz"
    else
        error "❌ Backup создан с ошибками"
        exit 1
    fi
    
    # Ротация старых backup
    rotate_backups
    
    log "🎉 Создание backup завершено: $TIMESTAMP"
}

# Запуск основной функции
main "$@"
