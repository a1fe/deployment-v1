#!/bin/bash

# =============================================================================
# Общие функции логирования для HR Analysis системы
# =============================================================================

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Функции логирования
log_info() {
    echo -e "${BLUE}[INFO $(date '+%H:%M:%S')]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS $(date '+%H:%M:%S')]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING $(date '+%H:%M:%S')]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR $(date '+%H:%M:%S')]${NC} $1" >&2
}

log_debug() {
    if [ "${DEBUG:-}" = "1" ]; then
        echo -e "${PURPLE}[DEBUG $(date '+%H:%M:%S')]${NC} $1"
    fi
}

log_step() {
    echo -e "${CYAN}[STEP $(date '+%H:%M:%S')]${NC} $1"
}

# Функция для красивого отображения заголовков
print_header() {
    local title="$1"
    local width=60
    local padding=$(( ($width - ${#title}) / 2 ))
    
    echo ""
    echo -e "${CYAN}$(printf '=%.0s' $(seq 1 $width))${NC}"
    echo -e "${CYAN}$(printf '%*s' $padding)${YELLOW}$title${CYAN}$(printf '%*s' $padding)${NC}"
    echo -e "${CYAN}$(printf '=%.0s' $(seq 1 $width))${NC}"
    echo ""
}

# Функция проверки статуса команды
check_status() {
    local command="$1"
    local description="$2"
    
    if eval "$command" >/dev/null 2>&1; then
        log_success "$description"
        return 0
    else
        log_error "$description"
        return 1
    fi
}

# Функция ожидания с прогрессом
wait_with_progress() {
    local seconds="$1"
    local message="$2"
    
    echo -n -e "${BLUE}[INFO $(date '+%H:%M:%S')]${NC} $message "
    
    for ((i=1; i<=seconds; i++)); do
        echo -n "."
        sleep 1
    done
    
    echo " готово!"
}

# Функция отображения использования памяти
show_memory_usage() {
    local title="${1:-Использование памяти}"
    
    echo ""
    log_info "$title:"
    echo "$(free -h | head -2)"
    echo ""
}

# Функция отображения использования диска
show_disk_usage() {
    local title="${1:-Использование диска}"
    
    echo ""
    log_info "$title:"
    echo "$(df -h / | tail -1)"
    echo ""
}
