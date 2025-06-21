# Централизованные имена очередей для Celery
EMBEDDINGS_GPU_QUEUE = "embeddings_gpu"
SCORING_TASKS_QUEUE = "scoring_tasks"
DEFAULT_QUEUE = "default"
FILLOUT_QUEUE = "fillout"
SEARCH_BASIC_QUEUE = "search_basic"

# Новые очереди для улучшенной системы
CPU_INTENSIVE_QUEUE = "cpu_intensive"  # Для парсинга документов
SYSTEM_QUEUE = "system"  # Для системных задач (GPU проверки)
AI_ANALYSIS_QUEUE = "ai_analysis"  # Для AI анализа на GPU
EMBEDDINGS_CPU_QUEUE = "embeddings_cpu"  # Для эмбеддингов на CPU (fallback)
