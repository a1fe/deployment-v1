# Централизованные имена очередей для Celery
# Очереди по техническим функциям

# Технические очереди
FILLOUT_PROCESSING_QUEUE = "fillout_processing"  # Получение данных из внешних источников (Fillout API)
EMBEDDINGS_QUEUE = "embeddings"                   # Генерация эмбеддингов
RERANKING_QUEUE = "reranking"                     # AI-переранжирование результатов
ORCHESTRATION_QUEUE = "orchestration"            # Управление workflow и координация задач
