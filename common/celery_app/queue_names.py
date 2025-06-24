# Централизованные имена очередей для Celery
# Очереди по техническим функциям

# Технические очереди
FILLOUT_PROCESSING_QUEUE = "fillout_processing"  # Получение данных из внешних источников (Fillout API)
TEXT_PROCESSING_QUEUE = "text_processing"        # Обработка и парсинг текстов (резюме, вакансии)
EMBEDDINGS_QUEUE = "embeddings"                   # Генерация эмбеддингов
RERANKING_QUEUE = "reranking"                     # AI-переранжирование результатов
ORCHESTRATION_QUEUE = "orchestration"            # Управление workflow и координация задач
