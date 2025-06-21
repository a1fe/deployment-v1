"""
Celery задачи для скоринга с помощью BGE-M3 Reranker модели

Переоценка результатов сопоставления для повышения качества и генерации точных scores
Работает с эмбеддингами из ChromaDB для максимальной производительности
"""

from datetime import datetime
from typing import Dict, List, Optional, Any
from celery.utils.log import get_task_logger
from database.operations.candidate_operations import SubmissionCRUD
from database.operations.company_operations import JobCRUD
from utils.reranker_config import get_reranker_client
from utils.chroma_config import chroma_client, ChromaConfig
from tasks.task_utils import get_db_session, safe_uuid_convert, serialize_for_json, mask_sensitive_data

# Импортируем Celery app
from celery_app.celery_app import get_celery_app

app = get_celery_app()

logger = get_task_logger(__name__)


@app.task(
    bind=True, 
    name='tasks.scoring.rerank_resume_matches', 
    max_retries=3, 
    default_retry_delay=60,
    soft_time_limit=600,  # 10 минут для реранкинга
    time_limit=720        # 12 минут
)
def rerank_resume_matches(self, job_id: int, matches: List[Dict[str, Any]], 
                         top_k: int = 20, min_rerank_score: float = 0.3) -> Dict[str, Any]:
    """
    СКОРИНГ РЕЗЮМЕ: Переоценка найденных резюме с помощью BGE-M3 Reranker
    Использует эмбеддинги из ChromaDB для максимальной точности и производительности
    
    Args:
        job_id: ID вакансии
        matches: Найденные совпадения резюме (из основной задачи поиска)
        top_k: Количество топ-результатов после reranking (по умолчанию 20)
        min_rerank_score: Минимальный rerank score для валидного результата (по умолчанию 0.3)
        
    Returns:
        Обновленные совпадения с rerank_score и улучшенным ранжированием
    """
    logger.info(f"🎯 Скоринг {len(matches)} резюме для вакансии {job_id} с помощью BGE-M3")
    
    with get_db_session() as db:
        try:
            # Валидируем входные данные
            if not matches:
                logger.warning("⚠️ Пустой список совпадений для скоринга")
                return serialize_for_json({
                    'job_id': job_id,
                    'reranked_matches': [],
                    'total_reranked': 0,
                    'message': 'Нет совпадений для скоринга'
                })
            
            # Получаем вакансию для контекста
            job = JobCRUD.get_by_id(db, job_id)
            if not job:
                logger.error(f"❌ Вакансия не найдена: {job_id}")
                raise ValueError(f"Вакансия не найдена: {job_id}")
            
            # Проверяем доступность ChromaDB и BGE-M3 Reranker
            if not chroma_client.health_check():
                logger.error("❌ ChromaDB недоступен")
                raise Exception("ChromaDB недоступен")
            
            reranker_client = get_reranker_client()
            if not reranker_client.health_check():
                logger.error("❌ BGE-M3 Reranker недоступен")
                raise Exception("BGE-M3 Reranker недоступен")
            
            # Получаем коллекции ChromaDB
            job_collection = chroma_client.get_or_create_collection(ChromaConfig.JOB_COLLECTION)
            resume_collection = chroma_client.get_or_create_collection(ChromaConfig.RESUME_COLLECTION)
            
            # Ищем эмбеддинг вакансии в ChromaDB
            job_prefix = f"job_{job_id}"
            job_results = job_collection.get(include=['embeddings', 'metadatas'])
            
            job_embedding = None
            for idx, job_id_str in enumerate(job_results['ids']):
                if job_id_str.startswith(job_prefix):
                    job_embedding = job_results['embeddings'][idx]
                    break
            
            if not job_embedding:
                logger.warning(f"⚠️ Эмбеддинг для вакансии {job_id} не найден в ChromaDB")
                # Возвращаем исходные matches без reranking
                logger.info("🔄 Используем исходные совпадения без BGE-M3 скоринга")
                return serialize_for_json({
                    'job_id': job_id,
                    'job_title': job.title,
                    'reranked_matches': matches[:top_k],
                    'total_reranked': len(matches[:top_k]),
                    'message': 'Эмбеддинг вакансии не найден, используются исходные результаты'
                })
            
            # Получаем эмбеддинги для резюме из matches
            enhanced_matches = []
            for match in matches:
                # Извлекаем submission_id и chroma_id из match
                submission_id = match.get('submission_id')
                chroma_id = match.get('chroma_id')
                
                if not chroma_id:
                    # Попробуем сконструировать chroma_id из submission_id
                    if submission_id:
                        chroma_id = f"resume_{submission_id}"
                    else:
                        logger.warning(f"⚠️ Отсутствует chroma_id для совпадения: {match}")
                        continue
                
                # Получаем эмбеддинг резюме из ChromaDB
                try:
                    resume_data = resume_collection.get(
                        ids=[chroma_id],
                        include=['embeddings', 'documents', 'metadatas']
                    )
                    
                    if resume_data['ids'] and resume_data['embeddings']:
                        # Добавляем эмбеддинг к match
                        enhanced_match = match.copy()
                        enhanced_match['embedding'] = resume_data['embeddings'][0]
                        
                        # Добавляем документ если есть
                        if resume_data['documents'] and resume_data['documents'][0]:
                            enhanced_match['document'] = resume_data['documents'][0]
                        
                        enhanced_matches.append(enhanced_match)
                    else:
                        logger.warning(f"⚠️ Эмбеддинг не найден для {chroma_id}")
                        # Добавляем без эмбеддинга
                        enhanced_matches.append(match)
                        
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка получения эмбеддинга для {chroma_id}: {e}")
                    # Добавляем без эмбеддинга
                    enhanced_matches.append(match)
            
            if not enhanced_matches:
                logger.warning("⚠️ Не удалось получить эмбеддинги для совпадений")
                return serialize_for_json({
                    'job_id': job_id,
                    'job_title': job.title,
                    'reranked_matches': matches[:top_k],
                    'total_reranked': len(matches[:top_k]),
                    'message': 'Не удалось получить эмбеддинги, используются исходные результаты'
                })
            
            # Выполняем reranking с помощью BGE-M3
            logger.info(f"🔍 Запуск BGE-M3 Reranker для {len(enhanced_matches)} совпадений")
            reranked_matches = reranker_client.rerank_chroma_matches(job_embedding, enhanced_matches)
            
            if not reranked_matches:
                logger.warning("⚠️ BGE-M3 Reranker не вернул результатов")
                return serialize_for_json({
                    'job_id': job_id,
                    'job_title': job.title,
                    'reranked_matches': enhanced_matches[:top_k],
                    'total_reranked': len(enhanced_matches[:top_k]),
                    'message': 'Reranker не вернул результатов, используются исходные'
                })
            
            # Фильтруем по минимальному rerank score
            filtered_matches = [
                match for match in reranked_matches 
                if match.get('rerank_score', 0) >= min_rerank_score
            ]
            
            # Ограничиваем количество результатов
            final_matches = filtered_matches[:top_k]
            
            # Обогащаем данными о качестве скоринга
            for match in final_matches:
                # Убираем эмбеддинг из результата (может быть большим)
                if 'embedding' in match:
                    del match['embedding']
                
                # Добавляем метрики качества
                match['quality_metrics'] = {
                    'original_similarity': match.get('similarity', 0),
                    'rerank_score': match.get('rerank_score', 0),
                    'score_improvement': match.get('rerank_score', 0) - match.get('similarity', 0),
                    'is_reranked': True,
                    'reranker_model': 'BGE-M3',
                    'used_embeddings': True
                }
                
                # Маскируем чувствительные данные
                if 'candidate_email' in match:
                    match['candidate_email'] = mask_sensitive_data(match['candidate_email'])
            
            logger.info(f"✅ BGE-M3 скоринг завершен: {len(final_matches)} высококачественных совпадений")
            
            # Формируем результат
            result = {
                'job_id': job_id,
                'job_title': job.title,
                'company_id': job.company_id,
                'reranked_matches': final_matches,
                'total_reranked': len(final_matches),
                'scoring_params': {
                    'top_k': top_k,
                    'min_rerank_score': min_rerank_score,
                    'reranker_model': 'BGE-M3',
                    'used_embeddings': True,
                    'max_token_length': 8192
                },
                'statistics': {
                    'original_matches': len(matches),
                    'enhanced_with_embeddings': len(enhanced_matches),
                    'after_reranking': len(reranked_matches),
                    'above_threshold': len(filtered_matches),
                    'final_results': len(final_matches)
                },
                'processed_at': datetime.utcnow().isoformat(),
                'message': f'BGE-M3 скоринг выполнен: {len(final_matches)} качественных совпадений'
            }
            
            return serialize_for_json(result)
            
        except Exception as e:
            logger.error(f"❌ Ошибка при BGE-M3 скоринге резюме для вакансии {job_id}: {str(e)}")
            # Retry логика
            if self.request.retries < self.max_retries:
                logger.info(f"🔄 Повторная попытка {self.request.retries + 1}/{self.max_retries}")
                raise self.retry(countdown=60 * (self.request.retries + 1))
            raise


@app.task(
    bind=True, 
    name='tasks.scoring.rerank_job_matches', 
    max_retries=3, 
    default_retry_delay=60,
    soft_time_limit=600,  # 10 минут для реранкинга
    time_limit=720        # 12 минут
)
def rerank_job_matches(self, submission_id: str, matches: List[Dict[str, Any]], 
                      top_k: int = 10, min_rerank_score: float = 0.3) -> Dict[str, Any]:
    """
    СКОРИНГ ВАКАНСИЙ: Переоценка найденных вакансий с помощью BGE-M3 Reranker
    Использует эмбеддинги из ChromaDB для максимальной точности
    
    Args:
        submission_id: ID заявки кандидата
        matches: Найденные совпадения вакансий
        top_k: Количество топ-результатов после reranking (по умолчанию 10)
        min_rerank_score: Минимальный rerank score (по умолчанию 0.3)
        
    Returns:
        Обновленные совпадения с rerank_score и улучшенным ранжированием
    """
    logger.info(f"🔍 Скоринг {len(matches)} вакансий для резюме {mask_sensitive_data(submission_id)}")
    
    with get_db_session() as db:
        try:
            # Валидируем входные данные
            if not matches:
                logger.warning("⚠️ Пустой список совпадений для скоринга")
                return serialize_for_json({
                    'submission_id': submission_id,
                    'reranked_matches': [],
                    'total_reranked': 0,
                    'message': 'Нет совпадений для скоринга'
                })
            
            # Валидируем submission_id
            submission_uuid = safe_uuid_convert(submission_id)
            if not submission_uuid:
                raise ValueError(f"Некорректный submission_id: {submission_id}")
            
            # Получаем заявку кандидата
            submission = SubmissionCRUD.get_by_id(db, submission_uuid)
            if not submission:
                raise ValueError(f"Заявка не найдена: {submission_id}")
            
            # Проверяем доступность ChromaDB и BGE-M3 Reranker
            if not chroma_client.health_check():
                logger.error("❌ ChromaDB недоступен")
                raise Exception("ChromaDB недоступен")
            
            reranker_client = get_reranker_client()
            if not reranker_client.health_check():
                logger.error("❌ BGE-M3 Reranker недоступен")
                raise Exception("BGE-M3 Reranker недоступен")
            
            # Получаем коллекции ChromaDB
            resume_collection = chroma_client.get_or_create_collection(ChromaConfig.RESUME_COLLECTION)
            job_collection = chroma_client.get_or_create_collection(ChromaConfig.JOB_COLLECTION)
            
            # Ищем эмбеддинг резюме в ChromaDB
            resume_chroma_id = f"resume_{submission_id}"
            try:
                resume_data = resume_collection.get(
                    ids=[resume_chroma_id],
                    include=['embeddings', 'documents']
                )
                
                if not resume_data['ids'] or not resume_data['embeddings']:
                    logger.warning(f"⚠️ Эмбеддинг для резюме {submission_id} не найден в ChromaDB")
                    # Возвращаем исходные matches без reranking
                    return serialize_for_json({
                        'submission_id': submission_id,
                        'candidate_id': submission.candidate_id,
                        'reranked_matches': matches[:top_k],
                        'total_reranked': len(matches[:top_k]),
                        'message': 'Эмбеддинг резюме не найден, используются исходные результаты'
                    })
                
                resume_embedding = resume_data['embeddings'][0]
                
            except Exception as e:
                logger.warning(f"⚠️ Ошибка получения эмбеддинга резюме {submission_id}: {e}")
                return serialize_for_json({
                    'submission_id': submission_id,
                    'candidate_id': submission.candidate_id,
                    'reranked_matches': matches[:top_k],
                    'total_reranked': len(matches[:top_k]),
                    'message': 'Ошибка получения эмбеддинга резюме'
                })
            
            # Получаем эмбеддинги для вакансий из matches
            enhanced_matches = []
            for match in matches:
                job_id = match.get('job_id')
                if not job_id:
                    logger.warning(f"⚠️ Отсутствует job_id в совпадении: {match}")
                    continue
                
                # Ищем эмбеддинг вакансии
                job_chroma_id = f"job_{job_id}"
                try:
                    job_data = job_collection.get(
                        ids=[job_chroma_id],
                        include=['embeddings', 'documents', 'metadatas']
                    )
                    
                    if job_data['ids'] and job_data['embeddings']:
                        # Добавляем эмбеддинг к match
                        enhanced_match = match.copy()
                        enhanced_match['embedding'] = job_data['embeddings'][0]
                        
                        # Добавляем документ если есть
                        if job_data['documents'] and job_data['documents'][0]:
                            enhanced_match['document'] = job_data['documents'][0]
                        
                        enhanced_matches.append(enhanced_match)
                    else:
                        logger.warning(f"⚠️ Эмбеддинг не найден для вакансии {job_id}")
                        # Добавляем без эмбеддинга
                        enhanced_matches.append(match)
                        
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка получения эмбеддинга для вакансии {job_id}: {e}")
                    # Добавляем без эмбеддинга
                    enhanced_matches.append(match)
            
            if not enhanced_matches:
                logger.warning("⚠️ Не удалось получить эмбеддинги для вакансий")
                return serialize_for_json({
                    'submission_id': submission_id,
                    'candidate_id': submission.candidate_id,
                    'reranked_matches': matches[:top_k],
                    'total_reranked': len(matches[:top_k]),
                    'message': 'Не удалось получить эмбеддинги вакансий'
                })
            
            # Выполняем reranking с помощью BGE-M3
            logger.info(f"🔍 Запуск BGE-M3 Reranker для {len(enhanced_matches)} вакансий")
            reranked_matches = reranker_client.rerank_chroma_matches(resume_embedding, enhanced_matches)
            
            if not reranked_matches:
                logger.warning("⚠️ BGE-M3 Reranker не вернул результатов")
                return serialize_for_json({
                    'submission_id': submission_id,
                    'candidate_id': submission.candidate_id,
                    'reranked_matches': enhanced_matches[:top_k],
                    'total_reranked': len(enhanced_matches[:top_k]),
                    'message': 'Reranker не вернул результатов'
                })
            
            # Фильтруем по минимальному rerank score
            filtered_matches = [
                match for match in reranked_matches 
                if match.get('rerank_score', 0) >= min_rerank_score
            ]
            
            # Ограничиваем количество результатов
            final_matches = filtered_matches[:top_k]
            
            # Обогащаем данными о качестве скоринга
            for match in final_matches:
                # Убираем эмбеддинг из результата
                if 'embedding' in match:
                    del match['embedding']
                
                # Добавляем метрики качества
                match['quality_metrics'] = {
                    'original_similarity': match.get('similarity', 0),
                    'rerank_score': match.get('rerank_score', 0),
                    'score_improvement': match.get('rerank_score', 0) - match.get('similarity', 0),
                    'is_reranked': True,
                    'reranker_model': 'BGE-M3',
                    'used_embeddings': True
                }
            
            logger.info(f"✅ BGE-M3 скоринг завершен: {len(final_matches)} качественных вакансий")
            
            result = {
                'submission_id': submission_id,
                'candidate_id': submission.candidate_id,
                'reranked_matches': final_matches,
                'total_reranked': len(final_matches),
                'scoring_params': {
                    'top_k': top_k,
                    'min_rerank_score': min_rerank_score,
                    'reranker_model': 'BGE-M3',
                    'used_embeddings': True,
                    'max_token_length': 8192
                },
                'statistics': {
                    'original_matches': len(matches),
                    'enhanced_with_embeddings': len(enhanced_matches),
                    'after_reranking': len(reranked_matches),
                    'above_threshold': len(filtered_matches),
                    'final_results': len(final_matches)
                },
                'processed_at': datetime.utcnow().isoformat(),
                'message': f'BGE-M3 скоринг выполнен: {len(final_matches)} качественных вакансий'
            }
            
            return serialize_for_json(result)
            
        except Exception as e:
            logger.error(f"❌ Ошибка при BGE-M3 скоринге вакансий для резюме: {str(e)}")
            # Retry логика
            if self.request.retries < self.max_retries:
                logger.info(f"🔄 Повторная попытка {self.request.retries + 1}/{self.max_retries}")
                raise self.retry(countdown=60 * (self.request.retries + 1))
            raise


@app.task(
    bind=True, 
    name='tasks.scoring.enhanced_resume_search', 
    max_retries=3, 
    default_retry_delay=60,
    soft_time_limit=900,  # 15 минут для расширенного поиска
    time_limit=1080       # 18 минут
)
def enhanced_resume_search(self, job_id: int, top_k: int = 20, min_similarity: float = 0.4, 
                          use_reranking: bool = True, min_rerank_score: float = 0.5) -> Dict[str, Any]:
    """
    КОМПЛЕКСНЫЙ ПОИСК С BGE-M3 СКОРИНГОМ: Основной поиск + BGE-M3 Reranker с эмбеддингами
    
    Выполняет полный цикл: поиск резюме в ChromaDB -> скоринг через BGE-M3 reranker -> финальные результаты
    
    Args:
        job_id: ID вакансии
        top_k: Количество финальных результатов (по умолчанию 20)
        min_similarity: Минимальный порог сходства для первичного поиска (по умолчанию 0.4)
        use_reranking: Использовать ли BGE-M3 Reranker (по умолчанию True)
        min_rerank_score: Минимальный rerank score (по умолчанию 0.5)
        
    Returns:
        Комплексные результаты с метриками качества BGE-M3
    """
    logger.info(f"🚀 Комплексный поиск резюме для вакансии {job_id} с BGE-M3 скорингом")
    
    try:
        # Шаг 1: Основной поиск резюме в ChromaDB (ИСПРАВЛЕНИЕ: убираем циклический импорт)
        logger.info("🔍 Шаг 1: Основной поиск резюме в ChromaDB...")
        # Используем send_task вместо прямого импорта
        search_task = app.send_task(
            'tasks.matching.find_matching_resumes_for_job',
            args=[job_id, top_k * 2, min_similarity],  # Берем больше для последующей фильтрации
        )
        search_data = search_task.get(timeout=300)
        
        initial_matches = search_data.get('matches', [])
        
        if not initial_matches:
            logger.warning("⚠️ Основной поиск не вернул результатов")
            return serialize_for_json({
                'job_id': job_id,
                'job_title': search_data.get('job_title', 'Unknown'),
                'enhanced_matches': [],
                'total_enhanced': 0,
                'workflow': {
                    'step1_search': 'completed',
                    'step2_reranking': 'skipped - no results',
                    'use_reranking': use_reranking,
                    'reranker_model': 'BGE-M3' if use_reranking else None
                },
                'message': 'Основной поиск в ChromaDB не вернул результатов'
            })
        
        # Шаг 2: Скоринг через BGE-M3 Reranker (если включен)
        final_matches = initial_matches
        reranking_stats = {}
        
        if use_reranking:
            logger.info("🎯 Шаг 2: Скоринг через BGE-M3 Reranker с эмбеддингами...")
            # Прямой вызов задачи через run() метод
            scoring_data = rerank_resume_matches.run(
                job_id,
                initial_matches,
                top_k=top_k,
                min_rerank_score=min_rerank_score
            )
            
            final_matches = scoring_data.get('reranked_matches', initial_matches)
            reranking_stats = scoring_data.get('statistics', {})
            logger.info(f"✅ BGE-M3 скоринг завершен: {len(final_matches)} финальных результатов")
        else:
            logger.info("🔄 Шаг 2: BGE-M3 скоринг отключен, используем результаты основного поиска")
            final_matches = initial_matches[:top_k]
        
        # Формируем комплексный результат
        result = {
            'job_id': job_id,
            'job_title': search_data.get('job_title', 'Unknown'),
            'company_id': search_data.get('company_id'),
            'enhanced_matches': final_matches,
            'total_enhanced': len(final_matches),
            'workflow': {
                'step1_search': 'completed',
                'step2_reranking': 'completed' if use_reranking else 'disabled',
                'use_reranking': use_reranking,
                'reranker_model': 'BGE-M3' if use_reranking else None,
                'data_source': 'ChromaDB embeddings',
                'max_token_length': 8192 if use_reranking else None
            },
            'search_params': {
                'top_k': top_k,
                'min_similarity': min_similarity,
                'min_rerank_score': min_rerank_score if use_reranking else None,
                'uses_embeddings': True
            },
            'comprehensive_statistics': {
                'initial_search': search_data.get('statistics', {}),
                'reranking': reranking_stats,
                'final_count': len(final_matches)
            },
            'processed_at': datetime.utcnow().isoformat(),
            'message': f'Комплексный поиск завершен: {len(final_matches)} качественных кандидатов (BGE-M3)'
        }
        
        # АВТОМАТИЧЕСКОЕ СОХРАНЕНИЕ РЕЗУЛЬТАТОВ В PostgreSQL
        if final_matches:  # Сохраняем только если есть результаты
            try:
                logger.info("💾 Запуск сохранения результатов BGE-M3 анализа в PostgreSQL...")
                
                # ИСПРАВЛЕНИЕ: Используем send_task вместо прямого импорта
                
                # Подготавливаем метаданные сессии
                session_metadata = {
                    'started_at': datetime.utcnow().isoformat(),
                    'search_type': 'enhanced_resume_search_bge_m3',
                    'task_id': self.request.id if hasattr(self.request, 'id') else None,
                    'reranker_model': 'BGE-M3',
                    'uses_embeddings': True,
                    'max_token_length': 8192
                }
                
                # Запускаем задачу сохранения асинхронно
                save_task = app.send_task(
                    'tasks.analysis_tasks.save_reranker_analysis_results',
                    args=[job_id, result],
                    kwargs={'session_metadata': session_metadata},
                    countdown=2  # Небольшая задержка для завершения основной задачи
                )
                
                logger.info(f"📤 Задача сохранения BGE-M3 результатов запущена: {save_task.id}")
                
                # Добавляем информацию о сохранении в результат
                result['analysis_saving'] = {
                    'task_id': save_task.id,
                    'status': 'scheduled',
                    'reranker_model': 'BGE-M3',
                    'message': 'Результаты BGE-M3 анализа будут сохранены в PostgreSQL'
                }
                
            except Exception as save_error:
                logger.warning(f"⚠️ Не удалось запустить сохранение BGE-M3 результатов: {save_error}")
                # Не прерываем основную задачу из-за ошибки сохранения
                result['analysis_saving'] = {
                    'status': 'failed',
                    'error': str(save_error),
                    'message': 'Ошибка при запуске сохранения BGE-M3 результатов'
                }
        
        logger.info(f"🏆 Комплексный поиск с BGE-M3 завершен успешно")
        return serialize_for_json(result)
        
    except Exception as e:
        logger.error(f"❌ Ошибка при комплексном поиске с BGE-M3: {str(e)}")
        # Retry логика
        if self.request.retries < self.max_retries:
            logger.info(f"🔄 Повторная попытка {self.request.retries + 1}/{self.max_retries}")
            raise self.retry(countdown=60 * (self.request.retries + 1))
        raise
