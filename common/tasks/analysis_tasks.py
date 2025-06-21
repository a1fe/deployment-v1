"""
Celery задачи для сохранения результатов анализа BGE Reranker

Автоматическое сохранение результатов enhanced search в PostgreSQL
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from celery.utils.log import get_task_logger
from database.operations.analysis_operations import reranker_analysis_result_crud, reranker_analysis_session_crud
from tasks.task_utils import get_db_session, serialize_for_json, mask_sensitive_data

# Импортируем Celery app
from celery_app.celery_app import get_celery_app

app = get_celery_app()

logger = get_task_logger(__name__)


@app.task(
    bind=True, 
    name='tasks.analysis.save_reranker_results', 
    max_retries=3, 
    default_retry_delay=60,
    soft_time_limit=300,  # 5 минут для сохранения
    time_limit=360        # 6 минут
)
def save_reranker_analysis_results(
    self,
    job_id: int,
    enhanced_search_data: Dict[str, Any],
    session_metadata: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    СОХРАНЕНИЕ РЕЗУЛЬТАТОВ АНАЛИЗА: Автоматическое сохранение результатов enhanced search
    
    Эта задача выполняется после завершения enhanced_resume_search для сохранения
    результатов анализа BGE Reranker в PostgreSQL для последующей аналитики.
    
    Args:
        job_id: ID вакансии
        enhanced_search_data: Результаты enhanced search из tasks.scoring.enhanced_resume_search
        session_metadata: Дополнительные метаданные сессии (опционально)
        
    Returns:
        Результат сохранения с количеством сохраненных записей
    """
    logger.info(f"💾 Сохранение результатов анализа BGE Reranker для вакансии {job_id}")
    
    with get_db_session() as db:
        try:
            # Валидируем входные данные
            if not enhanced_search_data:
                logger.warning("⚠️ Пустые данные enhanced search для сохранения")
                return serialize_for_json({
                    'job_id': job_id,
                    'saved_results': 0,
                    'session_created': False,
                    'message': 'Нет данных для сохранения'
                })
            
            enhanced_matches = enhanced_search_data.get('enhanced_matches', [])
            if not enhanced_matches:
                logger.warning(f"⚠️ Нет enhanced matches для сохранения в вакансии {job_id}")
                return serialize_for_json({
                    'job_id': job_id,
                    'saved_results': 0,
                    'session_created': False,
                    'message': 'Нет enhanced matches для сохранения'
                })
            
            # Извлекаем параметры поиска и метаданные
            search_params = enhanced_search_data.get('search_params', {})
            workflow = enhanced_search_data.get('workflow', {})
            reranker_model = workflow.get('reranker_model', 'unknown')
            comprehensive_stats = enhanced_search_data.get('comprehensive_statistics', {})
            company_id = enhanced_search_data.get('company_id')
            
            if not company_id:
                logger.error(f"❌ Отсутствует company_id в данных для вакансии {job_id}")
                raise ValueError(f"Отсутствует company_id для вакансии {job_id}")
            
            # Создаем сессию анализа
            session_data = session_metadata or {}
            session_started = session_data.get('started_at')
            if isinstance(session_started, str):
                session_started = datetime.fromisoformat(session_started.replace('Z', '+00:00'))
            elif not session_started:
                session_started = datetime.utcnow()
            
            analysis_session = reranker_analysis_session_crud.create_session(
                db=db,
                job_id=job_id,
                company_id=company_id,
                search_params=search_params,
                reranker_model=reranker_model,
                session_stats=comprehensive_stats,
                total_results=len(enhanced_matches),
                started_at=session_started,
                completed_at=datetime.utcnow()
            )
            
            logger.info(f"📝 Создана сессия анализа: {analysis_session.session_uuid}")
            
            # Сохраняем результаты анализа
            saved_results = reranker_analysis_result_crud.create_from_enhanced_search(
                db=db,
                job_id=job_id,
                enhanced_matches=enhanced_matches,
                search_params=search_params,
                workflow_stats=comprehensive_stats,
                reranker_model=reranker_model,
                session_uuid=str(analysis_session.session_uuid)
            )
            
            logger.info(f"💾 Сохранено {len(saved_results)} результатов анализа")
            
            # Маскируем чувствительные данные в логах
            masked_results = []
            for result in saved_results:
                masked_result = {
                    'analysis_id': result.analysis_id,
                    'job_id': result.job_id,
                    'submission_id': str(result.submission_id)[:8] + '...',
                    'candidate_email': mask_sensitive_data(str(result.candidate_email)),
                    'rerank_score': float(result.rerank_score) if result.rerank_score is not None else 0.0,
                    'rank_position': result.rank_position
                }
                masked_results.append(masked_result)
            
            # Формируем результат
            result = {
                'job_id': job_id,
                'company_id': company_id,
                'session_uuid': str(analysis_session.session_uuid),
                'saved_results': len(saved_results),
                'session_created': True,
                'reranker_model': reranker_model,
                'analysis_details': {
                    'total_enhanced_matches': len(enhanced_matches),
                    'search_params': search_params,
                    'workflow_completed': workflow.get('step2_reranking') == 'completed',
                    'session_started': session_started.isoformat(),
                    'session_completed': analysis_session.completed_at.isoformat()
                },
                'saved_analysis_ids': [r.analysis_id for r in saved_results],
                'sample_results': masked_results[:5],  # Показываем первые 5 для проверки
                'processed_at': datetime.utcnow().isoformat(),
                'message': f'Результаты анализа сохранены: {len(saved_results)} записей'
            }
            
            logger.info(f"✅ Анализ сохранен успешно: сессия {analysis_session.session_uuid}, {len(saved_results)} результатов")
            return serialize_for_json(result)
            
        except Exception as e:
            logger.error(f"❌ Ошибка при сохранении результатов анализа для вакансии {job_id}: {str(e)}")
            # Retry логика
            if self.request.retries < self.max_retries:
                logger.info(f"🔄 Повторная попытка {self.request.retries + 1}/{self.max_retries}")
                raise self.retry(countdown=60 * (self.request.retries + 1))
            raise


@app.task(
    bind=True, 
    name='tasks.analysis.get_analysis_summary', 
    max_retries=2, 
    default_retry_delay=30,
    soft_time_limit=120,  # 2 минуты для получения сводки
    time_limit=180        # 3 минуты
)
def get_reranker_analysis_summary(self, job_id: int, limit: int = 20) -> Dict[str, Any]:
    """
    ПОЛУЧЕНИЕ СВОДКИ АНАЛИЗА: Получение последних результатов анализа для вакансии
    
    Args:
        job_id: ID вакансии
        limit: Количество результатов для получения (по умолчанию 20)
        
    Returns:
        Сводка результатов анализа
    """
    logger.info(f"📊 Получение сводки анализа для вакансии {job_id}")
    
    with get_db_session() as db:
        try:
            # Получаем последние результаты
            latest_results = reranker_analysis_result_crud.get_latest_by_job(
                db=db,
                job_id=job_id,
                limit=limit
            )
            
            if not latest_results:
                return serialize_for_json({
                    'job_id': job_id,
                    'has_analysis': False,
                    'total_results': 0,
                    'message': 'Нет результатов анализа для данной вакансии'
                })
            
            # Получаем аналитику
            analytics = reranker_analysis_result_crud.get_analytics_by_job(db=db, job_id=job_id)
            
            # Получаем последние сессии
            recent_sessions = reranker_analysis_session_crud.get_recent_sessions(
                db=db,
                job_id=job_id,
                limit=5
            )
            
            # Маскируем чувствительные данные
            masked_results = []
            for result in latest_results:
                summary = result.to_summary_dict()
                summary['candidate_email'] = mask_sensitive_data(summary['candidate_email'])
                summary['submission_id'] = summary['submission_id'][:8] + '...'
                masked_results.append(summary)
            
            # Формируем сводку
            summary = {
                'job_id': job_id,
                'has_analysis': True,
                'total_results': len(latest_results),
                'latest_results': masked_results,
                'analytics': analytics,
                'recent_sessions': [
                    {
                        'session_uuid': str(s.session_uuid)[:8] + '...',
                        'total_results': s.total_results,
                        'reranker_model': s.reranker_model,
                        'completed_at': s.completed_at.isoformat() if hasattr(s.completed_at, 'isoformat') else None
                    } for s in recent_sessions
                ],
                'retrieved_at': datetime.utcnow().isoformat()
            }
            
            logger.info(f"📋 Получена сводка анализа: {len(latest_results)} результатов")
            return serialize_for_json(summary)
            
        except Exception as e:
            logger.error(f"❌ Ошибка при получении сводки анализа для вакансии {job_id}: {str(e)}")
            # Retry логика
            if self.request.retries < self.max_retries:
                logger.info(f"🔄 Повторная попытка {self.request.retries + 1}/{self.max_retries}")
                raise self.retry(countdown=30)
            raise


@app.task(
    bind=True, 
    name='tasks.analysis.cleanup_old_analysis', 
    max_retries=2, 
    default_retry_delay=60,
    soft_time_limit=600,  # 10 минут для очистки
    time_limit=720        # 12 минут
)
def cleanup_old_analysis_results(
    self,
    days_to_keep: int = 90,
    batch_size: int = 1000
) -> Dict[str, Any]:
    """
    ОЧИСТКА СТАРЫХ РЕЗУЛЬТАТОВ: Удаление устаревших результатов анализа
    
    Args:
        days_to_keep: Количество дней для хранения результатов (по умолчанию 90)
        batch_size: Размер пакета для удаления (по умолчанию 1000)
        
    Returns:
        Результат очистки
    """
    logger.info(f"🧹 Запуск очистки результатов анализа старше {days_to_keep} дней")
    
    with get_db_session() as db:
        try:
            # Получаем дату отсечения
            cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
            
            # Подсчитываем количество записей для удаления
            old_results_count = db.query(reranker_analysis_result_crud.model).filter(
                reranker_analysis_result_crud.model.created_at < cutoff_date
            ).count()
            
            old_sessions_count = db.query(reranker_analysis_session_crud.model).filter(
                reranker_analysis_session_crud.model.created_at < cutoff_date
            ).count()
            
            if old_results_count == 0 and old_sessions_count == 0:
                logger.info("✅ Нет старых записей для удаления")
                return serialize_for_json({
                    'deleted_results': 0,
                    'deleted_sessions': 0,
                    'cutoff_date': cutoff_date.isoformat(),
                    'message': 'Нет старых записей для удаления'
                })
            
            # Удаляем старые результаты пакетами
            deleted_results = 0
            while True:
                old_results = db.query(reranker_analysis_result_crud.model).filter(
                    reranker_analysis_result_crud.model.created_at < cutoff_date
                ).limit(batch_size).all()
                
                if not old_results:
                    break
                
                for result in old_results:
                    db.delete(result)
                deleted_results += len(old_results)
                db.commit()
                
                logger.info(f"🗑️ Удалено {len(old_results)} результатов анализа (всего: {deleted_results})")
            
            # Удаляем старые сессии пакетами
            deleted_sessions = 0
            while True:
                old_sessions = db.query(reranker_analysis_session_crud.model).filter(
                    reranker_analysis_session_crud.model.created_at < cutoff_date
                ).limit(batch_size).all()
                
                if not old_sessions:
                    break
                
                for session in old_sessions:
                    db.delete(session)
                deleted_sessions += len(old_sessions)
                db.commit()
                
                logger.info(f"🗑️ Удалено {len(old_sessions)} сессий анализа (всего: {deleted_sessions})")
            
            result = {
                'deleted_results': deleted_results,
                'deleted_sessions': deleted_sessions,
                'cutoff_date': cutoff_date.isoformat(),
                'days_kept': days_to_keep,
                'completed_at': datetime.utcnow().isoformat(),
                'message': f'Очистка завершена: удалено {deleted_results} результатов и {deleted_sessions} сессий'
            }
            
            logger.info(f"✅ Очистка завершена: {deleted_results} результатов, {deleted_sessions} сессий")
            return serialize_for_json(result)
            
        except Exception as e:
            logger.error(f"❌ Ошибка при очистке старых результатов анализа: {str(e)}")
            # Retry логика
            if self.request.retries < self.max_retries:
                logger.info(f"🔄 Повторная попытка {self.request.retries + 1}/{self.max_retries}")
                raise self.retry(countdown=60)
            raise


@app.task(
    bind=True,
    name='tasks.analysis_tasks.save_analysis_results',
    soft_time_limit=300,  # 5 минут
    time_limit=360,       # 6 минут
    max_retries=3
)
def save_analysis_results(
    self,
    entity_id: str,
    analysis_data: Dict[str, Any],
    processing_type: str = 'general'
) -> Dict[str, Any]:
    """
    Универсальная задача для сохранения результатов анализа из цепочек A и B
    
    Args:
        entity_id: ID сущности (job_id для цепочки A, submission_id для цепочки B)
        analysis_data: Данные анализа (включая результаты реранкинга и AI)
        processing_type: Тип обработки ('resume_processing', 'job_processing', 'general')
        
    Returns:
        Результат сохранения
    """
    logger.info(f"💾 Сохранение результатов анализа для {processing_type}: {entity_id}")
    
    try:
        # Определяем тип сохранения на основе processing_type
        if processing_type == 'resume_processing':
            return _save_resume_analysis_results(entity_id, analysis_data)
        elif processing_type == 'job_processing':
            return _save_job_analysis_results(entity_id, analysis_data)
        else:
            return _save_general_analysis_results(entity_id, analysis_data, processing_type)
    
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения результатов анализа: {e}")
        if self.request.retries < self.max_retries:
            logger.info(f"🔄 Повторная попытка сохранения {self.request.retries + 1}/{self.max_retries}")
            raise self.retry(countdown=60)
        raise


def _save_resume_analysis_results(job_id: str, analysis_data: Dict[str, Any]) -> Dict[str, Any]:
    """Сохранение результатов анализа резюме (цепочка A)"""
    logger.info(f"💾 Сохранение результатов анализа резюме для вакансии {job_id}")
    
    try:
        with get_db_session() as db_session:
            saved_count = 0
            
            # Сохраняем основные результаты реранкинга
            matches = analysis_data.get('matches', [])
            for match in matches:
                try:
                    # Подготавливаем данные для сохранения
                    result_data = {
                        'job_id': job_id,
                        'submission_id': match.get('submission_id'),
                        'original_score': match.get('original_score', 0.0),
                        'rerank_score': match.get('rerank_score', 0.0),
                        'similarity_score': match.get('similarity_score', 0.0),
                        'metadata': {
                            'processing_type': 'resume_processing',
                            'analysis_timestamp': datetime.utcnow().isoformat(),
                            'match_data': match
                        }
                    }
                    
                    # Добавляем AI анализ если есть
                    ai_analysis = analysis_data.get('ai_analysis')
                    if ai_analysis:
                        result_data['metadata']['ai_analysis'] = ai_analysis
                    
                    # Сохраняем в БД через CRUD операцию
                    result_crud = reranker_analysis_result_crud.create(db_session, result_data)
                    if result_crud:
                        saved_count += 1
                    
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка сохранения match для submission {match.get('submission_id')}: {e}")
                    continue
        
        logger.info(f"✅ Сохранено {saved_count} результатов анализа резюме")
        return {
            'status': 'success',
            'saved_count': saved_count,
            'job_id': job_id,
            'processing_type': 'resume_processing'
        }
    
    except Exception as e:
        logger.error(f"❌ Критическая ошибка сохранения результатов резюме: {e}")
        raise


def _save_job_analysis_results(submission_id: str, analysis_data: Dict[str, Any]) -> Dict[str, Any]:
    """Сохранение результатов анализа вакансий (цепочка B)"""
    logger.info(f"💾 Сохранение результатов анализа вакансий для submission {submission_id}")
    
    try:
        with get_db_session() as db_session:
            saved_count = 0
            
            # Сохраняем основные результаты реранкинга
            matches = analysis_data.get('matches', [])
            for match in matches:
                try:
                    # Подготавливаем данные для сохранения
                    result_data = {
                        'submission_id': submission_id,
                        'job_id': match.get('job_id'),
                        'original_score': match.get('original_score', 0.0),
                        'rerank_score': match.get('rerank_score', 0.0),
                        'similarity_score': match.get('similarity_score', 0.0),
                        'metadata': {
                            'processing_type': 'job_processing',
                            'analysis_timestamp': datetime.utcnow().isoformat(),
                            'match_data': match
                        }
                    }
                    
                    # Добавляем AI анализ если есть
                    ai_analysis = analysis_data.get('ai_analysis')
                    if ai_analysis:
                        result_data['metadata']['ai_analysis'] = ai_analysis
                    
                    # Сохраняем в БД через CRUD операцию
                    result_crud = reranker_analysis_result_crud.create(db_session, result_data)
                    if result_crud:
                        saved_count += 1
                    
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка сохранения match для job {match.get('job_id')}: {e}")
                    continue
        
        logger.info(f"✅ Сохранено {saved_count} результатов анализа вакансий")
        return {
            'status': 'success',
            'saved_count': saved_count,
            'submission_id': submission_id,
            'processing_type': 'job_processing'
        }
    
    except Exception as e:
        logger.error(f"❌ Критическая ошибка сохранения результатов вакансий: {e}")
        raise


def _save_general_analysis_results(entity_id: str, analysis_data: Dict[str, Any], processing_type: str) -> Dict[str, Any]:
    """Сохранение общих результатов анализа"""
    logger.info(f"💾 Сохранение общих результатов анализа {processing_type} для {entity_id}")
    
    try:
        with get_db_session() as db_session:
            # Создаем сессию анализа
            session_data = {
                'entity_id': entity_id,
                'processing_type': processing_type,
                'metadata': {
                    'analysis_data': analysis_data,
                    'timestamp': datetime.utcnow().isoformat()
                }
            }
            
            session_crud = reranker_analysis_session_crud.create(db_session, session_data)
        
        logger.info(f"✅ Сохранены общие результаты анализа для {entity_id}")
        return {
            'status': 'success',
            'entity_id': entity_id,
            'processing_type': processing_type,
            'session_id': session_crud.id if session_crud else None
        }
    
    except Exception as e:
        logger.error(f"❌ Критическая ошибка сохранения общих результатов: {e}")
        raise
