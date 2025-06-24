#!/usr/bin/env python3

import sys
import logging
print("sys.path:", sys.path)
try:
    import common.models.candidates as cmc
    print("Submission id:", id(cmc.Submission))
except Exception as e:
    print("Submission import error:", e)

from database.config import database
from database.operations.candidate_operations import SubmissionCRUD
from database.operations.company_operations import JobCRUD
from database.operations.embedding_operations import embedding_crud
from database.operations.analysis_operations import RerankerAnalysisResultCRUD
from common.models.candidates import Submission
from common.models.analysis_results import RerankerAnalysisResult
from common.utils.chroma_config import chroma_client, ChromaConfig
from common.utils.reranker_config import get_reranker_client
from uuid import UUID
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("manual_rerank")

analysis_crud = RerankerAnalysisResultCRUD()


def rerank_jobs_for_resume(submission_id: str, top_k: int = 50):
    db = database.get_session()
    try:
        submission = SubmissionCRUD().get_by_id(db, UUID(submission_id))
        if not submission or not getattr(submission, 'resume_raw_text', None):
            logger.error(f"No resume or text for submission {submission_id}")
            return
        resume_text = str(submission.resume_raw_text)[:32000]
        job_collection = chroma_client.get_job_collection()
        if job_collection.count() == 0:
            logger.warning("Job collection is empty")
            return
        search_results = job_collection.query(
            query_texts=[resume_text],
            n_results=min(top_k, job_collection.count()),
            include=['documents', 'metadatas', 'distances']
        )
        documents = search_results['documents'][0]
        metadatas = search_results['metadatas'][0]
        distances = search_results['distances'][0]
        reranker = get_reranker_client()
        reranked_results = reranker.rerank_texts(resume_text, documents)
        logger.info(f"Reranked jobs for resume {submission_id}: {reranked_results}")
        # Сохраняем результаты в БД
        batch_results = []
        for rank_position, (doc_idx, rerank_score) in enumerate(reranked_results, 1):
            if doc_idx >= len(metadatas):
                continue
            metadata = metadatas[doc_idx]
            job_id = metadata.get('source_id')
            if not job_id:
                continue
            job = JobCRUD().get_by_id(db, job_id)
            if not job:
                continue
            original_distance = distances[doc_idx] if doc_idx < len(distances) else 0.0
            original_similarity = max(0.0, 1.0 - original_distance)
            normalized_rerank_score = max(0.0, min(1.0, (rerank_score + 10.0) / 20.0))
            final_score = (original_similarity * 0.3) + (normalized_rerank_score * 0.7)
            score_improvement = normalized_rerank_score - original_similarity
            search_params = {
                'top_k': top_k,
                'min_similarity': 0.0,
                'min_rerank_score': -10.0,
                'search_type': 'resume_to_jobs',
                'query_text_length': len(resume_text),
                'original_text_length': len(str(submission.resume_raw_text)),
                'text_truncated': len(str(submission.resume_raw_text)) > 32000
            }
            workflow_stats = {
                'total_candidates_found': len(documents),
                'reranked_candidates': len(reranked_results),
                'processing_time': datetime.utcnow().isoformat(),
                'chroma_collection': ChromaConfig.JOB_COLLECTION
            }
            analysis_result = RerankerAnalysisResult(
                job_id=job_id,
                submission_id=submission.submission_id,
                original_similarity=original_similarity,
                rerank_score=rerank_score,
                final_score=final_score,
                score_improvement=score_improvement,
                rank_position=rank_position,
                search_params=search_params,
                reranker_model=reranker.model_name,
                workflow_stats=workflow_stats,
                job_title=job.title or 'Unknown',
                company_id=job.company_id,
                candidate_name=f"{submission.candidate.first_name or ''} {submission.candidate.last_name or ''}".strip() or 'Unknown',
                candidate_email=submission.candidate.email or 'Unknown',
                total_candidates_found=len(documents),
                analysis_type='resume_to_jobs_rerank'
            )
            batch_results.append(analysis_result)
        if batch_results:
            db.add_all(batch_results)
            db.commit()
            logger.info(f"Saved {len(batch_results)} rerank results for resume {submission_id}")
    finally:
        db.close()

def rerank_resumes_for_job(job_id: int, top_k: int = 50):
    db = database.get_session()
    try:
        job = JobCRUD().get_by_id(db, job_id)
        if not job or not getattr(job, 'job_description_raw_text', None):
            logger.error(f"No job or text for job {job_id}")
            return
        job_text = str(job.job_description_raw_text)[:32000]
        resume_collection = chroma_client.get_resume_collection()
        if resume_collection.count() == 0:
            logger.warning("Resume collection is empty")
            return
        search_results = resume_collection.query(
            query_texts=[job_text],
            n_results=min(top_k, resume_collection.count()),
            include=['documents', 'metadatas', 'distances']
        )
        documents = search_results['documents'][0]
        metadatas = search_results['metadatas'][0]
        distances = search_results['distances'][0]
        reranker = get_reranker_client()
        reranked_results = reranker.rerank_texts(job_text, documents)
        logger.info(f"Reranked resumes for job {job_id}: {reranked_results}")
        # Сохраняем результаты в БД
        batch_results = []
        for rank_position, (doc_idx, rerank_score) in enumerate(reranked_results, 1):
            if doc_idx >= len(metadatas):
                continue
            metadata = metadatas[doc_idx]
            source_id = metadata.get('source_id')
            if not source_id:
                continue
            submission = SubmissionCRUD().get_by_id(db, source_id)
            if not submission:
                continue
            original_distance = distances[doc_idx] if doc_idx < len(distances) else 0.0
            original_similarity = max(0.0, 1.0 - original_distance)
            normalized_rerank_score = max(0.0, min(1.0, (rerank_score + 10.0) / 20.0))
            final_score = (original_similarity * 0.3) + (normalized_rerank_score * 0.7)
            score_improvement = normalized_rerank_score - original_similarity
            search_params = {
                'top_k': top_k,
                'min_similarity': 0.0,
                'min_rerank_score': -10.0,
                'search_type': 'job_to_resumes',
                'query_text_length': len(job_text),
                'original_text_length': len(str(job.job_description_raw_text)),
                'text_truncated': len(str(job.job_description_raw_text)) > 32000
            }
            workflow_stats = {
                'total_candidates_found': len(documents),
                'reranked_candidates': len(reranked_results),
                'processing_time': datetime.utcnow().isoformat(),
                'chroma_collection': ChromaConfig.RESUME_COLLECTION
            }
            analysis_result = RerankerAnalysisResult(
                job_id=job_id,
                submission_id=submission.submission_id,
                original_similarity=original_similarity,
                rerank_score=rerank_score,
                final_score=final_score,
                score_improvement=score_improvement,
                rank_position=rank_position,
                search_params=search_params,
                reranker_model=reranker.model_name,
                workflow_stats=workflow_stats,
                job_title=job.title or 'Unknown',
                company_id=job.company_id,
                candidate_name=f"{submission.candidate.first_name or ''} {submission.candidate.last_name or ''}".strip() or 'Unknown',
                candidate_email=submission.candidate.email or 'Unknown',
                total_candidates_found=len(documents),
                analysis_type='job_to_resumes_rerank'
            )
            batch_results.append(analysis_result)
        if batch_results:
            db.add_all(batch_results)
            db.commit()
            logger.info(f"Saved {len(batch_results)} rerank results for job {job_id}")
    finally:
        db.close()

def rerank_all_resumes_to_jobs(top_k: int = 50):
    db = database.get_session()
    try:
        submissions = db.query(Submission).filter(
            Submission.resume_raw_text.isnot(None),
            Submission.resume_raw_text != ''
        ).all()
        logger.info(f"Found {len(submissions)} resumes for full rerank.")
        for submission in submissions:
            rerank_jobs_for_resume(str(submission.submission_id), top_k)
    finally:
        db.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python manual_rerank.py [resume|job|all] <id> [top_k]")
        sys.exit(1)
    mode = sys.argv[1]
    if mode == "all":
        top_k = int(sys.argv[2]) if len(sys.argv) > 2 else 50
        rerank_all_resumes_to_jobs(top_k)
        sys.exit(0)
    if len(sys.argv) < 3:
        print("Usage: python manual_rerank.py [resume|job|all] <id> [top_k]")
        sys.exit(1)
    id_arg = sys.argv[2]
    top_k = int(sys.argv[3]) if len(sys.argv) > 3 else 50
    if mode == "resume":
        rerank_jobs_for_resume(id_arg, top_k)
    elif mode == "job":
        rerank_resumes_for_job(int(id_arg), top_k)
    else:
        print("Unknown mode. Use 'resume', 'job' or 'all'.")
        sys.exit(1)
