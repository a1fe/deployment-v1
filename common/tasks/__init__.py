"""
Tasks package for HR Analysis system (Deployment Version)

Contains only essential Celery tasks for two main workflows:
- workflows: Main processing chains
- fillout_tasks: Data retrieval from Fillout API
- embedding_tasks: Vector embeddings generation
- reranking_tasks: Reranking resumes and jobs
"""

# Import all task modules to ensure they are registered with Celery
from . import workflows
from . import fillout_tasks
from . import embedding_tasks
from . import reranking_tasks
