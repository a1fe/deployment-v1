"""
Tasks package for HR Analysis system (Deployment Version)

Contains only essential Celery tasks for two main workflows:
- workflows: Main processing chains
- fillout_tasks: Data retrieval from Fillout API
- embedding_tasks: Vector embeddings generation
- matching: Job-resume matching and similarity search
- scoring_tasks: BGE reranking and scoring
- analysis_tasks: Results storage and analysis
"""

# Import all task modules to ensure they are registered with Celery
from . import workflows
from . import fillout_tasks
from . import embedding_tasks
from . import matching
from . import scoring_tasks
from . import analysis_tasks
