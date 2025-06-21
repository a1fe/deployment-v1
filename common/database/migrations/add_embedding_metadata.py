"""
Создание таблицы для метаданных эмбеддингов

Revision ID: add_embedding_metadata
Revises: previous_migration_id
Create Date: 2025-06-16
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = 'add_embedding_metadata'
down_revision = None  # Замените на ID предыдущей миграции
branch_labels = None
depends_on = None


def upgrade():
    """Создание таблицы embedding_metadata"""
    op.create_table(
        'embedding_metadata',
        sa.Column('embedding_id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('source_type', sa.String(50), nullable=False),
        sa.Column('source_id', sa.String(255), nullable=False),
        sa.Column('chroma_document_id', sa.String(255), nullable=False, unique=True),
        sa.Column('collection_name', sa.String(100), nullable=False),
        sa.Column('text_content', sa.Text, nullable=False),
        sa.Column('model_name', sa.String(100), nullable=False, server_default='nomic-embed-text-v1'),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.current_timestamp()),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.current_timestamp()),
        sa.Column('additional_metadata', postgresql.JSON),
    )
    
    # Создаем индексы
    op.create_index('idx_embedding_source', 'embedding_metadata', ['source_type', 'source_id'])
    op.create_index('idx_embedding_chroma_id', 'embedding_metadata', ['chroma_document_id'])
    op.create_index('idx_embedding_collection', 'embedding_metadata', ['collection_name'])
    
    # Создаем уникальное ограничение
    op.create_unique_constraint('uq_embedding_source', 'embedding_metadata', ['source_type', 'source_id'])


def downgrade():
    """Удаление таблицы embedding_metadata"""
    op.drop_table('embedding_metadata')
