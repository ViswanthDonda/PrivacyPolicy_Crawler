"""Create global_analysis_results table

Revision ID: abc123def456
Revises: d79419d3cb0c
Create Date: 2025-01-07

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '66e2c98c2bff'
down_revision: Union[str, None] = 'd79419d3cb0c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create global_analysis_results table
    op.create_table(
        'global_analysis_results',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('global_document_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('document_url', sa.String(length=2048), nullable=False),
        sa.Column('text_hash', sa.String(length=64), nullable=False),
        sa.Column('summary_100_words', sa.Text(), nullable=True),
        sa.Column('summary_one_sentence', sa.String(length=500), nullable=True),
        sa.Column('word_frequency', postgresql.JSONB(), nullable=True),
        sa.Column('measurements', postgresql.JSONB(), nullable=True),
        sa.Column('analysis_model', sa.String(length=20), server_default='groq', nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['global_document_id'], ['global_documents.id'], ondelete='CASCADE'),
    )
    
    # Create indexes for performance
    op.create_index('idx_global_analysis_url_hash', 'global_analysis_results', ['document_url', 'text_hash'])
    op.create_index('idx_global_analysis_hash', 'global_analysis_results', ['text_hash'])
    op.create_index('idx_global_analysis_doc_id', 'global_analysis_results', ['global_document_id'], unique=True)
    op.create_index('idx_global_analysis_url', 'global_analysis_results', ['document_url'])


def downgrade() -> None:
    # Drop indexes
    op.drop_index('idx_global_analysis_url', table_name='global_analysis_results')
    op.drop_index('idx_global_analysis_doc_id', table_name='global_analysis_results')
    op.drop_index('idx_global_analysis_hash', table_name='global_analysis_results')
    op.drop_index('idx_global_analysis_url_hash', table_name='global_analysis_results')
    
    # Drop table
    op.drop_table('global_analysis_results')

