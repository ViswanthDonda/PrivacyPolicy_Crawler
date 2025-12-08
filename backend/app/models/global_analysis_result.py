"""Global analysis result model for shared analysis cache"""
from sqlalchemy import Column, String, DateTime, Text, ForeignKey, Index
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database.base import Base
import uuid


class GlobalAnalysisResult(Base):
    """Global analysis result cache - shared across all users"""
    __tablename__ = "global_analysis_results"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    global_document_id = Column(UUID(as_uuid=True), ForeignKey("global_documents.id", ondelete="CASCADE"), nullable=False, unique=True, index=True)
    document_url = Column(String(2048), nullable=False, index=True)  # For quick lookup
    text_hash = Column(String(64), nullable=False, index=True)  # For version matching
    summary_100_words = Column(Text)
    summary_one_sentence = Column(String(500))
    word_frequency = Column(JSONB)  # Top 50 words with counts
    measurements = Column(JSONB)  # 10 text mining measurements
    analysis_model = Column(String(20), default='groq')  # 'groq' or 'gemini' (for tracking)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    global_document = relationship("GlobalDocument", backref="analysis")
    
    # Indexes for performance
    __table_args__ = (
        Index('idx_global_analysis_url_hash', 'document_url', 'text_hash'),
        Index('idx_global_analysis_hash', 'text_hash'),
    )

    def __repr__(self):
        return f"<GlobalAnalysisResult(id={self.id}, document_url={self.document_url}, model={self.analysis_model})>"

