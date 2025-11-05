"""AnalysisResult model"""
from sqlalchemy import Column, String, DateTime, ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database.base import Base
import uuid


class AnalysisResult(Base):
    """Analysis result model"""
    __tablename__ = "analysis_results"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    document_id = Column(UUID(as_uuid=True), ForeignKey("documents.id", ondelete="CASCADE"), nullable=False, unique=True, index=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    summary_100_words = Column(Text)
    summary_one_sentence = Column(String(500))
    word_frequency = Column(JSONB)  # Top 50 words with counts
    measurements = Column(JSONB)   # 10 text mining measurements
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    document = relationship("Document", back_populates="analysis")
    user = relationship("User", backref="analysis_results")

    def __repr__(self):
        return f"<AnalysisResult(id={self.id}, document_id={self.document_id})>"