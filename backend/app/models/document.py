"""Document model"""
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database.base import Base
import uuid


class Document(Base):
    """Document model"""
    __tablename__ = "documents"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    session_id = Column(UUID(as_uuid=True), ForeignKey("crawl_sessions.id", ondelete="CASCADE"), nullable=True, index=True)
    url = Column(String(2048), nullable=False)
    document_type = Column(String(50))  # privacy_policy, terms_of_service, etc.
    title = Column(String(500))
    raw_text = Column(Text)  # Full document text
    text_hash = Column(String(64))  # MD5/SHA256 for deduplication
    word_count = Column(Integer)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    user = relationship("User", backref="documents")
    session = relationship("CrawlSession", back_populates="documents")
    analysis = relationship("AnalysisResult", back_populates="document", uselist=False, cascade="all, delete-orphan")
    favorites = relationship("UserFavorite", back_populates="document", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Document(id={self.id}, url={self.url}, document_type={self.document_type})>"

