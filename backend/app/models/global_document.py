"""Global document model for shared document cache"""
from sqlalchemy import Column, String, Integer, DateTime, Text, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database.base import Base
import uuid


class GlobalDocument(Base):
    """Global document cache - shared across all users"""
    __tablename__ = "global_documents"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    base_url = Column(String(2048), nullable=False, index=True)  # e.g., "walmart.com"
    document_url = Column(String(2048), nullable=False, index=True)  # Full URL
    document_type = Column(String(50), nullable=False)  # privacy_policy, terms_of_service
    title = Column(String(500))
    raw_text = Column(Text)  # Full document text
    text_hash = Column(String(64), nullable=False, index=True)  # MD5/SHA256 for change detection
    word_count = Column(Integer)
    
    # Caching metadata
    last_crawled = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    crawl_status = Column(String(20), default='fresh')  # 'fresh', 'stale', 'failed'
    version = Column(Integer, default=1)  # For versioning when document changes
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Indexes for performance
    __table_args__ = (
        Index('idx_global_docs_url_type', 'document_url', 'document_type'),
        Index('idx_global_docs_base_url', 'base_url', 'last_crawled'),
        Index('idx_global_docs_hash', 'text_hash'),
    )

    def __repr__(self):
        return f"<GlobalDocument(id={self.id}, url={self.document_url}, type={self.document_type})>"

