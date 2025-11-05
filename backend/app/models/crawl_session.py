"""CrawlSession model"""
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Enum as SQLEnum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database.base import Base
import uuid
import enum


class SessionStatus(str, enum.Enum):
    """Crawl session status"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class CrawlSession(Base):
    """Crawl session model"""
    __tablename__ = "crawl_sessions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    url = Column(String(2048), nullable=False)
    status = Column(SQLEnum(SessionStatus), default=SessionStatus.PENDING, nullable=False, index=True)
    document_count = Column(Integer, default=0)
    analyzed_count = Column(Integer, default=0)
    error_message = Column(String(1000))
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    user = relationship("User", backref="crawl_sessions")
    documents = relationship("Document", back_populates="session", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<CrawlSession(id={self.id}, user_id={self.user_id}, url={self.url}, status={self.status})>"

