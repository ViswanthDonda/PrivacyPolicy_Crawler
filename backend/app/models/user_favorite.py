"""UserFavorite model"""
from sqlalchemy import Column, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database.base import Base
import uuid


class UserFavorite(Base):
    """User favorite documents model"""
    __tablename__ = "user_favorites"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    document_id = Column(UUID(as_uuid=True), ForeignKey("documents.id", ondelete="CASCADE"), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    user = relationship("User", backref="favorites")
    document = relationship("Document", back_populates="favorites")

    # Unique constraint
    __table_args__ = (
        UniqueConstraint('user_id', 'document_id', name='unique_user_document_favorite'),
    )

    def __repr__(self):
        return f"<UserFavorite(id={self.id}, user_id={self.user_id}, document_id={self.document_id})>"

