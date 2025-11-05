"""Crawler schemas"""
from pydantic import BaseModel, HttpUrl, Field
from uuid import UUID
from datetime import datetime
from typing import List, Optional
from enum import Enum


class DocumentType(str, Enum):
    """Document types - matching frontend nomenclature"""
    PRIVACY = "privacy"
    TOS = "tos"
    TERMS_AND_CONDITIONS = "terms_and_conditions"
    TERMS_OF_USE = "terms_of_use"


class SessionStatus(str, Enum):
    """Crawl session status"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class CrawlRequest(BaseModel):
    """Crawl request schema"""
    url: HttpUrl
    document_types: List[DocumentType] = Field(
        default=[DocumentType.PRIVACY, DocumentType.TOS],
        description="Types of documents to find"
    )


class CrawlResponse(BaseModel):
    """Crawl response schema"""
    session_id: UUID
    url: str
    status: SessionStatus
    created_at: datetime

    class Config:
        from_attributes = True


class CrawlStatusResponse(BaseModel):
    """Crawl status response"""
    id: UUID
    url: str
    status: SessionStatus
    document_count: int = 0
    analyzed_count: int = 0
    error_message: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class CrawlHistoryResponse(BaseModel):
    """Crawl history response"""
    sessions: List[CrawlStatusResponse]
    pagination: dict  # Will use common.Pagination

