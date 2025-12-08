"""Admin schemas"""
from pydantic import BaseModel, Field
from typing import Optional, List
from uuid import UUID
from datetime import datetime


class GlobalDocumentResponse(BaseModel):
    """Global document response schema"""
    id: UUID
    base_url: str
    document_url: str
    document_type: str
    title: Optional[str] = None
    word_count: Optional[int] = None
    last_crawled: datetime
    crawl_status: str
    version: int
    has_analysis: bool = False  # Will be set based on relationship
    created_at: datetime
    
    class Config:
        from_attributes = True


class GlobalDocumentSearchResponse(BaseModel):
    """Search response schema"""
    total: int
    page: int
    limit: int
    documents: List[GlobalDocumentResponse]


class DeleteDocumentRequest(BaseModel):
    """Delete document request schema"""
    document_url: Optional[str] = None
    document_id: Optional[UUID] = None


class DeleteDocumentResponse(BaseModel):
    """Delete document response schema"""
    success: bool
    message: str
    deleted_document_url: Optional[str] = None

