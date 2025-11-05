"""Common schemas"""
from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class MessageResponse(BaseModel):
    """Standard message response"""
    message: str
    success: bool = True


class ErrorResponse(BaseModel):
    """Error response schema"""
    error: str
    detail: Optional[str] = None


class Pagination(BaseModel):
    """Pagination metadata"""
    page: int = 1
    page_size: int = 20
    total: int
    total_pages: int

