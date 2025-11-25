"""Database models"""
from app.models.user import User
from app.models.crawl_session import CrawlSession
from app.models.document import Document
from app.models.analysis_result import AnalysisResult
from app.models.user_favorite import UserFavorite
from app.models.global_document import GlobalDocument

__all__ = [
    "User",
    "CrawlSession",
    "Document",
    "AnalysisResult",
    "UserFavorite",
    "GlobalDocument"
]

