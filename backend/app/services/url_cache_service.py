# COMMENTED OUT - URL Cache Service (Temporarily Disabled)
# Service for managing user-specific URL caching

"""
from typing import Optional
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_
from app.models.crawl_session import CrawlSession, SessionStatus
from urllib.parse import urlparse
from uuid import UUID
import logging

logger = logging.getLogger(__name__)


class UrlCacheService:
    \"\"\"Service for user-specific 30-day URL caching\"\"\"
    
    CACHE_VALIDITY_DAYS = 30  # URLs are cached for 30 days
    
    @staticmethod
    def normalize_url(url: str) -> str:
        \"\"\"
        Normalize URL for consistent matching
        
        Handles:
        - www. prefix removal
        - Trailing slash removal
        - http/https scheme normalization (prefers https)
        - Lowercase domain
        
        Args:
            url: URL string to normalize
            
        Returns:
            Normalized URL string
        \"\"\"
        if not url:
            return ""
        
        try:
            # Parse URL
            parsed = urlparse(url)
            
            # Normalize scheme (prefer https if available)
            scheme = parsed.scheme.lower() if parsed.scheme else 'https'
            if scheme not in ['http', 'https']:
                scheme = 'https'
            
            # Normalize netloc (domain)
            netloc = parsed.netloc.lower() if parsed.netloc else ''
            
            # Remove www. prefix for consistency
            if netloc.startswith('www.'):
                netloc = netloc[4:]
            
            # Normalize path (remove trailing slash)
            path = parsed.path.rstrip('/') if parsed.path else ''
            
            # Reconstruct URL (without query and fragment for matching)
            normalized = f"{scheme}://{netloc}{path}"
            
            return normalized
            
        except Exception as e:
            logger.error(f"Error normalizing URL {url}: {e}")
            # Return original URL if normalization fails
            return url.rstrip('/')
    
    @staticmethod
    def find_cached_session(
        db: Session,
        user_id: UUID,
        url: str
    ) -> Optional[CrawlSession]:
        \"\"\"
        Find a cached crawl session for a user and URL within 30 days
        
        Args:
            db: Database session
            user_id: User ID to search for
            url: URL to search for (will be normalized)
            
        Returns:
            CrawlSession object if found, None otherwise
        \"\"\"
        try:
            # Normalize URL for matching
            normalized_url = UrlCacheService.normalize_url(url)
            
            # Calculate cutoff date (30 days ago)
            cutoff_date = datetime.utcnow() - timedelta(days=UrlCacheService.CACHE_VALIDITY_DAYS)
            
            # Query for completed sessions within 30 days
            # We need to normalize URLs in the database for comparison
            # Since we can't normalize in SQL easily, we'll fetch and filter
            # For better performance, we'll query by user_id, status, and date first
            sessions = db.query(CrawlSession).filter(
                and_(
                    CrawlSession.user_id == user_id,
                    CrawlSession.status == SessionStatus.COMPLETED,
                    CrawlSession.created_at >= cutoff_date
                )
            ).order_by(CrawlSession.created_at.desc()).all()
            
            # Filter by normalized URL match
            for session in sessions:
                session_normalized = UrlCacheService.normalize_url(session.url)
                if session_normalized == normalized_url:
                    logger.info(f"Cache HIT for user {user_id}, URL {url} (normalized: {normalized_url})")
                    return session
            
            logger.info(f"Cache MISS for user {user_id}, URL {url} (normalized: {normalized_url})")
            return None
            
        except Exception as e:
            logger.error(f"Error finding cached session for user {user_id}, URL {url}: {e}")
            return None
    
    @staticmethod
    def is_url_cached(
        db: Session,
        user_id: UUID,
        url: str
    ) -> bool:
        \"\"\"
        Check if a URL is cached for a user (boolean check)
        
        Args:
            db: Database session
            user_id: User ID to check
            url: URL to check
            
        Returns:
            True if cached, False otherwise
        \"\"\"
        cached_session = UrlCacheService.find_cached_session(db, user_id, url)
        return cached_session is not None
    
    @staticmethod
    def get_cached_session_id(
        db: Session,
        user_id: UUID,
        url: str
    ) -> Optional[UUID]:
        \"\"\"
        Get cached session ID if URL is cached
        
        Args:
            db: Database session
            user_id: User ID to check
            url: URL to check
            
        Returns:
            Session ID (UUID) if cached, None otherwise
        \"\"\"
        cached_session = UrlCacheService.find_cached_session(db, user_id, url)
        return cached_session.id if cached_session else None
"""
