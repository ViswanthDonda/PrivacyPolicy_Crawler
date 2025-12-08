"""Service for managing global document cache"""
from typing import Optional, List
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_
from app.models.global_document import GlobalDocument
from app.crawler.text_extractor import calculate_text_hash
from urllib.parse import urlparse
import logging

logger = logging.getLogger(__name__)


class GlobalDocumentService:
    """Service for global document caching"""
    
    CACHE_VALIDITY_DAYS = 30  # Documents are fresh for 30 days
    
    @staticmethod
    def normalize_base_url(url: str) -> str:
        """Extract base URL from full URL"""
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        # Remove www. for consistency
        if base.startswith('https://www.'):
            base = base.replace('https://www.', 'https://')
        elif base.startswith('http://www.'):
            base = base.replace('http://www.', 'http://')
        return base
    
    @staticmethod
    def find_cached_documents(
        db: Session,
        base_url: str,
        document_types: Optional[List[str]] = None
    ) -> List[GlobalDocument]:
        """
        Find cached documents for a base URL
        
        Args:
            db: Database session
            base_url: Base URL to search for
            document_types: Optional list of document types to filter
        
        Returns:
            List of cached GlobalDocument objects
        """
        try:
            normalized_base = GlobalDocumentService.normalize_base_url(base_url)
            cutoff_date = datetime.utcnow() - timedelta(days=GlobalDocumentService.CACHE_VALIDITY_DAYS)
            
            query = db.query(GlobalDocument).filter(
                and_(
                    GlobalDocument.base_url == normalized_base,
                    GlobalDocument.last_crawled >= cutoff_date,
                    GlobalDocument.crawl_status == 'fresh'
                )
            )
            
            if document_types:
                query = query.filter(GlobalDocument.document_type.in_(document_types))
            
            return query.all()
            
        except Exception as e:
            logger.error(f"Error finding cached documents: {e}")
            return []
    
    @staticmethod
    def find_cached_document_by_url(
        db: Session,
        document_url: str
    ) -> Optional[GlobalDocument]:
        """Find a specific cached document by URL"""
        try:
            return db.query(GlobalDocument).filter(
                GlobalDocument.document_url == document_url
            ).first()
        except Exception as e:
            logger.error(f"Error finding cached document: {e}")
            return None
    
    @staticmethod
    def store_document(
        db: Session,
        document_url: str,
        document_type: str,
        raw_text: str,
        base_url: str,
        title: Optional[str] = None,
        word_count: Optional[int] = None
    ) -> GlobalDocument:
        """
        Store or update a document in global cache
        
        Args:
            db: Database session
            document_url: Full document URL
            document_type: Type of document
            raw_text: Document text content
            base_url: Base URL from original crawl (normalized)
            title: Document title
            word_count: Word count
        
        Returns:
            GlobalDocument object
        """
        try:
            # Use provided base_url (from original crawl) instead of extracting from document_url
            normalized_base = GlobalDocumentService.normalize_base_url(base_url)
            text_hash = calculate_text_hash(raw_text)
            
            # Check if document exists
            existing = db.query(GlobalDocument).filter(
                GlobalDocument.document_url == document_url
            ).first()
            
            if existing:
                # Check if content changed
                if existing.text_hash != text_hash:
                    # Content changed - increment version
                    existing.raw_text = raw_text
                    existing.text_hash = text_hash
                    existing.title = title or existing.title
                    existing.word_count = word_count or existing.word_count
                    existing.version += 1
                    existing.last_crawled = datetime.utcnow()
                    existing.crawl_status = 'fresh'
                    existing.updated_at = datetime.utcnow()
                    logger.info(f"Updated global document {document_url} to version {existing.version}")
                else:
                    # Content unchanged - just update timestamp
                    existing.last_crawled = datetime.utcnow()
                    existing.crawl_status = 'fresh'
                    logger.info(f"Refreshed timestamp for global document {document_url}")
                
                db.commit()
                db.refresh(existing)
                return existing
            else:
                # New document
                new_doc = GlobalDocument(
                    base_url=normalized_base,
                    document_url=document_url,
                    document_type=document_type,
                    title=title,
                    raw_text=raw_text,
                    text_hash=text_hash,
                    word_count=word_count or len(raw_text.split()),
                    crawl_status='fresh'
                )
                db.add(new_doc)
                db.commit()
                db.refresh(new_doc)
                logger.info(f"Stored new global document {document_url}")
                return new_doc
                
        except Exception as e:
            logger.error(f"Error storing global document: {e}")
            db.rollback()
            raise
    
    @staticmethod
    def mark_as_stale(
        db: Session,
        document_url: str,
        reason: str = "Failed to crawl"
    ):
        """Mark a document as stale"""
        try:
            doc = db.query(GlobalDocument).filter(
                GlobalDocument.document_url == document_url
            ).first()
            
            if doc:
                doc.crawl_status = 'stale'
                doc.updated_at = datetime.utcnow()
                db.commit()
                logger.warning(f"Marked document {document_url} as stale: {reason}")
        except Exception as e:
            logger.error(f"Error marking document as stale: {e}")
            db.rollback()
    
    @staticmethod
    def search_documents(
        db: Session,
        search_query: Optional[str] = None,
        page: int = 1,
        limit: int = 20
    ) -> tuple[List[GlobalDocument], int]:
        """
        Search global documents by URL (base_url or document_url)
        
        Args:
            db: Database session
            search_query: Search term (searches in base_url and document_url)
            page: Page number (1-indexed)
            limit: Number of results per page
        
        Returns:
            Tuple of (list of documents, total count)
        """
        try:
            from sqlalchemy import or_
            
            query = db.query(GlobalDocument)
            
            if search_query:
                # Search in both base_url and document_url
                search_pattern = f"%{search_query.lower()}%"
                query = query.filter(
                    or_(
                        GlobalDocument.base_url.ilike(search_pattern),
                        GlobalDocument.document_url.ilike(search_pattern)
                    )
                )
            
            # Get total count
            total = query.count()
            
            # Apply pagination
            offset = (page - 1) * limit
            documents = query.order_by(GlobalDocument.last_crawled.desc()).offset(offset).limit(limit).all()
            
            return documents, total
            
        except Exception as e:
            logger.error(f"Error searching documents: {e}")
            return [], 0
    
    @staticmethod
    def delete_document(
        db: Session,
        document_id: Optional[str] = None,
        document_url: Optional[str] = None
    ) -> bool:
        """
        Delete a global document and its analysis (cascade)
        
        Args:
            db: Database session
            document_id: Document ID (UUID)
            document_url: Document URL (alternative to document_id)
        
        Returns:
            True if deleted, False if not found
        """
        try:
            if document_id:
                doc = db.query(GlobalDocument).filter(GlobalDocument.id == document_id).first()
            elif document_url:
                doc = db.query(GlobalDocument).filter(GlobalDocument.document_url == document_url).first()
            else:
                logger.error("Either document_id or document_url must be provided")
                return False
            
            if doc:
                document_url_deleted = doc.document_url
                # Delete document (analysis will be cascade deleted)
                db.delete(doc)
                db.commit()
                logger.info(f"Deleted global document: {document_url_deleted}")
                return True
            else:
                logger.warning(f"Document not found for deletion (id={document_id}, url={document_url})")
                return False
                
        except Exception as e:
            logger.error(f"Error deleting document: {e}")
            db.rollback()
            return False

