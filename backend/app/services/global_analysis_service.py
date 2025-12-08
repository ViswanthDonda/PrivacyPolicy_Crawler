"""Service for managing global analysis result cache"""
from typing import Optional, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_
from app.models.global_analysis_result import GlobalAnalysisResult
from app.models.global_document import GlobalDocument
import logging

logger = logging.getLogger(__name__)


class GlobalAnalysisService:
    """Service for global analysis result caching"""
    
    @staticmethod
    def find_analysis(
        db: Session,
        document_url: str,
        text_hash: str
    ) -> Optional[GlobalAnalysisResult]:
        """
        Find cached analysis by document URL and text hash
        
        Args:
            db: Database session
            document_url: Full document URL
            text_hash: Text hash for version matching
            
        Returns:
            GlobalAnalysisResult object if found, None otherwise
        """
        try:
            analysis = db.query(GlobalAnalysisResult).filter(
                and_(
                    GlobalAnalysisResult.document_url == document_url,
                    GlobalAnalysisResult.text_hash == text_hash
                )
            ).first()
            
            if analysis:
                logger.info(f"Cache HIT for analysis: {document_url} (hash: {text_hash[:8]}...)")
            else:
                logger.info(f"Cache MISS for analysis: {document_url} (hash: {text_hash[:8]}...)")
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error finding cached analysis for {document_url}: {e}")
            return None
    
    @staticmethod
    def store_analysis(
        db: Session,
        global_document_id: str,
        document_url: str,
        text_hash: str,
        analysis_data: Dict[str, Any],
        analysis_model: str = 'groq',
        force_replace: bool = False
    ) -> GlobalAnalysisResult:
        """
        Store or update analysis in global cache
        
        Args:
            db: Database session
            global_document_id: ID of the global document
            document_url: Full document URL
            text_hash: Text hash for version matching
            analysis_data: Analysis result dictionary
            analysis_model: Model used ('groq' or 'gemini')
            force_replace: If True, replace analysis even if text_hash matches (for force_refresh)
        
        Returns:
            GlobalAnalysisResult object
        """
        try:
            # Check if analysis exists for this document URL
            existing = db.query(GlobalAnalysisResult).filter(
                GlobalAnalysisResult.document_url == document_url
            ).first()
            
            if existing:
                # Check if document content changed (different text_hash) OR force_replace is True
                if existing.text_hash != text_hash or force_replace:
                    # Document changed OR force refresh - REPLACE analysis
                    existing.text_hash = text_hash
                    existing.global_document_id = global_document_id
                    existing.summary_100_words = analysis_data.get('summary_100_words', '')
                    existing.summary_one_sentence = analysis_data.get('summary_one_sentence', '')
                    existing.word_frequency = analysis_data.get('word_frequency', {})
                    existing.measurements = analysis_data.get('measurements', {})
                    existing.analysis_model = analysis_model
                    existing.updated_at = datetime.utcnow()
                    db.commit()
                    db.refresh(existing)
                    if force_replace:
                        logger.info(f"Replaced analysis for {document_url} (force refresh, hash: {text_hash[:8]}...)")
                    else:
                        logger.info(f"Replaced analysis for {document_url} (content changed, hash: {text_hash[:8]}...)")
                    return existing
                else:
                    # Content unchanged and not force_replace - just refresh timestamp
                    existing.updated_at = datetime.utcnow()
                    db.commit()
                    db.refresh(existing)
                    logger.info(f"Refreshed analysis timestamp for {document_url}")
                    return existing
            else:
                # New analysis
                new_analysis = GlobalAnalysisResult(
                    global_document_id=global_document_id,
                    document_url=document_url,
                    text_hash=text_hash,
                    summary_100_words=analysis_data.get('summary_100_words', ''),
                    summary_one_sentence=analysis_data.get('summary_one_sentence', ''),
                    word_frequency=analysis_data.get('word_frequency', {}),
                    measurements=analysis_data.get('measurements', {}),
                    analysis_model=analysis_model
                )
                db.add(new_analysis)
                db.commit()
                db.refresh(new_analysis)
                logger.info(f"Stored new global analysis for {document_url} (hash: {text_hash[:8]}...)")
                return new_analysis
                
        except Exception as e:
            logger.error(f"Error storing global analysis for {document_url}: {e}")
            db.rollback()
            raise

