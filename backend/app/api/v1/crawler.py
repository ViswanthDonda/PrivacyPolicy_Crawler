"""Crawler endpoints"""
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from typing import List
from uuid import UUID, uuid4
from datetime import datetime
from app.middleware.auth_middleware import get_current_user
from app.models.user import User
from app.models.crawl_session import CrawlSession, SessionStatus
from app.models.document import Document
from app.models.analysis_result import AnalysisResult
from app.schemas.crawler import (
    CrawlRequest,
    CrawlResponse,
    CrawlStatusResponse
)
from app.schemas.analysis import AnalysisResponse, DocumentAnalysisResponse, SessionAnalysisResponse
from app.database.base import get_db
from app.services.crawler_service import CrawlerService
from app.services.groq_service import GroqService
from app.services.gemini_service import GeminiService
from app.services.global_document_service import GlobalDocumentService
from app.services.global_analysis_service import GlobalAnalysisService
from app.utils.url_normalizer import normalize_crawl_url
from sqlalchemy.orm import Session
import asyncio
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


async def crawl_task(session_id: UUID, url: str, user_id: UUID, force_refresh: bool = False):
    """
    Background task for crawling and analysis (creates its own DB session)
    
    Args:
        session_id: Session ID
        url: URL to crawl (already normalized)
        user_id: User ID
        force_refresh: If True, bypasses both document and analysis cache
    """
    from app.database.base import SessionLocal
    
    db = SessionLocal()
    try:
        # Update status to processing
        session = db.query(CrawlSession).filter(CrawlSession.id == session_id).first()
        if session:
            session.status = SessionStatus.PROCESSING
            db.commit()
        
        # Get original base_url for storing documents (normalized from entry point)
        original_base_url = GlobalDocumentService.normalize_base_url(url)
        logger.info(f"Processing crawl for base_url: {original_base_url} (force_refresh={force_refresh})")
        
        # Check global cache first (unless force_refresh is True)
        cached_docs = []
        if not force_refresh:
            cached_docs = GlobalDocumentService.find_cached_documents(
                db=db,
                base_url=url,
                document_types=['privacy_policy', 'terms_of_service', 'privacy', 'tos']
            )
            if cached_docs:
                logger.info(f"Found {len(cached_docs)} cached documents for {url} - using cache")
        else:
            logger.info(f"force_refresh=True: Bypassing global document cache for {url} - will crawl fresh")
        
        document_count = 0
        analyzed_count = 0
        documents_to_analyze = []
        cache_hit = False
        global_documents_map = {}  # Map document_url -> GlobalDocument for analysis lookup
        
        if cached_docs:
            logger.info(f"Found {len(cached_docs)} cached documents for {url} - using cache")
            cache_hit = True
            
            # Use cached documents
            for cached_doc in cached_docs:
                # Create user-specific document from cached data
                document = Document(
                    user_id=user_id,
                    session_id=session_id,
                    url=cached_doc.document_url,
                    document_type=cached_doc.document_type,
                    title=cached_doc.title,
                    raw_text=cached_doc.raw_text,
                    text_hash=cached_doc.text_hash,
                    word_count=cached_doc.word_count
                )
                db.add(document)
                db.flush()  # Get document ID
                
                # Store global document reference for analysis lookup
                global_documents_map[cached_doc.document_url] = cached_doc
                
                # Collect documents for analysis
                documents_to_analyze.append({
                    'document': document,
                    'global_document': cached_doc,
                    'text': cached_doc.raw_text,
                    'url': cached_doc.document_url,
                    'doc_type': cached_doc.document_type,
                    'text_hash': cached_doc.text_hash,
                    'from_cache': True
                })
                
                document_count += 1
        else:
            logger.info(f"No cached documents found for {url} - crawling fresh")
            # No cache - proceed with normal crawl
            async with CrawlerService() as crawler:
                result = await crawler.crawl_url(url)
            
            # Store documents in global cache and create user documents
            for doc_type, docs in result.get('documents', {}).items():
                for doc in docs:
                    # Store in global cache with original base_url
                    try:
                        global_doc = GlobalDocumentService.store_document(
                            db=db,
                            document_url=doc['url'],
                            document_type=doc['document_type'],
                            raw_text=doc['text'],
                            base_url=original_base_url,  # Use original crawl URL's base
                            title=doc.get('title'),
                            word_count=doc['word_count']
                        )
                        global_documents_map[doc['url']] = global_doc
                    except Exception as cache_error:
                        logger.warning(f"Error storing in global cache: {cache_error}")
                        # Continue even if cache storage fails
                        global_doc = None
                    
                    # Create user-specific document
                    document = Document(
                        user_id=user_id,
                        session_id=session_id,
                        url=doc['url'],
                        document_type=doc['document_type'],
                        title=doc.get('title'),
                        raw_text=doc['text'],
                        text_hash=doc['text_hash'],
                        word_count=doc['word_count']
                    )
                    db.add(document)
                    db.flush()  # Get document ID
                    
                    # Collect documents for analysis
                    documents_to_analyze.append({
                        'document': document,
                        'global_document': global_doc,
                        'text': doc['text'],
                        'url': doc['url'],
                        'doc_type': doc['document_type'],
                        'text_hash': doc['text_hash'],
                        'from_cache': False
                    })
                    
                    document_count += 1
        
        # Commit documents first
        db.commit()
        
        if cache_hit:
            logger.info(f"Using {len(cached_docs)} cached documents - saved crawling time")
        
        # Analyze documents - check global analysis cache first (unless force_refresh)
        groq_service = GroqService()
        gemini_service = None  # Only initialize if absolutely necessary
        
        # Track Gemini usage to prevent quota exhaustion
        gemini_usage_today = 0
        MAX_GEMINI_USAGE_PER_DAY = 15  # Conservative limit (20/day max, keep 5 buffer)
        
        for doc_info in documents_to_analyze:
            try:
                global_doc = doc_info.get('global_document')
                document_url = doc_info['url']
                text_hash = doc_info['text_hash']
                analysis_result = None
                used_gemini = False
                analysis_model = 'groq'
                
                # Check global analysis cache (unless force_refresh)
                global_analysis = None
                if not force_refresh and global_doc:
                    global_analysis = GlobalAnalysisService.find_analysis(
                        db=db,
                        document_url=document_url,
                        text_hash=text_hash
                    )
                
                if global_analysis:
                    # Use cached analysis - no API call needed!
                    logger.info(f"Using cached analysis for {document_url} (hash: {text_hash[:8]}...)")
                    analysis_result = {
                        'summary_100_words': global_analysis.summary_100_words,
                        'summary_one_sentence': global_analysis.summary_one_sentence,
                        'word_frequency': global_analysis.word_frequency or {},
                        'measurements': global_analysis.measurements or {}
                    }
                    analysis_model = global_analysis.analysis_model
                else:
                    # No cached analysis or force_refresh - run fresh analysis
                    if force_refresh:
                        logger.info(f"force_refresh=True: Running fresh analysis for {document_url}")
                    else:
                        logger.info(f"No cached analysis found for {document_url} - analyzing fresh")
                    
                    # PRIMARY: Try Groq first
                    try:
                        logger.info(f"Attempting analysis with Groq (Llama) for {document_url}")
                        analysis_result = await groq_service.analyze_document(
                            text=doc_info['text'],
                            url=document_url,
                            doc_type=doc_info['doc_type']
                        )
                        logger.info(f"Successfully analyzed with Groq for {document_url}")
                        analysis_model = 'groq'
                        
                    except Exception as groq_error:
                        # Groq failed - check if we should use Gemini
                        error_type = type(groq_error).__name__
                        error_msg = str(groq_error).lower()
                        
                        # STRICT SAFEGUARDS: Only use Gemini in very specific cases
                        should_use_gemini = False
                        
                        # Check 1: Is it a rate limit error? (429)
                        if "429" in error_msg or "rate limit" in error_msg or "quota" in error_msg:
                            logger.warning(f"Groq rate limit hit for {document_url}, considering Gemini fallback")
                            should_use_gemini = True
                        
                        # Check 2: Is it a service unavailable error? (503)
                        elif "503" in error_msg or "service unavailable" in error_msg or "unavailable" in error_msg:
                            logger.warning(f"Groq service unavailable for {document_url}, considering Gemini fallback")
                            should_use_gemini = True
                        
                        # Check 3: Is it a timeout error?
                        elif "timeout" in error_msg or "timed out" in error_msg:
                            logger.warning(f"Groq timeout for {document_url}, considering Gemini fallback")
                            should_use_gemini = True
                        
                        # For all other errors (auth, invalid request, parsing, etc.), DO NOT use Gemini
                        else:
                            logger.error(f"Groq error (non-fallback type) for {document_url}: {groq_error}")
                            logger.error(f"Error type: {error_type}, Message: {error_msg}")
                            logger.error("NOT using Gemini fallback - this error type is not eligible for fallback")
                            should_use_gemini = False
                        
                        # Check 4: Have we exceeded Gemini daily limit?
                        if should_use_gemini and gemini_usage_today >= MAX_GEMINI_USAGE_PER_DAY:
                            logger.error(f"Gemini daily limit reached ({gemini_usage_today}/{MAX_GEMINI_USAGE_PER_DAY}). NOT using Gemini fallback.")
                            should_use_gemini = False
                        
                        # Check 5: Is Gemini API key configured?
                        if should_use_gemini:
                            from app.config import settings
                            if not settings.GEMINI_API_KEY:
                                logger.error("Gemini API key not configured. Cannot use Gemini fallback.")
                                should_use_gemini = False
                        
                        # FINAL DECISION: Use Gemini only if all checks pass
                        if should_use_gemini:
                            logger.warning(f"⚠️  EMERGENCY FALLBACK: Using Gemini for {document_url} (Groq failed: {error_type})")
                            
                            # Initialize Gemini service only if needed
                            if gemini_service is None:
                                try:
                                    gemini_service = GeminiService()
                                except Exception as gemini_init_error:
                                    logger.error(f"Failed to initialize Gemini service: {gemini_init_error}")
                                    raise groq_error  # Re-raise original Groq error
                            
                            # Try Gemini with error handling
                            try:
                                analysis_result = await gemini_service.analyze_document(
                                    text=doc_info['text'],
                                    url=document_url,
                                    doc_type=doc_info['doc_type']
                                )
                                gemini_usage_today += 1
                                used_gemini = True
                                analysis_model = 'gemini'
                                logger.warning(f"⚠️  Gemini fallback successful for {document_url} (Usage: {gemini_usage_today}/{MAX_GEMINI_USAGE_PER_DAY})")
                            except Exception as gemini_error:
                                logger.error(f"Gemini fallback also failed for {document_url}: {gemini_error}")
                                # Re-raise original Groq error, not Gemini error
                                raise groq_error
                        else:
                            # Do not use Gemini - re-raise the original Groq error
                            logger.error(f"Analysis failed for {document_url} - NOT using Gemini fallback")
                            raise groq_error
                    
                    # Store analysis in global cache if we have a global_document
                    if analysis_result and global_doc:
                        try:
                            GlobalAnalysisService.store_analysis(
                                db=db,
                                global_document_id=global_doc.id,
                                document_url=document_url,
                                text_hash=text_hash,
                                analysis_data=analysis_result,
                                analysis_model=analysis_model,
                                force_replace=force_refresh  # Force replace if admin did force_refresh
                            )
                            logger.info(f"Stored analysis in global cache for {document_url}")
                        except Exception as cache_error:
                            logger.warning(f"Error storing analysis in global cache: {cache_error}")
                            # Continue even if cache storage fails
                
                # Store analysis in user's analysis_results
                if analysis_result:
                    analysis = AnalysisResult(
                        document_id=doc_info['document'].id,
                        user_id=user_id,
                        summary_100_words=analysis_result.get('summary_100_words', ''),
                        summary_one_sentence=analysis_result.get('summary_one_sentence', ''),
                        word_frequency=analysis_result.get('word_frequency', {}),
                        measurements=analysis_result.get('measurements', {})
                    )
                    db.add(analysis)
                    analyzed_count += 1
                    
                    if used_gemini:
                        logger.warning(f"⚠️  Document analyzed using Gemini fallback (should be rare!)")
                else:
                    logger.error(f"No analysis result for {document_url}")
                
            except Exception as analysis_error:
                logger.error(f"Error analyzing document {doc_info['url']}: {analysis_error}")
                logger.error(f"Analysis failed completely - document will be stored without analysis")
                # Continue with other documents even if one fails
        
        # Update session
        if session:
            session.status = SessionStatus.COMPLETED
            session.document_count = document_count
            session.analyzed_count = analyzed_count
            db.commit()
        
        logger.info(f"Crawl completed for session {session_id}: {document_count} documents, {analyzed_count} analyzed")
        
    except Exception as e:
        logger.error(f"Error in crawl task for session {session_id}: {e}")
        # Update session status to failed
        session = db.query(CrawlSession).filter(CrawlSession.id == session_id).first()
        if session:
            session.status = SessionStatus.FAILED
            session.error_message = str(e)[:500]  # Truncate error message
            db.commit()
    finally:
        db.close()


@router.post("/analyze", response_model=CrawlResponse)
async def start_crawl(
    request: CrawlRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Start crawling and analysis for a URL
    
    Args:
        request: Crawl request with URL and document types
        background_tasks: FastAPI background tasks
        current_user: Authenticated user
        db: Database session
    
    Returns:
        CrawlResponse with session_id and status
    """
    # If force_refresh is True, require admin access
    if request.force_refresh and not current_user.is_admin:
        raise HTTPException(
            status_code=403,
            detail="Admin access required for force refresh. This feature updates the global cache for all users."
        )
    
    try:
        # Normalize URL at entry point
        normalized_url = normalize_crawl_url(str(request.url))
        logger.info(f"Normalized URL: {str(request.url)} → {normalized_url}")
        
        # Create crawl session with normalized URL
        session = CrawlSession(
            id=uuid4(),
            user_id=current_user.id,
            url=normalized_url,
            status=SessionStatus.PENDING
        )
        db.add(session)
        db.commit()
        db.refresh(session)
        
        # Start background task with normalized URL
        background_tasks.add_task(
            crawl_task,
            session.id,
            normalized_url,
            current_user.id,
            request.force_refresh
        )
        
        return CrawlResponse(
            session_id=session.id,
            url=normalized_url,
            status=session.status,
            created_at=session.created_at
        )
        
    except Exception as e:
        logger.error(f"Error starting crawl: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start crawl: {str(e)}"
        )


@router.get("/status/{session_id}", response_model=CrawlStatusResponse)
async def get_crawl_status(
    session_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get status of a crawl session
    
    Args:
        session_id: Session ID
        current_user: Authenticated user
        db: Database session
    
    Returns:
        CrawlStatusResponse with session details
    """
    # Get session (with user isolation)
    session = db.query(CrawlSession).filter(
        CrawlSession.id == session_id,
        CrawlSession.user_id == current_user.id
    ).first()
    
    if not session:
        raise HTTPException(
            status_code=404,
            detail="Session not found"
        )
    
    return session


@router.get("/session/{session_id}/results", response_model=SessionAnalysisResponse)
async def get_session_results(
    session_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get full session results with all documents and analyses
    
    Args:
        session_id: Session ID
        current_user: Authenticated user
        db: Database session
    
    Returns:
        SessionAnalysisResponse with all documents and analyses
    """
    # Get session (with user isolation)
    session = db.query(CrawlSession).filter(
        CrawlSession.id == session_id,
        CrawlSession.user_id == current_user.id
    ).first()
    
    if not session:
        raise HTTPException(
            status_code=404,
            detail="Session not found"
        )
    
    # Get all documents for this session
    documents = db.query(Document).filter(
        Document.session_id == session_id,
        Document.user_id == current_user.id
    ).all()
    
    # Build document responses with analyses
    document_responses = []
    for doc in documents:
        analysis = doc.analysis
        document_responses.append(
            DocumentAnalysisResponse(
                document_id=doc.id,
                url=doc.url,
                document_type=doc.document_type,
                title=doc.title,
                word_count=doc.word_count,
                created_at=doc.created_at,
                analysis=AnalysisResponse(
                    id=analysis.id,
                    document_id=analysis.document_id,
                    summary_100_words=analysis.summary_100_words,
                    summary_one_sentence=analysis.summary_one_sentence,
                    word_frequency=analysis.word_frequency or {},
                    measurements=analysis.measurements or {},
                    created_at=analysis.created_at
                ) if analysis else None
            )
        )
    
    return SessionAnalysisResponse(
        session_id=session.id,
        url=session.url,
        status=session.status,
        document_count=session.document_count,
        analyzed_count=session.analyzed_count,
        created_at=session.created_at,
        documents=document_responses
    )


@router.get("/history", response_model=List[CrawlStatusResponse])
async def get_crawl_history(
    page: int = 1,
    page_size: int = 20,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get user's crawl history with pagination
    
    Args:
        page: Page number (1-indexed)
        page_size: Items per page
        current_user: Authenticated user
        db: Database session
    
    Returns:
        List of crawl sessions
    """
    # Pagination
    offset = (page - 1) * page_size
    
    # Get user's sessions (user isolation)
    sessions = db.query(CrawlSession).filter(
        CrawlSession.user_id == current_user.id
    ).order_by(
        CrawlSession.created_at.desc()
    ).offset(offset).limit(page_size).all()
    
    return sessions


@router.delete("/session/{session_id}")
async def delete_crawl_session(
    session_id: UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Delete a crawl session and all associated data
    
    Args:
        session_id: Session ID to delete
        current_user: Authenticated user
        db: Database session
    
    Returns:
        Success message
    """
    # Get session (with user isolation)
    session = db.query(CrawlSession).filter(
        CrawlSession.id == session_id,
        CrawlSession.user_id == current_user.id
    ).first()
    
    if not session:
        raise HTTPException(
            status_code=404,
            detail="Session not found"
        )
    
    # Delete session (cascade will handle documents and analyses)
    db.delete(session)
    db.commit()
    
    return {"success": True, "message": "Session deleted successfully"}

