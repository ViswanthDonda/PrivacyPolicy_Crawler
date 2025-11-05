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
from app.services.gemini_service import GeminiService
from sqlalchemy.orm import Session
import asyncio
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


async def crawl_task(session_id: UUID, url: str, user_id: UUID):
    """
    Background task for crawling and analysis (creates its own DB session)
    
    Args:
        session_id: Session ID
        url: URL to crawl
        user_id: User ID
    """
    from app.database.base import SessionLocal
    
    db = SessionLocal()
    try:
        # Update status to processing
        session = db.query(CrawlSession).filter(CrawlSession.id == session_id).first()
        if session:
            session.status = SessionStatus.PROCESSING
            db.commit()
        
        # Run crawler
        async with CrawlerService() as crawler:
            result = await crawler.crawl_url(url)
        
        # Store documents
        document_count = 0
        analyzed_count = 0
        documents_to_analyze = []
        
        for doc_type, docs in result.get('documents', {}).items():
            for doc in docs:
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
                    'text': doc['text'],
                    'url': doc['url'],
                    'doc_type': doc['document_type']
                })
                
                document_count += 1
        
        # Commit documents first
        db.commit()
        
        # Analyze documents with Gemini
        gemini_service = GeminiService()
        for doc_info in documents_to_analyze:
            try:
                # Run analysis
                analysis_result = await gemini_service.analyze_document(
                    text=doc_info['text'],
                    url=doc_info['url'],
                    doc_type=doc_info['doc_type']
                )
                
                # Store analysis
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
                
            except Exception as analysis_error:
                logger.error(f"Error analyzing document {doc_info['url']}: {analysis_error}")
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
    try:
        # Create crawl session
        session = CrawlSession(
            id=uuid4(),
            user_id=current_user.id,
            url=str(request.url),
            status=SessionStatus.PENDING
        )
        db.add(session)
        db.commit()
        db.refresh(session)
        
        # Start background task
        background_tasks.add_task(
            crawl_task,
            session.id,
            str(request.url),
            current_user.id
        )
        
        return CrawlResponse(
            session_id=session.id,
            url=str(request.url),
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

