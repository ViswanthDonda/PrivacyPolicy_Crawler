"""Admin endpoints"""
from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Optional
from sqlalchemy.orm import Session
from app.middleware.admin_middleware import get_admin_user
from app.middleware.auth_middleware import get_current_user
from app.models.user import User
from app.models.global_document import GlobalDocument
from app.models.global_analysis_result import GlobalAnalysisResult
from app.schemas.admin import (
    GlobalDocumentSearchResponse,
    GlobalDocumentResponse,
    DeleteDocumentRequest,
    DeleteDocumentResponse
)
from app.services.global_document_service import GlobalDocumentService
from app.database.base import get_db
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/global-documents", response_model=GlobalDocumentSearchResponse)
async def search_global_documents(
    search: Optional[str] = Query(None, description="Search term for base_url or document_url"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Results per page"),
    current_user: User = Depends(get_admin_user),
    db: Session = Depends(get_db)
):
    """
    Search global documents (Admin only)
    
    Args:
        search: Search term to filter by base_url or document_url
        page: Page number (1-indexed)
        limit: Number of results per page (max 100)
        current_user: Admin user
        db: Database session
    
    Returns:
        List of global documents with pagination
    """
    try:
        documents, total = GlobalDocumentService.search_documents(
            db=db,
            search_query=search,
            page=page,
            limit=limit
        )
        
        # Convert to response format and check for analysis
        document_responses = []
        for doc in documents:
            # Check if analysis exists
            has_analysis = db.query(GlobalAnalysisResult).filter(
                GlobalAnalysisResult.global_document_id == doc.id
            ).first() is not None
            
            document_responses.append(
                GlobalDocumentResponse(
                    id=doc.id,
                    base_url=doc.base_url,
                    document_url=doc.document_url,
                    document_type=doc.document_type,
                    title=doc.title,
                    word_count=doc.word_count,
                    last_crawled=doc.last_crawled,
                    crawl_status=doc.crawl_status,
                    version=doc.version,
                    has_analysis=has_analysis,
                    created_at=doc.created_at
                )
            )
        
        return GlobalDocumentSearchResponse(
            total=total,
            page=page,
            limit=limit,
            documents=document_responses
        )
        
    except Exception as e:
        logger.error(f"Error searching global documents: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to search documents: {str(e)}"
        )


@router.delete("/global-documents/{document_id}", response_model=DeleteDocumentResponse)
async def delete_global_document(
    document_id: str,
    current_user: User = Depends(get_admin_user),
    db: Session = Depends(get_db)
):
    """
    Delete a global document and its analysis (Admin only)
    
    Args:
        document_id: Document ID (UUID)
        current_user: Admin user
        db: Database session
    
    Returns:
        Success message
    """
    try:
        # Get document first to get URL for response
        doc = db.query(GlobalDocument).filter(GlobalDocument.id == document_id).first()
        
        if not doc:
            raise HTTPException(
                status_code=404,
                detail="Document not found"
            )
        
        document_url = doc.document_url
        
        # Delete document (analysis will be cascade deleted)
        success = GlobalDocumentService.delete_document(
            db=db,
            document_id=document_id
        )
        
        if success:
            logger.info(f"Admin {current_user.email} deleted document: {document_url}")
            return DeleteDocumentResponse(
                success=True,
                message="Document and analysis deleted successfully",
                deleted_document_url=document_url
            )
        else:
            raise HTTPException(
                status_code=500,
                detail="Failed to delete document"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting global document: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete document: {str(e)}"
        )


@router.post("/global-documents/delete-by-url", response_model=DeleteDocumentResponse)
async def delete_global_document_by_url(
    request: DeleteDocumentRequest,
    current_user: User = Depends(get_admin_user),
    db: Session = Depends(get_db)
):
    """
    Delete a global document by URL (Admin only)
    
    Args:
        request: Delete request with document_url
        current_user: Admin user
        db: Database session
    
    Returns:
        Success message
    """
    try:
        if not request.document_url:
            raise HTTPException(
                status_code=400,
                detail="document_url is required"
            )
        
        # Delete document (analysis will be cascade deleted)
        success = GlobalDocumentService.delete_document(
            db=db,
            document_url=request.document_url
        )
        
        if success:
            logger.info(f"Admin {current_user.email} deleted document by URL: {request.document_url}")
            return DeleteDocumentResponse(
                success=True,
                message="Document and analysis deleted successfully",
                deleted_document_url=request.document_url
            )
        else:
            raise HTTPException(
                status_code=404,
                detail="Document not found"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting global document by URL: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete document: {str(e)}"
        )

