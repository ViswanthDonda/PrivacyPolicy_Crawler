"""Admin authorization middleware"""
from fastapi import HTTPException, Depends
from app.middleware.auth_middleware import get_current_user
from app.models.user import User


async def get_admin_user(
    current_user: User = Depends(get_current_user)
) -> User:
    """
    Dependency to ensure user is admin
    
    Args:
        current_user: Current authenticated user
        
    Returns:
        User object if admin
        
    Raises:
        HTTPException: 403 if user is not admin
    """
    if not current_user.is_admin:
        raise HTTPException(
            status_code=403,
            detail="Admin access required"
        )
    return current_user

