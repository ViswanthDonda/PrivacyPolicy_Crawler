"""Authentication endpoints"""
from fastapi import APIRouter, HTTPException, Depends
from app.middleware.auth_middleware import get_current_user
from app.models.user import User
from app.schemas.user import UserResponse

router = APIRouter()


@router.get("/verify", response_model=UserResponse)
async def verify_token(
    current_user: User = Depends(get_current_user)
):
    """
    Verify Firebase token and return user info
    This endpoint is used by frontend to verify authentication
    """
    return current_user


@router.post("/register")
async def register_user(
    current_user: User = Depends(get_current_user)
):
    """
    Register user in database (called after Firebase registration)
    User is already created by get_current_user dependency
    """
    return {
        "message": "User registered successfully",
        "user": UserResponse.from_orm(current_user)
    }

