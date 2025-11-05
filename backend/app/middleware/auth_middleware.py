"""Authentication middleware"""
from fastapi import HTTPException, Depends, Header
from typing import Optional
from app.services.auth_service import verify_firebase_token
from app.database.base import get_db
from app.models.user import User
from sqlalchemy.orm import Session

async def get_current_user(
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db)
) -> User:
    """
    Dependency to get current authenticated user
    """
    if not authorization:
        raise HTTPException(
            status_code=401,
            detail="Authorization header missing"
        )
    
    try:
        # Extract token from "Bearer <token>"
        scheme, token = authorization.split()
        if scheme.lower() != "bearer":
            raise HTTPException(
                status_code=401,
                detail="Invalid authentication scheme"
            )
    except ValueError:
        raise HTTPException(
            status_code=401,
            detail="Invalid authorization header format"
        )
    
    # Verify Firebase token
    try:
        firebase_user = await verify_firebase_token(token)
    except ValueError as e:
        raise HTTPException(
            status_code=401,
            detail=str(e)
        )
    
    # Get or create user in database
    user = db.query(User).filter(
        User.firebase_uid == firebase_user["firebase_uid"]
    ).first()
    
    if not user:
        # Create user if doesn't exist
        user = User(
            firebase_uid=firebase_user["firebase_uid"],
            email=firebase_user["email"],
            display_name=firebase_user.get("name"),
            username=firebase_user.get("email", "").split("@")[0] if firebase_user.get("email") else None
        )
        db.add(user)
        db.commit()
        db.refresh(user)
    
    return user

