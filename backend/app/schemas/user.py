"""User schemas"""
from pydantic import BaseModel, EmailStr
from uuid import UUID
from datetime import datetime
from typing import Optional


class UserBase(BaseModel):
    """Base user schema"""
    email: EmailStr
    username: Optional[str] = None
    display_name: Optional[str] = None


class UserCreate(UserBase):
    """User creation schema"""
    firebase_uid: str


class UserResponse(UserBase):
    """User response schema"""
    id: UUID
    firebase_uid: str
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

