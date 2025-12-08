"""Application configuration"""
from pydantic_settings import BaseSettings
from typing import List
import os


class Settings(BaseSettings):
    """Application settings"""
    
    # Application
    APP_NAME: str = "TOS Privacy Policy Crawler"
    APP_ENV: str = "development"
    DEBUG: bool = True
    
    # Server
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    
    # Database - override via environment variable in production
    DATABASE_URL: str = "postgresql://user:password@localhost:5432/tos_crawler"
    
    # Redis
    REDIS_URL: str = "redis://localhost:6379"
    
    # Firebase Admin SDK
    FIREBASE_PROJECT_ID: str = ""
    FIREBASE_PRIVATE_KEY: str = ""
    FIREBASE_CLIENT_EMAIL: str = ""
    
    # Google Gemini API (backup only - use with extreme caution)
    GEMINI_API_KEY: str = ""
    
    # Groq API (primary analysis service)
    GROQ_API_KEY: str = ""
    
    # CORS
    CORS_ORIGINS: str = "http://localhost:3000"
    
    # Rate Limiting
    RATE_LIMIT_PER_MINUTE: int = 10
    
    # Cache TTL (seconds)
    CACHE_TTL_ANALYSIS: int = 2592000  # 30 days
    CACHE_TTL_FAVORITES: int = 3600    # 1 hour
    CACHE_TTL_SESSION: int = 86400     # 1 day
    
    # API
    API_V1_PREFIX: str = "/api/v1"
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# Parse CORS origins
def get_cors_origins(origins_str: str) -> List[str]:
    """Parse comma-separated CORS origins"""
    return [origin.strip() for origin in origins_str.split(",")]


# Global settings instance
settings = Settings()

# Get CORS origins list
CORS_ORIGINS = get_cors_origins(settings.CORS_ORIGINS)

