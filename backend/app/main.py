"""FastAPI application entry point"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import settings, CORS_ORIGINS
from app.api.v1 import auth, crawler, documents, users, admin

app = FastAPI(
    title=settings.APP_NAME,
    version="0.1.0",
    description="Backend API for TOS Privacy Policy Crawler",
    debug=settings.DEBUG
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Health check endpoint
@app.get("/")
async def root():
    """Root endpoint - health check"""
    return {
        "status": "healthy",
        "service": settings.APP_NAME,
        "version": "0.1.0"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "environment": settings.APP_ENV
    }

# Include routers
app.include_router(auth.router, prefix=f"{settings.API_V1_PREFIX}/auth", tags=["Authentication"])
app.include_router(crawler.router, prefix=f"{settings.API_V1_PREFIX}/crawler", tags=["Crawler"])
app.include_router(documents.router, prefix=f"{settings.API_V1_PREFIX}/documents", tags=["Documents"])
app.include_router(users.router, prefix=f"{settings.API_V1_PREFIX}/users", tags=["Users"])
app.include_router(admin.router, prefix=f"{settings.API_V1_PREFIX}/admin", tags=["Admin"])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG
    )

