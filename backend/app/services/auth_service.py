"""Firebase authentication service"""
import firebase_admin
from firebase_admin import auth, credentials
from app.config import settings
import logging

logger = logging.getLogger(__name__)

# Initialize Firebase Admin SDK
_firebase_app = None


def init_firebase():
    """Initialize Firebase Admin SDK"""
    global _firebase_app
    
    if _firebase_app is not None:
        return _firebase_app
    
    try:
        # For production (Render), use credentials from environment
        if settings.FIREBASE_PRIVATE_KEY and settings.FIREBASE_CLIENT_EMAIL:
            cred = credentials.Certificate({
                "type": "service_account",
                "project_id": settings.FIREBASE_PROJECT_ID,
                "private_key": settings.FIREBASE_PRIVATE_KEY.replace('\\n', '\n'),
                "client_email": settings.FIREBASE_CLIENT_EMAIL,
                "token_uri": "https://oauth2.googleapis.com/token",
            })
            _firebase_app = firebase_admin.initialize_app(cred)
        else:
            logger.warning("Firebase credentials not found. Using default credentials.")
            _firebase_app = firebase_admin.initialize_app()
        
        logger.info("Firebase Admin SDK initialized successfully")
        return _firebase_app
    except Exception as e:
        logger.error(f"Error initializing Firebase: {e}")
        raise


async def verify_firebase_token(token: str) -> dict:
    """Verify Firebase ID token and return user info"""
    try:
        # Initialize if not already done
        if _firebase_app is None:
            init_firebase()
        
        # Verify the token
        decoded_token = auth.verify_id_token(token)
        
        return {
            "firebase_uid": decoded_token["uid"],
            "email": decoded_token.get("email"),
            "email_verified": decoded_token.get("email_verified", False),
            "name": decoded_token.get("name"),
            "picture": decoded_token.get("picture")
        }
    except Exception as e:
        logger.error(f"Error verifying Firebase token: {e}")
        raise ValueError(f"Invalid token: {str(e)}")
