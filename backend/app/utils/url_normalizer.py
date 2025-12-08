"""URL normalization utilities"""
from urllib.parse import urlparse
import logging

logger = logging.getLogger(__name__)


def normalize_crawl_url(url: str) -> str:
    """
    Normalize user-entered URL for consistent storage and lookup
    
    Handles:
    - Missing scheme → adds https://
    - www. prefix → removes for consistency
    - Trailing slash → removes
    - Case → lowercase domain
    - Scheme → prefers https over http
    
    Examples:
    - "example.com" → "https://example.com"
    - "www.example.com" → "https://example.com"
    - "https://www.example.com" → "https://example.com"
    - "http://example.com" → "https://example.com"
    
    Args:
        url: URL string to normalize
        
    Returns:
        Normalized URL string
    """
    if not url:
        raise ValueError("URL cannot be empty")
    
    # Add https:// if no scheme
    if not url.startswith(('http://', 'https://')):
        url = f'https://{url}'
    
    # Parse URL
    try:
        parsed = urlparse(url)
    except Exception as e:
        logger.error(f"Error parsing URL {url}: {e}")
        raise ValueError(f"Invalid URL format: {url}")
    
    # Normalize scheme (prefer https)
    scheme = 'https'
    
    # Normalize netloc (domain)
    netloc = parsed.netloc.lower()
    
    # Remove www. prefix
    if netloc.startswith('www.'):
        netloc = netloc[4:]
    
    # Normalize path (remove trailing slash)
    path = parsed.path.rstrip('/')
    
    # Reconstruct normalized URL
    if path:
        normalized = f"{scheme}://{netloc}{path}"
    else:
        normalized = f"{scheme}://{netloc}"
    
    return normalized

