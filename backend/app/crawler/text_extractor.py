"""Text extraction and cleaning utilities"""
from bs4 import BeautifulSoup
from typing import Optional
import re
import hashlib
import logging

logger = logging.getLogger(__name__)


def extract_text(soup: BeautifulSoup) -> str:
    """
    Extract clean text from HTML
    
    Args:
        soup: BeautifulSoup object
    
    Returns:
        Clean plain text
    """
    try:
        # Get text
        text = soup.get_text()
        
        # Clean whitespace
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        return text
    except Exception as e:
        logger.error(f"Error extracting text: {e}")
        return ""


def is_valid_document(text: str, min_length: int = 100) -> bool:
    """
    Validate if extracted text is a valid document
    
    Args:
        text: Extracted text
        min_length: Minimum acceptable length
    
    Returns:
        True if valid, False otherwise
    """
    if not text or len(text) < min_length:
        return False
    
    # Check if mostly text (not just numbers or symbols)
    words = text.split()
    if len(words) < 20:  # Too few words
        return False
    
    # Check if has meaningful content (at least some longer words)
    long_words = [w for w in words if len(w) > 4]
    if len(long_words) < 10:
        return False
    
    return True


def calculate_text_hash(text: str) -> str:
    """
    Calculate hash of text for deduplication
    
    Args:
        text: Text to hash
    
    Returns:
        SHA256 hash as hex string
    """
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def count_words(text: str) -> int:
    """Count words in text"""
    if not text:
        return 0
    
    words = re.findall(r'\b\w+\b', text)
    return len(words)


def extract_metadata(soup: BeautifulSoup) -> dict:
    """
    Extract metadata from HTML
    
    Args:
        soup: BeautifulSoup object
    
    Returns:
        Dictionary of metadata
    """
    metadata = {}
    
    try:
        # Title
        title_tag = soup.find('title')
        if title_tag:
            metadata['title'] = title_tag.get_text().strip()
        
        # Meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            metadata['description'] = meta_desc['content'].strip()
        
        # Meta keywords
        meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
        if meta_keywords and meta_keywords.get('content'):
            metadata['keywords'] = meta_keywords['content'].strip()
        
        # Last modified
        meta_modified = soup.find('meta', attrs={'name': 'last-modified'}) or \
                       soup.find('meta', attrs={'http-equiv': 'last-modified'})
        if meta_modified and meta_modified.get('content'):
            metadata['last_modified'] = meta_modified['content'].strip()
        
        # Language
        lang = soup.find('html', lang=True)
        if lang:
            metadata['language'] = lang['lang']
        
    except Exception as e:
        logger.error(f"Error extracting metadata: {e}")
    
    return metadata
