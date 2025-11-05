"""HTML parsing utilities using BeautifulSoup"""
from bs4 import BeautifulSoup
from typing import List, Optional, Dict
import logging

logger = logging.getLogger(__name__)


def parse_html(html_content: str, parser: str = 'lxml') -> BeautifulSoup:
    """
    Parse HTML content using BeautifulSoup
    
    Args:
        html_content: Raw HTML string
        parser: Parser to use ('lxml', 'html.parser', 'html5lib')
    
    Returns:
        BeautifulSoup object
    """
    try:
        soup = BeautifulSoup(html_content, parser)
        return soup
    except Exception as e:
        logger.error(f"Error parsing HTML: {e}")
        # Try fallback parser
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            return soup
        except Exception as e2:
            logger.error(f"Fallback parser also failed: {e2}")
            raise ValueError(f"Failed to parse HTML: {str(e)}")


def get_page_title(soup: BeautifulSoup) -> Optional[str]:
    """Extract page title from HTML"""
    try:
        title_tag = soup.find('title')
        return title_tag.get_text().strip() if title_tag else None
    except Exception as e:
        logger.error(f"Error extracting title: {e}")
        return None


def get_all_links(soup: BeautifulSoup, base_url: str) -> List[Dict[str, str]]:
    """
    Extract all links from HTML
    
    Args:
        soup: BeautifulSoup parsed HTML
        base_url: Base URL for resolving relative links
    
    Returns:
        List of link dictionaries with 'url' and 'text'
    """
    links = []
    try:
        for anchor in soup.find_all('a', href=True):
            href = anchor['href'].strip()
            link_text = anchor.get_text().strip()
            
            # Convert relative URLs to absolute
            absolute_url = resolve_url(href, base_url)
            
            links.append({
                'url': absolute_url,
                'text': link_text,
                'href': href
            })
    except Exception as e:
        logger.error(f"Error extracting links: {e}")
    
    return links


def resolve_url(relative_url: str, base_url: str) -> str:
    """
    Convert relative URL to absolute URL
    
    Args:
        relative_url: Relative or absolute URL
        base_url: Base URL
    
    Returns:
        Absolute URL
    """
    if not relative_url:
        return base_url
    
    # Already absolute URL
    if relative_url.startswith(('http://', 'https://')):
        return relative_url
    
    # Data URIs, javascript:, mailto:, etc.
    if ':' in relative_url:
        return base_url  # Skip these
    
    # Remove anchors and query params for now
    relative_url = relative_url.split('#')[0]
    
    from urllib.parse import urljoin
    return urljoin(base_url, relative_url)


def clean_html(soup: BeautifulSoup) -> BeautifulSoup:
    """
    Remove unwanted HTML elements
    
    Args:
        soup: BeautifulSoup object
    
    Returns:
        Cleaned BeautifulSoup object (mutated in place)
    """
    # Remove script and style elements
    for element in soup(['script', 'style', 'meta', 'link', 'noscript']):
        element.decompose()
    
    return soup
