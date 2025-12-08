"""Main crawling service"""
import asyncio
import aiohttp
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse
import logging

from app.crawler.html_parser import parse_html, get_all_links, get_page_title, clean_html
from app.crawler.link_finder import find_document_links
from app.crawler.text_extractor import extract_text, is_valid_document, calculate_text_hash, count_words

logger = logging.getLogger(__name__)


class CrawlerService:
    """Service for crawling websites and extracting legal documents"""
    
    def __init__(self):
        """Initialize crawler service"""
        self.session = None
        self.user_agent = "Mozilla/5.0 (compatible; TOSAnalyzer/1.0)"
        self.timeout = aiohttp.ClientTimeout(total=30, connect=10)
    
    async def __aenter__(self):
        """Async context manager entry"""
        connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=self.timeout,
            headers={'User-Agent': self.user_agent}
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    async def crawl_url(self, url: str) -> Dict:
        """
        Crawl a URL and discover legal documents
        
        Args:
            url: URL to crawl
        
        Returns:
            Dictionary with discovered documents and metadata
        """
        try:
            # Normalize URL
            url = self._normalize_url(url)
            
            logger.info(f"Starting crawl for {url}")
            
            # Fetch main page
            html_content, page_title = await self._fetch_page(url)
            
            # Parse HTML
            soup = parse_html(html_content)
            
            # Clean HTML
            soup = clean_html(soup)
            
            # Extract all links
            all_links = get_all_links(soup, url)
            
            logger.info(f"Found {len(all_links)} links on {url}")
            
            # Find document links
            document_links = find_document_links(all_links, url)
            
            logger.info(f"Found document links: {sum(len(links) for links in document_links.values())}")
            
            # Fetch and process documents
            documents = await self._process_documents(document_links)
            
            return {
                'url': url,
                'page_title': page_title,
                'documents': documents,
                'document_count': sum(len(docs) for docs in documents.values()),
            }
            
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            logger.error(f"Error crawling {url}")
            logger.error(f"  Error Type: {error_type}")
            logger.error(f"  Error Message: {error_msg}")
            logger.error(f"  Error Details: {repr(e)}")
            
            # Log traceback for crawl errors
            import traceback
            logger.error(f"  Traceback: {traceback.format_exc()}")
            
            raise
    
    async def _fetch_page(self, url: str) -> tuple[str, Optional[str]]:
        """
        Fetch a page and return HTML content
        
        Args:
            url: URL to fetch
        
        Returns:
            Tuple of (html_content, page_title)
        """
        try:
            logger.info(f"Attempting to fetch: {url}")
            logger.debug(f"Request headers: User-Agent={self.user_agent}, Timeout={self.timeout}")
            
            async with self.session.get(url) as response:
                # Log response details
                logger.info(f"Response received for {url}: Status={response.status}, Headers={dict(response.headers)}")
                
                # Check status code
                if response.status >= 400:
                    # Try to get error response body for debugging
                    try:
                        error_body = await response.text()
                        logger.error(f"HTTP Error {response.status} for {url}")
                        logger.error(f"Response headers: {dict(response.headers)}")
                        logger.error(f"Response body (first 500 chars): {error_body[:500]}")
                    except Exception as body_error:
                        logger.error(f"Could not read error response body: {body_error}")
                    
                    response.raise_for_status()  # This will raise HTTPException
                
                # Success - log content info
                html_content = await response.text()
                content_length = len(html_content)
                logger.info(f"Successfully fetched {url}: Content length={content_length} bytes, Status={response.status}")
                
                # Try to extract title quickly
                soup = parse_html(html_content)
                page_title = get_page_title(soup)
                
                if page_title:
                    logger.debug(f"Page title extracted: {page_title}")
                
                return html_content, page_title
                
        except aiohttp.ClientError as e:
            # Network/HTTP client errors
            error_type = type(e).__name__
            error_msg = str(e)
            logger.error(f"HTTP Client Error fetching {url}")
            logger.error(f"  Error Type: {error_type}")
            logger.error(f"  Error Message: {error_msg}")
            logger.error(f"  Error Details: {repr(e)}")
            
            # Check for specific error types
            if isinstance(e, aiohttp.ClientResponseError):
                logger.error(f"  HTTP Status: {e.status}")
                logger.error(f"  Request Info: {e.request_info}")
                logger.error(f"  History: {e.history}")
            elif isinstance(e, aiohttp.ClientConnectorError):
                logger.error(f"  Connection Error - Cannot connect to host")
                logger.error(f"  Connection Details: {e.os_error if hasattr(e, 'os_error') else 'N/A'}")
            elif isinstance(e, aiohttp.ClientTimeout):
                logger.error(f"  Timeout Error - Request took longer than {self.timeout.total} seconds")
            elif isinstance(e, aiohttp.ServerTimeoutError):
                logger.error(f"  Server Timeout - Server did not respond in time")
            
            raise ValueError(f"Failed to fetch page: {error_type}: {error_msg}")
            
        except asyncio.TimeoutError as e:
            logger.error(f"Timeout Error fetching {url}")
            logger.error(f"  Error Type: {type(e).__name__}")
            logger.error(f"  Error Message: {str(e)}")
            logger.error(f"  Timeout Settings: Total={self.timeout.total}s, Connect={self.timeout.connect}s")
            raise ValueError(f"Failed to fetch page: Timeout after {self.timeout.total} seconds")
            
        except Exception as e:
            # Catch-all for any other exceptions
            error_type = type(e).__name__
            error_msg = str(e)
            logger.error(f"Unexpected Error fetching {url}")
            logger.error(f"  Error Type: {error_type}")
            logger.error(f"  Error Message: {error_msg}")
            logger.error(f"  Error Details: {repr(e)}")
            logger.error(f"  Error Args: {e.args if hasattr(e, 'args') else 'N/A'}")
            
            # Log traceback for unexpected errors
            import traceback
            logger.error(f"  Traceback: {traceback.format_exc()}")
            
            raise ValueError(f"Failed to fetch page: {error_type}: {error_msg}")
    
    async def _process_documents(self, document_links: Dict[str, List[Dict]]) -> Dict[str, List[Dict]]:
        """
        Fetch and process document links
        
        Args:
            document_links: Dictionary of document_type -> list of links
        
        Returns:
            Dictionary of document_type -> list of processed documents
        """
        documents = {}
        
        for doc_type, links in document_links.items():
            documents[doc_type] = []
            
            for link in links[:5]:  # Limit to 5 per type
                try:
                    doc = await self._process_single_document(link['url'], doc_type)
                    if doc:
                        documents[doc_type].append(doc)
                except Exception as e:
                    error_type = type(e).__name__
                    error_msg = str(e)
                    logger.error(f"Error processing document {link['url']} (type: {doc_type})")
                    logger.error(f"  Error Type: {error_type}")
                    logger.error(f"  Error Message: {error_msg}")
                    logger.error(f"  Error Details: {repr(e)}")
                    continue
        
        return documents
    
    async def _process_single_document(self, url: str, doc_type: str) -> Optional[Dict]:
        """
        Process a single document
        
        Args:
            url: Document URL
            doc_type: Type of document
        
        Returns:
            Document dictionary or None if invalid
        """
        try:
            # Fetch document
            html_content, page_title = await self._fetch_page(url)
            
            # Parse HTML
            soup = parse_html(html_content)
            soup = clean_html(soup)
            
            # Extract text
            text = extract_text(soup)
            
            # Validate document
            if not is_valid_document(text):
                logger.warning(f"Invalid document at {url}")
                return None
            
            # Calculate hash
            text_hash = calculate_text_hash(text)
            
            # Count words
            word_count = count_words(text)
            
            return {
                'url': url,
                'document_type': doc_type,
                'title': page_title,
                'text': text,
                'text_hash': text_hash,
                'word_count': word_count,
            }
            
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            logger.error(f"Error processing document {url} (type: {doc_type})")
            logger.error(f"  Error Type: {error_type}")
            logger.error(f"  Error Message: {error_msg}")
            logger.error(f"  Error Details: {repr(e)}")
            
            # Log traceback for processing errors
            import traceback
            logger.error(f"  Traceback: {traceback.format_exc()}")
            
            return None
    
    def _normalize_url(self, url: str) -> str:
        """
        Normalize and validate URL
        
        Args:
            url: URL string
        
        Returns:
            Normalized URL
        """
        if not url:
            raise ValueError("URL cannot be empty")
        
        # Add https:// if no scheme
        if not url.startswith(('http://', 'https://')):
            url = f'https://{url}'
        
        # Remove trailing slash for consistency
        url = url.rstrip('/')
        
        return url
