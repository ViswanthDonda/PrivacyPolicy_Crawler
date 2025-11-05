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
            logger.error(f"Error crawling {url}: {e}")
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
            async with self.session.get(url) as response:
                response.raise_for_status()
                html_content = await response.text()
                
                # Try to extract title quickly
                soup = parse_html(html_content)
                page_title = get_page_title(soup)
                
                return html_content, page_title
                
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            raise ValueError(f"Failed to fetch page: {str(e)}")
    
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
                    logger.error(f"Error processing document {link['url']}: {e}")
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
            logger.error(f"Error processing document {url}: {e}")
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
