"""Link discovery for legal documents"""
from typing import List, Dict, Set
from urllib.parse import urljoin, urlparse
import re
import logging

logger = logging.getLogger(__name__)


# Document type keywords - using frontend nomenclature
DOCUMENT_KEYWORDS = {
    'privacy': [
        'privacy policy', 'privacy statement', 'privacy notice',
        'data protection', 'personal information', 'data privacy',
        'our privacy', 'how we use', 'privacy and security policy'
    ],
    'tos': [
        'terms of service', 'terms and conditions', 'terms of use',
        'terms & conditions', 'user agreement', 'terms agreement',
        'service agreement', 'conditions of use', 'service terms'
    ],
    'terms_and_conditions': [
        'terms and conditions', 'terms & conditions', 'general terms'
    ],
    'terms_of_use': [
        'terms of use', 'usage terms', 'acceptance terms'
    ]
}

# URL patterns to look for
URL_PATTERNS = [
    (r'/privacy', 'privacy'),
    (r'/privacy-policy', 'privacy'),
    (r'/privacy_policy', 'privacy'),
    (r'/terms', 'tos'),
    (r'/terms-of-service', 'tos'),
    (r'/terms_of_service', 'tos'),
    (r'/terms-and-conditions', 'terms_and_conditions'),
    (r'/terms_and_conditions', 'terms_and_conditions'),
    (r'/terms-of-use', 'terms_of_use'),
    (r'/terms_of_use', 'terms_of_use'),
    (r'/legal/privacy', 'privacy'),
    (r'/legal/terms', 'tos'),
    (r'/conditions', 'terms_and_conditions'),
]


class LinkFinder:
    """Find legal document links using multiple strategies"""
    
    def __init__(self, base_url: str):
        """
        Initialize link finder
        
        Args:
            base_url: Base URL of the website being crawled
        """
        self.base_url = base_url
        self.base_domain = urlparse(base_url).netloc
        self.found_links = {}  # {doc_type: [(url, link_text, score)]}
    
    def find_links(self, links: List[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
        """
        Find legal document links from a list of links
        
        Args:
            links: List of link dictionaries with 'url', 'text', 'href'
        
        Returns:
            Dictionary mapping document_type to list of matched links
        """
        for link in links:
            self._analyze_link(link)
        
        # Sort by score and return
        result = {}
        for doc_type, matches in self.found_links.items():
            # Sort by score descending and deduplicate
            unique_urls = set()
            deduplicated = []
            
            for url, text, score in sorted(matches, key=lambda x: x[2], reverse=True):
                if url not in unique_urls:
                    unique_urls.add(url)
                    deduplicated.append({
                        'url': url,
                        'text': text,
                        'confidence_score': score
                    })
            
            result[doc_type] = deduplicated
        
        return result
    
    def _analyze_link(self, link: Dict[str, str]):
        """Analyze a single link for document type matching"""
        url = link['url']
        text = link['text'].lower()
        href = link.get('href', '').lower()
        
        # Skip if not from same domain or invalid URLs
        if not self._is_valid_link(url):
            return
        
        # Check URL patterns (highest confidence)
        doc_type = self._check_url_patterns(href)
        if doc_type:
            self._add_link(doc_type, url, text, 100)
            return
        
        # Check keywords in link text
        doc_type = self._check_keywords(text)
        if doc_type:
            self._add_link(doc_type, url, text, 80)
            return
        
        # Check title attribute if available
        title = link.get('title', '').lower()
        if title:
            doc_type = self._check_keywords(title)
            if doc_type:
                self._add_link(doc_type, url, text, 70)
    
    def _check_url_patterns(self, href: str) -> str:
        """Check if href matches known URL patterns"""
        for pattern, doc_type in URL_PATTERNS:
            if re.search(pattern, href, re.IGNORECASE):
                return doc_type
        return None
    
    def _check_keywords(self, text: str) -> str:
        """Check if text contains document keywords"""
        # Score by number of keyword matches
        scores = {}
        
        for doc_type, keywords in DOCUMENT_KEYWORDS.items():
            score = sum(1 for keyword in keywords if keyword in text)
            if score > 0:
                scores[doc_type] = score
        
        if scores:
            # Return the type with highest score
            return max(scores, key=scores.get)
        
        return None
    
    def _is_valid_link(self, url: str) -> bool:
        """Check if link is valid for crawling"""
        try:
            parsed = urlparse(url)
            
            # Must have http or https
            if parsed.scheme not in ['http', 'https']:
                return False
            
            # Prefer same domain (but allow cross-domain for now)
            # Future enhancement: Add configurable domain filtering option
            
            # Skip common non-document URLs
            skip_patterns = [
                r'\.(jpg|jpeg|png|gif|svg|pdf|zip|exe|dmg)$',
                r'#.*$',  # Fragments
                r'javascript:',  # JavaScript links
                r'mailto:',  # Email links
                r'tel:',  # Phone links
            ]
            
            for pattern in skip_patterns:
                if re.search(pattern, url, re.IGNORECASE):
                    return False
            
            return True
        except Exception as e:
            logger.error(f"Error validating link {url}: {e}")
            return False
    
    def _add_link(self, doc_type: str, url: str, text: str, score: int):
        """Add a found link to results"""
        if doc_type not in self.found_links:
            self.found_links[doc_type] = []
        
        self.found_links[doc_type].append((url, text, score))


def find_document_links(links: List[Dict[str, str]], base_url: str) -> Dict[str, List[Dict[str, str]]]:
    """
    Main function to find document links
    
    Args:
        links: List of all links from HTML
        base_url: Base URL
    
    Returns:
        Dictionary of document_type -> list of matched links
    """
    finder = LinkFinder(base_url)
    return finder.find_links(links)
