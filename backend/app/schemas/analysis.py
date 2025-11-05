"""Analysis result schemas"""
from pydantic import BaseModel, Field
from typing import Dict, List, Any
from uuid import UUID
from datetime import datetime


class WordFrequencyItem(BaseModel):
    """Word frequency item"""
    word: str
    count: int


class TextMeasurements(BaseModel):
    """Text mining measurements"""
    word_count: int = 0
    sentence_count: int = 0
    average_words_per_sentence: float = 0.0
    flesch_reading_ease: float = 0.0
    flesch_kincaid_grade: float = 0.0
    automated_readability_index: float = 0.0
    sentiment_score: float = 0.0  # -1 to 1
    keyword_density: float = 0.0  # percentage
    legal_clause_count: int = 0
    risk_indicator_score: float = 0.0  # 0 to 100


class AnalysisResponse(BaseModel):
    """Analysis result response"""
    id: UUID
    document_id: UUID
    summary_100_words: str
    summary_one_sentence: str
    word_frequency: Dict[str, int] = Field(default_factory=dict)
    measurements: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    
    class Config:
        from_attributes = True


class DocumentAnalysisResponse(BaseModel):
    """Complete document with analysis"""
    document_id: UUID
    url: str
    document_type: str
    title: str | None
    word_count: int
    created_at: datetime
    analysis: AnalysisResponse | None = None


class SessionAnalysisResponse(BaseModel):
    """Crawl session with all documents and analyses"""
    session_id: UUID
    url: str
    status: str
    document_count: int
    analyzed_count: int
    created_at: datetime
    documents: List[DocumentAnalysisResponse] = Field(default_factory=list)

