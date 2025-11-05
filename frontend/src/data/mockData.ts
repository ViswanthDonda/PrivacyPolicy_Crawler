import { Document, AnalysisResult, CrawlSession, User, TextMiningMeasurements } from '@/types'

// Mock user data
export const mockUser: User = {
  id: 'user-123',
  email: 'john.doe@example.com',
  firebase_uid: 'firebase-uid-123',
  created_at: '2024-01-01T00:00:00Z',
  updated_at: '2024-01-01T00:00:00Z',
  is_active: true
}

// Mock documents
export const mockDocuments: Document[] = [
  {
    id: 'doc-1',
    url: 'https://example.com/terms',
    domain: 'example.com',
    document_type: 'tos',
    title: 'Terms of Service',
    content: 'This is a sample terms of service document. It contains various clauses about user obligations, company rights, data usage, and legal responsibilities. The document outlines the terms under which users may access and use the service, including restrictions, limitations, and user responsibilities.',
    word_count: 2500,
    sentence_count: 150,
    created_at: '2024-01-01T00:00:00Z',
    updated_at: '2024-01-01T00:00:00Z'
  },
  {
    id: 'doc-2',
    url: 'https://example.com/privacy',
    domain: 'example.com',
    document_type: 'privacy',
    title: 'Privacy Policy',
    content: 'This privacy policy describes how we collect, use, and protect your personal information. We are committed to protecting your privacy and ensuring the security of your data. This policy explains what information we collect, how we use it, and your rights regarding your personal data.',
    word_count: 1800,
    sentence_count: 120,
    created_at: '2024-01-01T00:00:00Z',
    updated_at: '2024-01-01T00:00:00Z'
  },
  {
    id: 'doc-3',
    url: 'https://techcorp.com/terms',
    domain: 'techcorp.com',
    document_type: 'tos',
    title: 'Terms and Conditions',
    content: 'Welcome to TechCorp. These terms and conditions govern your use of our technology services. By accessing our platform, you agree to be bound by these terms. We reserve the right to modify these terms at any time, and your continued use constitutes acceptance of any changes.',
    word_count: 3200,
    sentence_count: 200,
    created_at: '2024-01-02T00:00:00Z',
    updated_at: '2024-01-02T00:00:00Z'
  }
]

// Mock text mining measurements
export const mockMeasurements: TextMiningMeasurements = {
  word_count: 2500,
  sentence_count: 150,
  avg_words_per_sentence: 16.7,
  flesch_reading_ease: 45.2,
  flesch_kincaid_grade: 12.5,
  automated_readability_index: 13.8,
  sentiment_score: -0.2,
  keyword_density: 0.15,
  legal_clause_count: 25,
  risk_indicator_score: 7.5
}

// Mock analysis results
export const mockAnalysisResults: AnalysisResult[] = [
  {
    document_id: 'doc-1',
    summary_100: 'This terms of service document establishes user obligations and company rights regarding data usage and service access. It outlines restrictions, limitations, and user responsibilities while defining the legal framework for service usage.',
    summary_sentence: 'The terms of service establish user obligations and company rights with moderate risk indicators for data usage and legal responsibilities.',
    word_frequency: {
      'user': 45,
      'service': 38,
      'company': 32,
      'data': 28,
      'privacy': 25,
      'rights': 22,
      'obligations': 20,
      'responsibilities': 18,
      'restrictions': 15,
      'limitations': 12
    },
    measurements: mockMeasurements,
    created_at: '2024-01-01T00:00:00Z'
  },
  {
    document_id: 'doc-2',
    summary_100: 'This privacy policy describes data collection, usage, and protection practices. It explains what information is collected, how it is used, and user rights regarding personal data. The policy emphasizes commitment to privacy protection and data security.',
    summary_sentence: 'The privacy policy outlines data collection practices with high emphasis on user rights and data protection.',
    word_frequency: {
      'data': 52,
      'privacy': 48,
      'information': 35,
      'personal': 30,
      'collection': 28,
      'rights': 25,
      'protection': 22,
      'security': 20,
      'usage': 18,
      'policy': 15
    },
    measurements: {
      ...mockMeasurements,
      word_count: 1800,
      sentence_count: 120,
      avg_words_per_sentence: 15.0,
      sentiment_score: 0.1,
      risk_indicator_score: 4.2
    },
    created_at: '2024-01-01T00:00:00Z'
  }
]

// Mock crawl sessions
export const mockCrawlSessions: CrawlSession[] = [
  {
    id: 'session-1',
    user_id: 'user-123',
    url: 'https://example.com',
    document_types: ['tos', 'privacy'],
    status: 'completed',
    progress: 100,
    documents_found: 2,
    documents_analyzed: 2,
    created_at: '2024-01-01T00:00:00Z',
    completed_at: '2024-01-01T00:01:00Z',
    documents: [
      {
        id: 'doc-1',
        type: 'tos',
        title: 'Terms of Service',
        url: 'https://example.com/terms',
        status: 'analyzed'
      },
      {
        id: 'doc-2',
        type: 'privacy',
        title: 'Privacy Policy',
        url: 'https://example.com/privacy',
        status: 'analyzed'
      }
    ]
  },
  {
    id: 'session-2',
    user_id: 'user-123',
    url: 'https://techcorp.com',
    document_types: ['tos'],
    status: 'processing',
    progress: 75,
    documents_found: 1,
    documents_analyzed: 0,
    created_at: '2024-01-02T00:00:00Z',
    documents: [
      {
        id: 'doc-3',
        type: 'tos',
        title: 'Terms and Conditions',
        url: 'https://techcorp.com/terms',
        status: 'pending'
      }
    ]
  },
  {
    id: 'session-3',
    user_id: 'user-123',
    url: 'https://invalid-site.com',
    document_types: ['tos', 'privacy'],
    status: 'failed',
    progress: 0,
    documents_found: 0,
    documents_analyzed: 0,
    error_message: 'Unable to access website. Please check the URL and try again.',
    created_at: '2024-01-03T00:00:00Z'
  }
]

// Mock word frequency data for charts
export const mockWordFrequencyData = [
  { word: 'user', count: 45, percentage: 1.8 },
  { word: 'service', count: 38, percentage: 1.5 },
  { word: 'company', count: 32, percentage: 1.3 },
  { word: 'data', count: 28, percentage: 1.1 },
  { word: 'privacy', count: 25, percentage: 1.0 },
  { word: 'rights', count: 22, percentage: 0.9 },
  { word: 'obligations', count: 20, percentage: 0.8 },
  { word: 'responsibilities', count: 18, percentage: 0.7 },
  { word: 'restrictions', count: 15, percentage: 0.6 },
  { word: 'limitations', count: 12, percentage: 0.5 }
]

// Mock risk indicators
export const mockRiskIndicators = [
  {
    level: 'high' as const,
    score: 8.5,
    description: 'High risk data collection practices',
    factors: ['Extensive data collection', 'Third-party sharing', 'Limited user control']
  },
  {
    level: 'medium' as const,
    score: 6.2,
    description: 'Moderate legal complexity',
    factors: ['Complex terms', 'Limited liability', 'Arbitration clauses']
  },
  {
    level: 'low' as const,
    score: 3.1,
    description: 'Standard user obligations',
    factors: ['Basic user responsibilities', 'Clear usage guidelines']
  }
]

// Mock legal clauses
export const mockLegalClauses = [
  {
    type: 'Data Collection',
    content: 'We collect personal information including name, email, and usage data...',
    risk_level: 'high' as const,
    start_position: 150,
    end_position: 300
  },
  {
    type: 'Liability Limitation',
    content: 'Our liability is limited to the maximum extent permitted by law...',
    risk_level: 'medium' as const,
    start_position: 500,
    end_position: 650
  },
  {
    type: 'User Obligations',
    content: 'Users must comply with all applicable laws and regulations...',
    risk_level: 'low' as const,
    start_position: 800,
    end_position: 950
  }
]

// Mock sentiment analysis
export const mockSentimentAnalysis = {
  score: -0.2,
  magnitude: 0.8,
  label: 'negative' as const,
  confidence: 0.75
}

// Mock readability scores
export const mockReadabilityScores = {
  flesch_reading_ease: 45.2,
  flesch_kincaid_grade: 12.5,
  automated_readability_index: 13.8,
  coleman_liau_index: 14.2,
  smog_index: 15.1,
  average_grade_level: 13.9
}

// Mock user favorites
export const mockUserFavorites = [
  {
    id: 'fav-1',
    user_id: 'user-123',
    document_id: 'doc-1',
    created_at: '2024-01-01T00:00:00Z'
  },
  {
    id: 'fav-2',
    user_id: 'user-123',
    document_id: 'doc-2',
    created_at: '2024-01-01T00:00:00Z'
  }
]

// Mock search results
export const mockSearchResults = {
  documents: mockDocuments,
  pagination: {
    page: 1,
    limit: 20,
    total: 3,
    pages: 1
  }
}

// Mock API responses
export const mockApiResponses = {
  success: {
    success: true,
    message: 'Operation completed successfully',
    timestamp: new Date().toISOString()
  },
  error: {
    success: false,
    error: {
      code: 'VALIDATION_ERROR',
      message: 'Invalid input data',
      details: {}
    },
    timestamp: new Date().toISOString()
  }
}