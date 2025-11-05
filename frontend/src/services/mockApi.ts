import { 
  AuthResponse, 
  LoginCredentials, 
  RegisterCredentials, 
  GoogleAuthRequest,
  CrawlRequest,
  CrawlResponse,
  CrawlStatusResponse,
  CrawlHistoryResponse,
  DocumentResponse,
  ApiResponse,
  AnalysisResponse,
  FavoritesResponse,
  FavoriteActionResponse,
  DocumentSearchResponse
} from '@/types'
import { 
  mockUser, 
  mockDocuments, 
  mockAnalysisResults, 
  mockCrawlSessions,
  mockUserFavorites,
  mockSearchResults,
  mockApiResponses
} from '@/data/mockData'

// Simulate API delay
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

// Mock API base configuration
const MOCK_API_BASE = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000/api/v1'

class MockApiService {
  private baseURL: string
  private token: string | null = null

  constructor(baseURL: string = MOCK_API_BASE) {
    this.baseURL = baseURL
    // Load authentication token from localStorage if available
    if (typeof window !== 'undefined') {
      this.token = localStorage.getItem('auth_token')
    }
  }

  // Make mock API request with simulated network delay
  private async request<T>(
    endpoint: string, 
    options: RequestInit = {}
  ): Promise<ApiResponse<T>> {
    // Simulate realistic network latency
    await delay(500 + Math.random() * 1000)

    // Randomly simulate network errors (5% chance)
    if (Math.random() < 0.05) {
      return {
        success: false,
        error: {
          code: 'NETWORK_ERROR',
          message: 'Network request failed',
          details: {} as unknown as ApiResponse<any>
        },
        timestamp: new Date().toISOString()
      }
    }

    const url = `${this.baseURL}${endpoint}`
    const config: RequestInit = {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...(this.token && { 'Authorization': `Bearer ${this.token}` }),
        ...options.headers,
      },
    }

    // Return mock successful response
    return {
      success: true,
      data: {} as T,
      message: 'Mock response',
      timestamp: new Date().toISOString()
    }
  }

  // Authentication methods
  async login(credentials: LoginCredentials): Promise<AuthResponse> {
    await delay(1000)
    
    if (credentials.email === 'test@example.com' && credentials.password === 'password') {
      this.token = 'mock-jwt-token'
      if (typeof window !== 'undefined') {
        localStorage.setItem('auth_token', this.token)
      }
      
      return {
        success: true,
        data: {
          user: mockUser,
          access_token: this.token,
          token_type: 'bearer',
          expires_in: 3600
        }
      }
    }
    
    return {
      success: false,
      data: {
        user: {} as any,
        access_token: '',
        token_type: '',
        expires_in: 0
      },
      message: 'Invalid email or password'
    }
  }

  async register(credentials: RegisterCredentials): Promise<AuthResponse> {
    await delay(1200)
    
    if (credentials.password !== credentials.confirm_password) {
      return {
        success: false,
        data: {
          user: {} as any,
          access_token: '',
          token_type: '',
          expires_in: 0
        },
        message: 'Passwords do not match'
      }
    }
    
    this.token = 'mock-jwt-token'
    if (typeof window !== 'undefined') {
      localStorage.setItem('auth_token', this.token)
    }
    
    return {
      success: true,
      data: {
        user: { ...mockUser, email: credentials.email },
        access_token: this.token,
        token_type: 'bearer',
        expires_in: 3600
      }
    }
  }

  async googleAuth(request: GoogleAuthRequest): Promise<AuthResponse> {
    await delay(800)
    
    this.token = 'mock-jwt-token'
    if (typeof window !== 'undefined') {
      localStorage.setItem('auth_token', this.token)
    }
    
    return {
      success: true,
      data: {
        user: { ...mockUser, email: 'user@gmail.com', firebase_uid: 'firebase-uid-123' },
        access_token: this.token,
        token_type: 'bearer',
        expires_in: 3600
      }
    }
  }

  async logout(): Promise<ApiResponse> {
    await delay(500)
    
    this.token = null
    if (typeof window !== 'undefined') {
      localStorage.removeItem('auth_token')
    }
    
    return mockApiResponses.success
  }

  // Crawler methods
  async startCrawl(request: CrawlRequest): Promise<ApiResponse<CrawlResponse>> {
    await delay(1000)
    
    const sessionId = `session-${Date.now()}`
    
    return {
      success: true,
      data: {
        session_id: sessionId,
        status: 'pending',
        url: request.url,
        document_types: request.document_types,
        estimated_time: '30-60 seconds'
      },
      timestamp: new Date().toISOString()
    } as unknown as ApiResponse<CrawlResponse>
  }

  async getCrawlStatus(sessionId: string): Promise<ApiResponse<CrawlStatusResponse>> {
    await delay(500)
    
    const session = mockCrawlSessions.find(s => s.id === sessionId)
    if (!session) {
      return {
        success: false,
        error: {
          code: 'SESSION_NOT_FOUND',
          message: 'Crawl session not found',
          details: {} as unknown as ApiResponse<any>
        },
        timestamp: new Date().toISOString()
      } as unknown as ApiResponse<CrawlStatusResponse>
    }
    
    return {
      success: true,
      data: session,
      timestamp: new Date().toISOString()
    } as unknown as ApiResponse<CrawlStatusResponse>
  }

  async getCrawlHistory(params: {
    page?: number
    limit?: number
    status?: string
    document_type?: string
  } = {}): Promise<ApiResponse<CrawlHistoryResponse>> {
    await delay(800)
    
    const { page = 1, limit = 20 } = params
    const startIndex = (page - 1) * limit
    const endIndex = startIndex + limit
    const sessions = mockCrawlSessions.slice(startIndex, endIndex)
    
    return {
      success: true,
      data: {
        sessions,
        pagination: {
          page,
          limit,
          total: mockCrawlSessions.length,
          pages: Math.ceil(mockCrawlSessions.length / limit)
        }
      },
      timestamp: new Date().toISOString()
    } as unknown as ApiResponse<CrawlHistoryResponse>
  }

  // Document methods
  async getDocument(documentId: string): Promise<ApiResponse<DocumentResponse>> {
    await delay(600)
    
    const document = mockDocuments.find(d => d.id === documentId)
    if (!document) {
      return {
        success: false,
        error: {
          code: 'DOCUMENT_NOT_FOUND',
          message: 'Document not found',
          details: {} as unknown as ApiResponse<any>
        },
        timestamp: new Date().toISOString()
      } as unknown as ApiResponse<DocumentResponse>
    }
    
    return {
      success: true,
      data: document,
      timestamp: new Date().toISOString()
    } as unknown as ApiResponse<DocumentResponse>
  }

  async getDocumentAnalysis(documentId: string): Promise<ApiResponse<AnalysisResponse>> {
    await delay(800)
    
    const analysis = mockAnalysisResults.find(a => a.document_id === documentId)
    if (!analysis) {
      return {
        success: false,
        error: {
          code: 'ANALYSIS_NOT_FOUND',
          message: 'Analysis not found',
          details: {} as unknown as ApiResponse<any>
        },
        timestamp: new Date().toISOString()
      }
    }
    
    return {
      success: true,
      data: analysis
    } as unknown as ApiResponse<any>
  }

  async searchDocuments(params: {
    q?: string
    document_type?: string
    domain?: string
    page?: number
    limit?: number
  } = {}): Promise<ApiResponse<DocumentSearchResponse>> {
    await delay(700)
    
    const { page = 1, limit = 20 } = params
    let filteredDocuments = [...mockDocuments]
    
    if (params.q) {
      filteredDocuments = filteredDocuments.filter(doc => 
        doc.title.toLowerCase().includes(params.q!.toLowerCase()) ||
        doc.content.toLowerCase().includes(params.q!.toLowerCase())
      )
    }
    
    if (params.document_type) {
      filteredDocuments = filteredDocuments.filter(doc => 
        doc.document_type === params.document_type
      )
    }
    
    if (params.domain) {
      filteredDocuments = filteredDocuments.filter(doc => 
        doc.domain.includes(params.domain!)
      )
    }
    
    const startIndex = (page - 1) * limit
    const endIndex = startIndex + limit
    const documents = filteredDocuments.slice(startIndex, endIndex)
    
    return {
      success: true,
      data: {
        documents,
        pagination: {
          page,
          limit,
          total: filteredDocuments.length,
          pages: Math.ceil(filteredDocuments.length / limit)
        }
      },
      timestamp: new Date().toISOString()
    } as unknown as ApiResponse<DocumentSearchResponse>
  }

  // Favorites methods
  async getFavorites(params: {
    page?: number
    limit?: number
    document_type?: string
  } = {}): Promise<ApiResponse<FavoritesResponse>> {
    await delay(600)
    
    const { page = 1, limit = 20 } = params
    const favoriteDocuments = mockUserFavorites.map(fav => {
      const doc = mockDocuments.find(d => d.id === fav.document_id)
      return doc ? { ...doc, favorited_at: fav.created_at } : null
    }).filter(Boolean)
    
    const startIndex = (page - 1) * limit
    const endIndex = startIndex + limit
    const documents = favoriteDocuments.slice(startIndex, endIndex)
    
    return {
      success: true,
      data: {
        documents: favoriteDocuments,
        pagination: {
          page,
          limit,
          total: favoriteDocuments.length,
          pages: Math.ceil(favoriteDocuments.length / limit)
        }
      },
      timestamp: new Date().toISOString()
    } as unknown as ApiResponse<FavoritesResponse>
  }

  async addToFavorites(documentId: string): Promise<ApiResponse<FavoriteActionResponse>> {
    await delay(500)
    
    return {
      success: true,
      message: 'Document added to favorites'
    } as unknown as ApiResponse<any>
  }

  async removeFromFavorites(documentId: string): Promise<ApiResponse<FavoriteActionResponse>> {
    await delay(500)
    
    return {
      success: true,
      message: 'Document removed from favorites'
    } as unknown as ApiResponse<any>
  }

  // Report methods
  async downloadReport(documentId: string, format: 'pdf' | 'json'): Promise<Blob> {
    await delay(1000)
    
    if (format === 'json') {
      const document = mockDocuments.find(d => d.id === documentId)
      const analysis = mockAnalysisResults.find(a => a.document_id === documentId)
      
      const reportData = {
        document,
        analysis,
        generated_at: new Date().toISOString()
      }
      
      return new Blob([JSON.stringify(reportData, null, 2)], { 
        type: 'application/json' 
      })
    }
    
    // Mock PDF blob
    return new Blob(['Mock PDF content'], { type: 'application/pdf' })
  }
}

// Create singleton instance
export const mockApiService = new MockApiService()

// Export for easy replacement with real API
export default mockApiService