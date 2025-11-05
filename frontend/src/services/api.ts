import axios, { AxiosInstance, AxiosRequestConfig, AxiosResponse } from 'axios'
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
  AnalysisResponse,
  FavoritesResponse,
  FavoriteActionResponse,
  DocumentSearchResponse,
  ApiResponse
} from '@/types'

class ApiService {
  private client: AxiosInstance
  private baseURL: string

  constructor() {
    // Set API base URL from environment or use localhost default
    this.baseURL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000/api/v1'
    
    // Create axios instance with base configuration
    this.client = axios.create({
      baseURL: this.baseURL,
      timeout: 30000,
      headers: {
        'Content-Type': 'application/json',
      },
    })

    // Add authentication token to all requests
    this.client.interceptors.request.use(
      (config) => {
        const token = this.getAuthToken()
        if (token) {
          config.headers.Authorization = `Bearer ${token}`
        }
        return config
      },
      (error) => {
        return Promise.reject(error)
      }
    )

    // Response interceptor for error handling
    this.client.interceptors.response.use(
      (response: AxiosResponse) => {
        return response
      },
      async (error) => {
        if (error.response?.status === 401) {
          // Clear invalid token from storage
          this.clearAuthToken()
          
          // Check if user is on protected route before redirecting
          if (typeof window !== 'undefined') {
            const currentPath = window.location.pathname
            const isProtectedRoute = !currentPath.startsWith('/auth/')
            
            // Redirect to login only if on protected route and not already on login page
            // Prevents redirect loops during initial page load
            if (isProtectedRoute && currentPath !== '/auth/login') {
              // Verify Firebase authentication state before redirecting
              try {
                const { auth } = await import('@/lib/firebase')
                // If Firebase user doesn't exist, redirect to login
                if (!auth.currentUser) {
                  window.location.href = '/auth/login'
                }
                // If Firebase user exists, don't redirect - let the component handle token refresh
                // The component will retry with a fresh token
              } catch (importError) {
                // If we can't check Firebase auth, redirect to be safe
                window.location.href = '/auth/login'
              }
            }
          }
        }
        return Promise.reject(error)
      }
    )
  }

  private getAuthToken(): string | null {
    if (typeof window !== 'undefined') {
      return localStorage.getItem('auth_token')
    }
    return null
  }

  private setAuthToken(token: string): void {
    if (typeof window !== 'undefined') {
      localStorage.setItem('auth_token', token)
    }
  }

  private clearAuthToken(): void {
    if (typeof window !== 'undefined') {
      localStorage.removeItem('auth_token')
    }
  }

  private async request<T>(
    config: AxiosRequestConfig
  ): Promise<ApiResponse<T>> {
    try {
      const response = await this.client.request(config)
      
      // FastAPI returns data directly, so response.data contains the actual response
      const data = response.data
      
      // Check if it's already wrapped in ApiResponse format
      if (data && typeof data === 'object' && 'success' in data) {
        return data as ApiResponse<T>
      }
      
      // Otherwise wrap it
      return {
        success: true,
        data: data as T,
        timestamp: new Date().toISOString()
      }
    } catch (error: any) {
      // Handle error responses
      if (error.response?.data) {
        const errorData = error.response.data
        // Check if it's already in ApiResponse format
        if (errorData && typeof errorData === 'object' && 'success' in errorData) {
          return errorData
        }
        
        // Extract error message from FastAPI validation errors
        let errorMessage = 'Request failed'
        if (errorData.detail) {
          // FastAPI validation errors come as array or string
          if (Array.isArray(errorData.detail)) {
            // Extract messages from validation error array
            errorMessage = errorData.detail
              .map((err: any) => err.msg || JSON.stringify(err))
              .join(', ')
          } else if (typeof errorData.detail === 'string') {
            errorMessage = errorData.detail
          } else {
            errorMessage = JSON.stringify(errorData.detail)
          }
        } else if (errorData.message) {
          errorMessage = errorData.message
        } else if (error.message) {
          errorMessage = error.message
        }
        
        // Wrap error detail from FastAPI
        return {
          success: false,
          error: {
            code: error.response.status?.toString() || 'API_ERROR',
            message: errorMessage,
            details: errorData
          },
          timestamp: new Date().toISOString()
        }
      }
      
      return {
        success: false,
        error: {
          code: 'NETWORK_ERROR',
          message: error.message || 'Network request failed',
          details: {}
        },
        timestamp: new Date().toISOString()
      }
    }
  }

  // Authentication methods
  async login(credentials: LoginCredentials): Promise<AuthResponse> {
    const response = await this.request<AuthResponse>({
      method: 'POST',
      url: '/auth/login',
      data: credentials
    })

    if (response.success && response.data && 'access_token' in response.data) {
      this.setAuthToken((response.data as any).access_token)
    }

    return response.data as AuthResponse
  }

  async register(credentials: RegisterCredentials): Promise<AuthResponse> {
    const response = await this.request<AuthResponse>({
      method: 'POST',
      url: '/auth/register',
      data: credentials
    })

    if (response.success && response.data && 'access_token' in response.data) {
      this.setAuthToken((response.data as any).access_token)
    }

    return response.data as AuthResponse
  }

  async googleAuth(request: GoogleAuthRequest): Promise<AuthResponse> {
    const response = await this.request<AuthResponse>({
      method: 'POST',
      url: '/auth/google',
      data: request
    })

    if (response.success && response.data && 'access_token' in response.data) {
      this.setAuthToken((response.data as any).access_token)
    }

    return response.data as AuthResponse
  }

  async logout(): Promise<ApiResponse> {
    const response = await this.request<ApiResponse>({
      method: 'POST',
      url: '/auth/logout'
    })

    if (response.success) {
      this.clearAuthToken()
    }

    return response
  }

  async refreshToken(refreshToken: string): Promise<ApiResponse> {
    const response = await this.request<ApiResponse>({
      method: 'POST',
      url: '/auth/refresh',
      data: { refresh_token: refreshToken }
    })

    if (response.success && response.data && 'access_token' in response.data) {
      this.setAuthToken((response.data as any).access_token)
    }

    return response
  }

  // Crawler methods
  async startCrawl(request: CrawlRequest): Promise<ApiResponse<CrawlResponse>> {
    return this.request<CrawlResponse>({
      method: 'POST',
      url: '/crawler/analyze',
      data: request
    })
  }

  async getCrawlStatus(sessionId: string): Promise<ApiResponse<CrawlStatusResponse>> {
    return this.request<CrawlStatusResponse>({
      method: 'GET',
      url: `/crawler/status/${sessionId}`
    })
  }

  async getCrawlHistory(params: {
    page?: number
    limit?: number
    status?: string
    document_type?: string
  } = {}): Promise<ApiResponse<CrawlHistoryResponse>> {
    return this.request<CrawlHistoryResponse>({
      method: 'GET',
      url: '/crawler/history',
      params
    })
  }

  async deleteCrawlSession(sessionId: string): Promise<ApiResponse<{ success: boolean; message: string }>> {
    // Validate UUID format
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
    if (!uuidRegex.test(sessionId)) {
      console.error('Invalid session ID format:', sessionId)
      return {
        success: false,
        error: {
          code: 'INVALID_UUID',
          message: `Invalid session ID format: ${sessionId}`,
          details: {}
        },
        timestamp: new Date().toISOString()
      }
    }
    
    return this.request<{ success: boolean; message: string }>({
      method: 'DELETE',
      url: `/crawler/session/${sessionId}`
    })
  }

  async getCrawlResults(sessionId: string): Promise<ApiResponse<any>> {
    // Validate UUID format
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
    if (!uuidRegex.test(sessionId)) {
      console.error('Invalid session ID format:', sessionId)
      return {
        success: false,
        error: {
          code: 'INVALID_UUID',
          message: `Invalid session ID format: ${sessionId}`,
          details: {}
        },
        timestamp: new Date().toISOString()
      }
    }
    
    return this.request<any>({
      method: 'GET',
      url: `/crawler/session/${sessionId}/results`
    })
  }

  // Document methods
  async getDocument(documentId: string): Promise<ApiResponse<DocumentResponse>> {
    return this.request<DocumentResponse>({
      method: 'GET',
      url: `/documents/${documentId}`
    })
  }

  async getDocumentAnalysis(documentId: string): Promise<ApiResponse<AnalysisResponse>> {
    return this.request<AnalysisResponse>({
      method: 'GET',
      url: `/documents/${documentId}/analysis`
    })
  }

  async searchDocuments(params: {
    q?: string
    document_type?: string
    domain?: string
    page?: number
    limit?: number
  } = {}): Promise<ApiResponse<DocumentSearchResponse>> {
    return this.request<DocumentSearchResponse>({
      method: 'GET',
      url: '/documents/search',
      params
    })
  }

  // Favorites methods
  async getFavorites(params: {
    page?: number
    limit?: number
    document_type?: string
  } = {}): Promise<ApiResponse<FavoritesResponse>> {
    return this.request<FavoritesResponse>({
      method: 'GET',
      url: '/documents/favorites',
      params
    })
  }

  async addToFavorites(documentId: string): Promise<ApiResponse<FavoriteActionResponse>> {
    return this.request<FavoriteActionResponse>({
      method: 'POST',
      url: `/documents/${documentId}/favorite`
    })
  }

  async removeFromFavorites(documentId: string): Promise<ApiResponse<FavoriteActionResponse>> {
    return this.request<FavoriteActionResponse>({
      method: 'DELETE',
      url: `/documents/${documentId}/favorite`
    })
  }

  // Report methods
  async downloadReport(documentId: string, format: 'pdf' | 'json'): Promise<Blob> {
    const response = await this.client.get(`/reports/${documentId}/${format}`, {
      responseType: 'blob'
    })
    
    return response.data
  }

  // Health check
  async healthCheck(): Promise<ApiResponse> {
    return this.request<ApiResponse>({
      method: 'GET',
      url: '/health'
    })
  }
}

// Create singleton instance for application-wide use
export const apiService = new ApiService()

// Default export for convenience - can be replaced with mock service for testing
export default apiService