// Common types used across multiple modules

export interface Pagination {
  page: number
  limit: number
  total: number
  pages: number
}

export interface ApiResponse<T = any> {
  success: boolean
  data?: T
  message?: string
  error?: {
    code: string
    message: string
    details?: Record<string, any>
  }
  timestamp: string
}

export interface ApiError {
  code: string
  message: string
  details?: Record<string, any>
}

export interface PaginatedResponse<T> {
  items: T[]
  pagination: Pagination
}

export interface ApiConfig {
  baseURL: string
  timeout: number
  headers: Record<string, string>
}

export interface RequestConfig {
  method: 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH'
  url: string
  data?: any
  params?: Record<string, any>
  headers?: Record<string, string>
}

export interface RateLimitInfo {
  limit: number
  remaining: number
  reset: number
}

export interface ApiClient {
  get<T>(url: string, config?: Partial<RequestConfig>): Promise<ApiResponse<T>>
  post<T>(url: string, data?: any, config?: Partial<RequestConfig>): Promise<ApiResponse<T>>
  put<T>(url: string, data?: any, config?: Partial<RequestConfig>): Promise<ApiResponse<T>>
  delete<T>(url: string, config?: Partial<RequestConfig>): Promise<ApiResponse<T>>
  patch<T>(url: string, data?: any, config?: Partial<RequestConfig>): Promise<ApiResponse<T>>
}