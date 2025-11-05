// Service selector - easily switch between mock and real API
import { apiService } from './api'
import { mockApiService } from './mockApi'

// Use mock service in development, real service in production
const isDevelopment = process.env.NODE_ENV === 'development'
const useMockData = process.env.NEXT_PUBLIC_USE_MOCK_DATA === 'true'

// Export the appropriate service
export const api = isDevelopment && useMockData ? mockApiService : apiService

// Export individual services for specific use cases
export { apiService as realApiService }
export { mockApiService }

// Export types
export * from './api'
export * from './mockApi'