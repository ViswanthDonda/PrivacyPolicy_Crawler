import { create } from 'zustand'
import { Document, AnalysisResult, DocumentSearchParams } from '@/types'
import { api } from '@/services'

interface DocumentState {
  documents: Document[]
  currentDocument: Document | null
  currentAnalysis: AnalysisResult | null
  favorites: Document[]
  searchResults: Document[]
  isLoading: boolean
  error: string | null
  searchParams: DocumentSearchParams
  pagination: {
    page: number
    limit: number
    total: number
    pages: number
  }
}

interface DocumentActions {
  getDocument: (documentId: string) => Promise<void>
  getDocumentAnalysis: (documentId: string) => Promise<void>
  searchDocuments: (params: DocumentSearchParams) => Promise<void>
  getFavorites: (params?: {
    page?: number
    limit?: number
    document_type?: string
  }) => Promise<void>
  addToFavorites: (documentId: string) => Promise<boolean>
  removeFromFavorites: (documentId: string) => Promise<boolean>
  clearCurrentDocument: () => void
  clearSearchResults: () => void
  setLoading: (loading: boolean) => void
  setError: (error: string | null) => void
  setSearchParams: (params: DocumentSearchParams) => void
}

type DocumentStore = DocumentState & DocumentActions

export const useDocumentStore = create<DocumentStore>((set, get) => ({
  // Initial state
  documents: [],
  currentDocument: null,
  currentAnalysis: null,
  favorites: [],
  searchResults: [],
  isLoading: false,
  error: null,
  searchParams: {},
  pagination: {
    page: 1,
    limit: 20,
    total: 0,
    pages: 0
  },

  // Actions
  getDocument: async (documentId: string) => {
    set({ isLoading: true, error: null })
    
    try {
      const response = await api.getDocument(documentId)
      
      if (response.success && response.data) {
        set({
          currentDocument: response.data.data,
          isLoading: false,
          error: null
        })
      } else {
        set({
          isLoading: false,
          error: response.error?.message || 'Failed to get document'
        })
      }
    } catch (error) {
      set({
        isLoading: false,
        error: 'An unexpected error occurred'
      })
    }
  },

  getDocumentAnalysis: async (documentId: string) => {
    set({ isLoading: true, error: null })
    
    try {
      const response = await api.getDocumentAnalysis(documentId)
      
      if (response.success && response.data) {
        set({
          currentAnalysis: response.data.data,
          isLoading: false,
          error: null
        })
      } else {
        set({
          isLoading: false,
          error: response.error?.message || 'Failed to get document analysis'
        })
      }
    } catch (error) {
      set({
        isLoading: false,
        error: 'An unexpected error occurred'
      })
    }
  },

  searchDocuments: async (params: DocumentSearchParams) => {
    set({ isLoading: true, error: null, searchParams: params })
    
    try {
      const response = await api.searchDocuments(params)
      
      if (response.success && response.data) {
        set({
          searchResults: response.data.data.documents,
          pagination: response.data.data.pagination,
          isLoading: false,
          error: null
        })
      } else {
        set({
          isLoading: false,
          error: response.error?.message || 'Failed to search documents'
        })
      }
    } catch (error) {
      set({
        isLoading: false,
        error: 'An unexpected error occurred'
      })
    }
  },

  getFavorites: async (params = {}) => {
    set({ isLoading: true, error: null })
    
    try {
      const response = await api.getFavorites(params)
      
      if (response.success && response.data) {
        set({
          favorites: response.data.data.documents,
          pagination: response.data.data.pagination,
          isLoading: false,
          error: null
        })
      } else {
        set({
          isLoading: false,
          error: response.error?.message || 'Failed to get favorites'
        })
      }
    } catch (error) {
      set({
        isLoading: false,
        error: 'An unexpected error occurred'
      })
    }
  },

  addToFavorites: async (documentId: string) => {
    set({ isLoading: true, error: null })
    
    try {
      const response = await api.addToFavorites(documentId)
      
      if (response.success) {
        // Refresh favorites list
        get().getFavorites()
        
        set({
          isLoading: false,
          error: null
        })
        return true
      } else {
        set({
          isLoading: false,
          error: response.message || 'Failed to add to favorites'
        })
        return false
      }
    } catch (error) {
      set({
        isLoading: false,
        error: 'An unexpected error occurred'
      })
      return false
    }
  },

  removeFromFavorites: async (documentId: string) => {
    set({ isLoading: true, error: null })
    
    try {
      const response = await api.removeFromFavorites(documentId)
      
      if (response.success) {
        // Refresh favorites list
        get().getFavorites()
        
        set({
          isLoading: false,
          error: null
        })
        return true
      } else {
        set({
          isLoading: false,
          error: response.message || 'Failed to remove from favorites'
        })
        return false
      }
    } catch (error) {
      set({
        isLoading: false,
        error: 'An unexpected error occurred'
      })
      return false
    }
  },

  clearCurrentDocument: () => {
    set({ currentDocument: null, currentAnalysis: null })
  },

  clearSearchResults: () => {
    set({ searchResults: [], searchParams: {} })
  },

  setLoading: (loading: boolean) => {
    set({ isLoading: loading })
  },

  setError: (error: string | null) => {
    set({ error })
  },

  setSearchParams: (params: DocumentSearchParams) => {
    set({ searchParams: params })
  }
}))

// Selectors for easier access
export const useDocuments = () => useDocumentStore((state) => ({
  documents: state.documents,
  currentDocument: state.currentDocument,
  currentAnalysis: state.currentAnalysis,
  favorites: state.favorites,
  searchResults: state.searchResults,
  isLoading: state.isLoading,
  error: state.error,
  pagination: state.pagination
}))

export const useDocumentActions = () => useDocumentStore((state) => ({
  getDocument: state.getDocument,
  getDocumentAnalysis: state.getDocumentAnalysis,
  searchDocuments: state.searchDocuments,
  getFavorites: state.getFavorites,
  addToFavorites: state.addToFavorites,
  removeFromFavorites: state.removeFromFavorites,
  clearCurrentDocument: state.clearCurrentDocument,
  clearSearchResults: state.clearSearchResults,
  setLoading: state.setLoading,
  setError: state.setError
}))

// Helper hook for document type filtering
export const useDocumentTypes = () => {
  const documentTypes = [
    { value: 'tos', label: 'Terms of Service' },
    { value: 'privacy', label: 'Privacy Policy' }
  ]

  return documentTypes
}

// Helper hook for domain filtering
export const useDomains = () => {
  const domains = [
    { value: '.com', label: '.com domains' },
    { value: '.edu', label: '.edu domains' },
    { value: '.org', label: '.org domains' },
    { value: '.gov', label: '.gov domains' }
  ]

  return domains
}