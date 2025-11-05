import { create } from 'zustand'
import { persist } from 'zustand/middleware'
import { CrawlSession, CrawlRequest, Document } from '@/types'
import { apiService as api } from '@/services' // Use real API service, not mock

interface CrawlerState {
  sessions: CrawlSession[]
  currentSession: CrawlSession | null
  isLoading: boolean
  error: string | null
  documentTypes: Array<{
    value: string
    label: string
    description: string
  }>
}

interface CrawlerActions {
  startCrawl: (request: CrawlRequest) => Promise<boolean>
  getCrawlStatus: (sessionId: string) => Promise<void>
  getCrawlHistory: (params?: {
    page?: number
    limit?: number
    status?: string
    document_type?: string
  }) => Promise<void>
  clearSessions: () => void
  setCurrentSession: (session: CrawlSession | null) => void
  clearError: () => void
  setLoading: (loading: boolean) => void
  addSession: (session: CrawlSession) => void
  updateSession: (sessionId: string, updates: Partial<CrawlSession>) => void
}

type CrawlerStore = CrawlerState & CrawlerActions

export const useCrawlerStore = create<CrawlerStore>()(
  persist(
    (set, get) => ({
      // Initial state
      sessions: [],
      currentSession: null,
      isLoading: false,
      error: null,
      documentTypes: [
        {
          value: 'tos',
          label: 'Terms of Service',
          description: 'Terms and conditions for using the service'
        },
        {
          value: 'privacy',
          label: 'Privacy Policy',
          description: 'How personal data is collected and used'
        }
      ],

      // Actions
      startCrawl: async (request: CrawlRequest) => {
        set({ isLoading: true, error: null })
        
        try {
          const response = await api.startCrawl(request)
          
          console.log('Crawl response:', response) // Debug log
          
          // API service wraps response in { success: true, data: ... }
          // Extract the actual crawl data
          const crawlData = response.success && response.data 
            ? response.data 
            : (response.data || response)
          
          console.log('Crawl data:', crawlData) // Debug log
          
          // Extract session_id - it might be nested or a UUID object
          let sessionId: string | null = null
          
          if (crawlData) {
            if (crawlData.session_id) {
              // Handle UUID object or string
              sessionId = typeof crawlData.session_id === 'string' 
                ? crawlData.session_id 
                : String(crawlData.session_id)
            } else if (crawlData.id) {
              // Alternative field name
              sessionId = typeof crawlData.id === 'string'
                ? crawlData.id
                : String(crawlData.id)
            }
          }
          
          console.log('Extracted session ID:', sessionId) // Debug log
          
          if (sessionId) {
            const newSession: CrawlSession = {
              id: sessionId,
              user_id: 'current-user', // This should come from auth context
              url: crawlData.url || request.url,
              document_types: ['tos', 'privacy'], // Map back from backend enum
              status: crawlData.status || 'pending',
              progress: 0,
              documents_found: 0,
              documents_analyzed: 0,
              created_at: crawlData.created_at || new Date().toISOString(),
              documents: []
            }
            
            console.log('Created session:', newSession) // Debug log
            
            set((state) => ({
              sessions: [newSession, ...state.sessions],
              currentSession: newSession,
              isLoading: false,
              error: null
            }))
            
            // Start polling for status updates
            get().getCrawlStatus(sessionId)
            
            return true
          } else {
            console.error('No session_id found in response:', response)
            const errorMsg = typeof response.error?.message === 'string' 
              ? response.error.message 
              : (typeof response.error === 'string' ? response.error : 'Failed to start crawl')
            set({
              isLoading: false,
              error: errorMsg
            })
            return false
          }
        } catch (error: any) {
          console.error('Error starting crawl:', error)
          const errorMsg = typeof error?.message === 'string' 
            ? error.message 
            : (typeof error === 'string' ? error : 'An unexpected error occurred')
          set({
            isLoading: false,
            error: errorMsg
          })
          return false
        }
      },

      getCrawlStatus: async (sessionId: string) => {
        try {
          const response = await api.getCrawlStatus(sessionId)
          
          if (response.success && response.data) {
            // Backend returns CrawlStatusResponse with 'id' field (not 'session_id')
            const sessionData = response.data
            
            if (sessionData) {
              // Map backend CrawlStatusResponse format to frontend CrawlSession format
              const updatedSession: CrawlSession = {
                id: String(sessionData.id || sessionId),
                user_id: 'current-user', // Backend doesn't return user_id in status response
                url: sessionData.url || '',
                document_types: ['tos', 'privacy'], // Not in status response
                status: sessionData.status || 'pending',
                progress: sessionData.analyzed_count && sessionData.document_count
                  ? sessionData.analyzed_count / sessionData.document_count
                  : 0,
                documents_found: sessionData.document_count || 0,
                documents_analyzed: sessionData.analyzed_count || 0,
                created_at: sessionData.created_at || new Date().toISOString(),
                documents: [] // Not in status response
              }
              
              set((state) => ({
                sessions: state.sessions
                  .map(session => 
                    session && session.id === sessionId ? updatedSession : session
                  )
                  .filter(Boolean) as CrawlSession[], // Remove any undefined entries
                currentSession: state.currentSession?.id === sessionId 
                  ? updatedSession 
                  : state.currentSession
              }))
            }
          }
        } catch (error) {
          console.error('Failed to get crawl status:', error)
        }
      },

      getCrawlHistory: async (params = {}) => {
        set({ isLoading: true, error: null })
        
        try {
          const response = await api.getCrawlHistory(params)
          
          if (response.success && response.data) {
            // Backend returns List[CrawlStatusResponse] directly
            // ApiService wraps it as { success: true, data: [sessions...] }
            // Handle both response formats for compatibility
            let sessionsList: CrawlSession[] = []
            
            if (Array.isArray(response.data)) {
              // Backend returns array directly
              sessionsList = response.data.map((session: any) => ({
                id: String(session.id || session.session_id),
                user_id: 'current-user', // User is authenticated, sessions are filtered by backend
                url: session.url || '',
                document_types: ['tos', 'privacy'], // Default, backend doesn't return this
                status: session.status || 'pending',
                progress: session.analyzed_count && session.document_count
                  ? session.analyzed_count / session.document_count
                  : 0,
                documents_found: session.document_count || 0,
                documents_analyzed: session.analyzed_count || 0,
                created_at: session.created_at || new Date().toISOString(),
                error_message: session.error_message,
                documents: []
              }))
            } else if (response.data.data?.sessions) {
              // Wrapped format (if pagination wrapper exists)
              sessionsList = response.data.data.sessions
            } else if (response.data.sessions) {
              // Alternative wrapped format
              sessionsList = response.data.sessions
            }
            
            set({
              sessions: sessionsList,
              isLoading: false,
              error: null
            })
          } else {
            set({
              isLoading: false,
              error: response.error?.message || 'Failed to load crawl history'
            })
          }
        } catch (error: any) {
          console.error('Error fetching crawl history:', error)
          // Don't show error for 401 - token might need refresh
          // The API interceptor will handle auth errors, component will retry
          const isAuthError = error?.response?.status === 401 || error?.message?.includes('401')
          
          set({
            isLoading: false,
            error: isAuthError ? null : 'An unexpected error occurred'
          })
        }
      },
      
      clearSessions: () => {
        set({ sessions: [], currentSession: null })
      },

      setCurrentSession: (session: CrawlSession | null) => {
        set({ currentSession: session })
      },

      clearError: () => {
        set({ error: null })
      },

      setLoading: (loading: boolean) => {
        set({ isLoading: loading })
      },

      addSession: (session: CrawlSession) => {
        set((state) => ({
          sessions: [session, ...state.sessions]
        }))
      },

      updateSession: (sessionId: string, updates: Partial<CrawlSession>) => {
        set((state) => ({
          sessions: state.sessions.map(session => 
            session.id === sessionId ? { ...session, ...updates } : session
          ),
          currentSession: state.currentSession?.id === sessionId 
            ? { ...state.currentSession, ...updates }
            : state.currentSession
        }))
      },

      // Helper method for polling (not exposed in interface)
      pollCrawlStatus: async (sessionId: string) => {
        const pollInterval = setInterval(async () => {
          const { currentSession } = get()
          
          if (!currentSession || currentSession.id !== sessionId) {
            clearInterval(pollInterval)
            return
          }
          
          if (currentSession.status === 'completed' || currentSession.status === 'failed') {
            clearInterval(pollInterval)
            return
          }
          
          await get().getCrawlStatus(sessionId)
        }, 2000) // Poll every 2 seconds
      }
    }),
    {
      name: 'crawler-storage',
      partialize: (state) => ({
        // Only persist valid sessions
        sessions: state.sessions.filter(s => s && s.id && s.url),
        documentTypes: state.documentTypes
      }),
      // Migration to clean up corrupted data
      migrate: (persistedState: any, version: number) => {
        if (persistedState?.sessions) {
          // Filter out any null/undefined/invalid sessions
          persistedState.sessions = persistedState.sessions.filter((s: any) => 
            s && s.id && s.url && s.status
          )
        }
        return persistedState
      }
    }
  )
)

// Selectors for easier access
export const useCrawler = () => useCrawlerStore((state) => ({
  // Filter out any invalid sessions
  sessions: (state.sessions || []).filter(s => s && s.id && s.url && s.status),
  currentSession: state.currentSession,
  isLoading: state.isLoading,
  error: state.error
}))

export const useCrawlerActions = () => useCrawlerStore((state) => ({
  startCrawl: state.startCrawl,
  getCrawlStatus: state.getCrawlStatus,
  getCrawlHistory: state.getCrawlHistory,
  clearSessions: state.clearSessions,
  setCurrentSession: state.setCurrentSession,
  clearError: state.clearError,
  setLoading: state.setLoading,
  addSession: state.addSession,
  updateSession: state.updateSession
}))

export const useDocumentTypes = () => useCrawlerStore((state) => state.documentTypes)