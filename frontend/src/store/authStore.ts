import { create } from 'zustand'
import { persist } from 'zustand/middleware'
import { User, LoginCredentials, RegisterCredentials, GoogleAuthRequest } from '@/types'
import { api } from '@/services'

interface AuthState {
  user: User | null
  token: string | null
  isAuthenticated: boolean
  isLoading: boolean
  error: string | null
}

interface AuthActions {
  login: (credentials: LoginCredentials) => Promise<boolean>
  register: (credentials: RegisterCredentials) => Promise<boolean>
  googleAuth: (request: GoogleAuthRequest) => Promise<boolean>
  logout: () => Promise<void>
  clearError: () => void
  setLoading: (loading: boolean) => void
  setUser: (user: User | null) => void
  setToken: (token: string | null) => void
}

type AuthStore = AuthState & AuthActions

export const useAuthStore = create<AuthStore>()(
  persist(
    (set, get) => ({
      // Initial state
      user: null,
      token: null,
      isAuthenticated: false,
      isLoading: false,
      error: null,

      // Actions
      login: async (credentials: LoginCredentials) => {
        set({ isLoading: true, error: null })
        
        try {
          const response = await api.login(credentials)
          
          if (response.success && response.data) {
            set({
              user: response.data.user,
              token: response.data.access_token,
              isAuthenticated: true,
              isLoading: false,
              error: null
            })
            return true
          } else {
            set({
              isLoading: false,
              error: response.message || 'Login failed'
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

      register: async (credentials: RegisterCredentials) => {
        set({ isLoading: true, error: null })
        
        try {
          const response = await api.register(credentials)
          
          if (response.success && response.data) {
            set({
              user: response.data.user,
              token: response.data.access_token,
              isAuthenticated: true,
              isLoading: false,
              error: null
            })
            return true
          } else {
            set({
              isLoading: false,
              error: response.message || 'Registration failed'
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

      googleAuth: async (request: GoogleAuthRequest) => {
        set({ isLoading: true, error: null })
        
        try {
          const response = await api.googleAuth(request)
          
          if (response.success && response.data) {
            set({
              user: response.data.user,
              token: response.data.access_token,
              isAuthenticated: true,
              isLoading: false,
              error: null
            })
            return true
          } else {
            set({
              isLoading: false,
              error: response.message || 'Google authentication failed'
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

      logout: async () => {
        set({ isLoading: true })
        
        try {
          await api.logout()
        } catch (error) {
          // Ignore logout errors
        } finally {
          set({
            user: null,
            token: null,
            isAuthenticated: false,
            isLoading: false,
            error: null
          })
        }
      },

      clearError: () => {
        set({ error: null })
      },

      setLoading: (loading: boolean) => {
        set({ isLoading: loading })
      },

      setUser: (user: User | null) => {
        set({ user, isAuthenticated: !!user })
      },

      setToken: (token: string | null) => {
        set({ token, isAuthenticated: !!token })
      }
    }),
    {
      name: 'auth-storage',
      partialize: (state) => ({
        user: state.user,
        token: state.token,
        isAuthenticated: state.isAuthenticated
      })
    }
  )
)

// Selectors for easier access
export const useAuth = () => useAuthStore((state) => ({
  user: state.user,
  isAuthenticated: state.isAuthenticated,
  isLoading: state.isLoading,
  error: state.error
}))

export const useAuthActions = () => useAuthStore((state) => ({
  login: state.login,
  register: state.register,
  googleAuth: state.googleAuth,
  logout: state.logout,
  clearError: state.clearError,
  setLoading: state.setLoading
}))