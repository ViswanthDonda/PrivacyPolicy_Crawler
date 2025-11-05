import { create } from 'zustand'
import { persist } from 'zustand/middleware'

interface UIState {
  theme: 'light' | 'dark' | 'system'
  sidebarOpen: boolean
  mobileMenuOpen: boolean
  notifications: Notification[]
  modals: {
    login: boolean
    register: boolean
    report: boolean
    settings: boolean
  }
  toasts: Toast[]
}

interface Notification {
  id: string
  type: 'info' | 'success' | 'warning' | 'error'
  title: string
  message: string
  timestamp: Date
  read: boolean
}

interface Toast {
  id: string
  type: 'info' | 'success' | 'warning' | 'error'
  title: string
  message: string
  duration?: number
}

interface UIActions {
  setTheme: (theme: 'light' | 'dark' | 'system') => void
  toggleSidebar: () => void
  setSidebarOpen: (open: boolean) => void
  toggleMobileMenu: () => void
  setMobileMenuOpen: (open: boolean) => void
  addNotification: (notification: Omit<Notification, 'id' | 'timestamp' | 'read'>) => void
  markNotificationAsRead: (id: string) => void
  removeNotification: (id: string) => void
  clearNotifications: () => void
  openModal: (modal: keyof UIState['modals']) => void
  closeModal: (modal: keyof UIState['modals']) => void
  closeAllModals: () => void
  addToast: (toast: Omit<Toast, 'id'>) => void
  removeToast: (id: string) => void
  clearToasts: () => void
}

type UIStore = UIState & UIActions

export const useUIStore = create<UIStore>()(
  persist(
    (set, get) => ({
      // Initial state
      theme: 'system',
      sidebarOpen: true,
      mobileMenuOpen: false,
      notifications: [],
      modals: {
        login: false,
        register: false,
        report: false,
        settings: false
      },
      toasts: [],

      // Actions
      setTheme: (theme) => {
        set({ theme })
        
        // Apply theme to document
        if (typeof window !== 'undefined') {
          const root = document.documentElement
          if (theme === 'dark') {
            root.classList.add('dark')
          } else if (theme === 'light') {
            root.classList.remove('dark')
          } else {
            // System theme
            const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches
            if (prefersDark) {
              root.classList.add('dark')
            } else {
              root.classList.remove('dark')
            }
          }
        }
      },

      toggleSidebar: () => {
        set((state) => ({ sidebarOpen: !state.sidebarOpen }))
      },

      setSidebarOpen: (open) => {
        set({ sidebarOpen: open })
      },

      toggleMobileMenu: () => {
        set((state) => ({ mobileMenuOpen: !state.mobileMenuOpen }))
      },

      setMobileMenuOpen: (open) => {
        set({ mobileMenuOpen: open })
      },

      addNotification: (notification) => {
        const newNotification: Notification = {
          ...notification,
          id: `notification-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
          timestamp: new Date(),
          read: false
        }
        
        set((state) => ({
          notifications: [newNotification, ...state.notifications]
        }))
      },

      markNotificationAsRead: (id) => {
        set((state) => ({
          notifications: state.notifications.map(notification =>
            notification.id === id ? { ...notification, read: true } : notification
          )
        }))
      },

      removeNotification: (id) => {
        set((state) => ({
          notifications: state.notifications.filter(notification => notification.id !== id)
        }))
      },

      clearNotifications: () => {
        set({ notifications: [] })
      },

      openModal: (modal) => {
        set((state) => ({
          modals: { ...state.modals, [modal]: true }
        }))
      },

      closeModal: (modal) => {
        set((state) => ({
          modals: { ...state.modals, [modal]: false }
        }))
      },

      closeAllModals: () => {
        set({
          modals: {
            login: false,
            register: false,
            report: false,
            settings: false
          }
        })
      },

      addToast: (toast) => {
        const newToast: Toast = {
          ...toast,
          id: `toast-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`
        }
        
        set((state) => ({
          toasts: [...state.toasts, newToast]
        }))
        
        // Auto-remove toast after duration
        const duration = toast.duration || 5000
        setTimeout(() => {
          get().removeToast(newToast.id)
        }, duration)
      },

      removeToast: (id) => {
        set((state) => ({
          toasts: state.toasts.filter(toast => toast.id !== id)
        }))
      },

      clearToasts: () => {
        set({ toasts: [] })
      }
    }),
    {
      name: 'ui-storage',
      partialize: (state) => ({
        theme: state.theme,
        sidebarOpen: state.sidebarOpen
      })
    }
  )
)

// Selectors for easier access
export const useTheme = () => useUIStore((state) => ({
  theme: state.theme,
  setTheme: state.setTheme
}))

export const useSidebar = () => useUIStore((state) => ({
  sidebarOpen: state.sidebarOpen,
  toggleSidebar: state.toggleSidebar,
  setSidebarOpen: state.setSidebarOpen
}))

export const useMobileMenu = () => useUIStore((state) => ({
  mobileMenuOpen: state.mobileMenuOpen,
  toggleMobileMenu: state.toggleMobileMenu,
  setMobileMenuOpen: state.setMobileMenuOpen
}))

export const useNotifications = () => useUIStore((state) => ({
  notifications: state.notifications,
  addNotification: state.addNotification,
  markNotificationAsRead: state.markNotificationAsRead,
  removeNotification: state.removeNotification,
  clearNotifications: state.clearNotifications
}))

export const useModals = () => useUIStore((state) => ({
  modals: state.modals,
  openModal: state.openModal,
  closeModal: state.closeModal,
  closeAllModals: state.closeAllModals
}))

export const useToasts = () => useUIStore((state) => ({
  toasts: state.toasts,
  addToast: state.addToast,
  removeToast: state.removeToast,
  clearToasts: state.clearToasts
}))

// Helper functions
export const showToast = (toast: Omit<Toast, 'id'>) => {
  useUIStore.getState().addToast(toast)
}

export const showNotification = (notification: Omit<Notification, 'id' | 'timestamp' | 'read'>) => {
  useUIStore.getState().addNotification(notification)
}