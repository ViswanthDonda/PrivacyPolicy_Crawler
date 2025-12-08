import React, { useState, useEffect } from 'react'
import Link from 'next/link'
import { useRouter } from 'next/router'
import { Button } from '@/components/ui'
import { useAuth } from '@/hooks/useAuth'
import { useAuthStore } from '@/store/authStore'
import { apiService } from '@/services'
import {
  Menu,
  X,
  User,
  LogOut,
  Key,
  Bookmark,
  ChevronDown,
  History,
  Shield
} from 'lucide-react'
import toast from 'react-hot-toast'

const Header: React.FC = () => {
  const router = useRouter()
  const { user, userProfile, loading, signOut } = useAuth()
  const { user: storeUser } = useAuthStore()
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false)
  const [userMenuOpen, setUserMenuOpen] = useState(false)
  const [isAdmin, setIsAdmin] = useState(false)

  // Check admin status
  useEffect(() => {
    const checkAdmin = async () => {
      if (storeUser?.is_admin) {
        setIsAdmin(true)
      } else if (user) {
        try {
          const response = await apiService.getCurrentUser()
          if (response.success && response.data) {
            setIsAdmin((response.data as any).is_admin || false)
          }
        } catch (error) {
          // Ignore errors
        }
      }
    }
    checkAdmin()
  }, [user, storeUser])

  // Handle user logout and redirect to home
  const handleLogout = async () => {
    try {
      const result = await signOut()
      if (result.success) {
        toast.success('Signed out successfully')
        router.push('/')
      } else {
        toast.error(result.error || 'Failed to sign out')
      }
    } catch (error) {
      console.error('Logout error:', error)
      toast.error('An unexpected error occurred')
    }
  }

  const navigation: Array<{ name: string; href: string }> = []

  // Check if current route matches given path
  const isActive = (path: string) => {
    return router.pathname === path
  }

  // Get user display name from profile, displayName, or email prefix
  const getDisplayName = () => {
    if (userProfile?.username) return userProfile.username
    if (user?.displayName) return user.displayName
    if (user?.email) return user.email.split('@')[0]
    return 'User'
  }

  const isAuthenticated = !!user

  return (
    <header className="bg-gray-900/80 backdrop-blur-sm shadow-lg border-b border-gray-700 transition-colors duration-300 sticky top-0 z-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center h-16">
          {/* Logo */}
          <div className="flex items-center">
            <Link 
              href={isAuthenticated ? "/crawler" : "/"} 
              className="flex items-center space-x-2 group hover-lift"
            >
              <div className="w-8 h-8 bg-gradient-to-br from-gray-600 to-gray-800 rounded-lg flex items-center justify-center shadow-lg group-hover:shadow-gray-500/50 transition-all duration-300 border border-gray-500">
                <span className="text-white font-bold text-sm">T</span>
              </div>
              <span className="text-2xl font-bold text-white">
                TOS Crawler
              </span>
            </Link>
          </div>

          {/* Desktop Navigation */}
          <nav className="hidden md:flex space-x-8">
            {navigation.map((item) => (
              <Link
                key={item.name}
                href={item.href}
                className={`px-4 py-2 rounded-lg text-sm font-medium transition-all duration-300 ${
                  isActive(item.href)
                    ? 'text-white bg-gray-800/50 border border-gray-600'
                    : 'text-gray-300 hover:text-white hover:bg-gray-800/50'
                }`}
              >
                {item.name}
              </Link>
            ))}
          </nav>

          {/* Right side */}
          <div className="flex items-center space-x-4">
            {/* User Menu */}
            {isAuthenticated ? (
              <div className="flex items-center space-x-4">
                {/* Admin Link */}
                {isAdmin && (
                  <Link
                    href="/admin/global-documents"
                    className={`px-3 py-2 rounded-lg text-sm font-medium transition-all duration-300 ${
                      router.pathname?.startsWith('/admin')
                        ? 'text-white bg-gray-800/50 border border-gray-600'
                        : 'text-gray-300 hover:text-white hover:bg-gray-800/50'
                    }`}
                  >
                    <div className="flex items-center space-x-2">
                      <Shield className="h-4 w-4" />
                      <span>Admin</span>
                    </div>
                  </Link>
                )}
                
                {/* History Link */}
                <Link
                  href="/history"
                  className={`px-3 py-2 rounded-lg text-sm font-medium transition-all duration-300 ${
                    router.pathname === '/history'
                      ? 'text-white bg-gray-800/50 border border-gray-600'
                      : 'text-gray-300 hover:text-white hover:bg-gray-800/50'
                  }`}
                >
                  <div className="flex items-center space-x-2">
                    <History className="h-4 w-4" />
                    <span>History</span>
                  </div>
                </Link>
                
                <div className="hidden sm:block text-sm">
                  <p className="font-medium text-gray-100">{getDisplayName()}</p>
                </div>
                
                <div className="relative">
                  <button 
                    onClick={() => setUserMenuOpen(!userMenuOpen)}
                    className="flex items-center space-x-2 p-2 text-gray-300 hover:text-white transition-colors hover-lift"
                  >
                    <div className="w-8 h-8 bg-gradient-to-br from-gray-600 to-gray-800 rounded-full flex items-center justify-center shadow-md hover:shadow-gray-500/50 transition-all duration-300 border border-gray-500">
                      <User className="h-4 w-4 text-white" />
                    </div>
                    <ChevronDown className={`h-3 w-3 text-gray-400 transition-transform ${userMenuOpen ? 'rotate-180' : ''}`} />
                  </button>
          
                  {/* Dropdown Menu */}
                  {userMenuOpen && (
                    <div className="absolute right-0 mt-2 w-56 bg-gray-800/90 backdrop-blur-sm rounded-lg shadow-xl py-1 z-50 border border-gray-700">
                      <div className="px-4 py-2 border-b border-gray-700">
                        <p className="text-sm font-medium text-white">{getDisplayName()}</p>
                        <p className="text-xs text-gray-400 truncate">{user?.email}</p>
                      </div>
                      
                      <Link
                        href="/settings/change-password"
                        onClick={() => setUserMenuOpen(false)}
                        className="flex items-center px-4 py-2 text-sm text-gray-300 hover:text-white hover:bg-gray-700/50 transition-colors"
                      >
                        <Key className="h-4 w-4 mr-3" />
                        Change Password
                      </Link>
                      
                      <Link
                        href="/bookmarks"
                        onClick={() => setUserMenuOpen(false)}
                        className="flex items-center px-4 py-2 text-sm text-gray-300 hover:text-white hover:bg-gray-700/50 transition-colors"
                      >
                        <Bookmark className="h-4 w-4 mr-3" />
                        Bookmarks
                      </Link>
                      
                      <div className="border-t border-gray-700 my-1"></div>
                      
                      <button
                        onClick={() => {
                          setUserMenuOpen(false)
                          handleLogout()
                        }}
                        className="flex items-center w-full px-4 py-2 text-sm text-red-400 hover:text-red-300 hover:bg-gray-700/50 transition-colors"
                      >
                        <LogOut className="h-4 w-4 mr-3" />
                        Logout
                      </button>
                    </div>
                  )}
                </div>
              </div>
            ) : (
              <div className="flex items-center space-x-3">
                <Link href="/auth/login">
                  <Button variant="outline" size="sm">
                    Sign in
                  </Button>
                </Link>
                <Link href="/auth/register">
                  <Button variant="primary" size="sm">
                    Sign up
                  </Button>
                </Link>
              </div>
            )}

            {/* Mobile menu button */}
            <button
              onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
              className="md:hidden p-2 text-gray-400 hover:text-white transition-colors hover-lift rounded-lg hover:bg-gray-800/50"
            >
              {mobileMenuOpen ? (
                <X className="h-6 w-6" />
              ) : (
                <Menu className="h-6 w-6" />
              )}
            </button>
          </div>
        </div>
      </div>

      {/* Mobile Navigation */}
      {mobileMenuOpen && (
        <div className="md:hidden border-t border-gray-700 bg-gray-900/90 backdrop-blur-sm transition-colors duration-300 animate-slide-down">
          <div className="px-4 py-2 space-y-1">
            {navigation.map((item) => (
              <Link
                key={item.name}
                href={item.href}
                className={`block px-4 py-3 rounded-lg text-base font-medium transition-all duration-300 ${
                  isActive(item.href)
                    ? 'text-white bg-gray-800/50 border border-gray-600'
                    : 'text-gray-300 hover:text-white hover:bg-gray-800/50'
                }`}
                onClick={() => setMobileMenuOpen(false)}
              >
                {item.name}
              </Link>
            ))}
            
            {isAuthenticated && (
              <div className="pt-4 border-t border-gray-700 space-y-1">
                <Link 
                  href="/history" 
                  onClick={() => setMobileMenuOpen(false)}
                  className="flex items-center space-x-2 px-4 py-3 rounded-lg text-base font-medium text-gray-300 hover:text-white hover:bg-gray-800/50 transition-all"
                >
                  <History className="h-4 w-4" />
                  <span>History</span>
                </Link>
                <Link 
                  href="/settings/change-password" 
                  onClick={() => setMobileMenuOpen(false)}
                  className="block px-4 py-3 rounded-lg text-base font-medium text-gray-300 hover:text-white hover:bg-gray-800/50 transition-all"
                >
                  Change Password
                </Link>
                <Link 
                  href="/bookmarks" 
                  onClick={() => setMobileMenuOpen(false)}
                  className="block px-4 py-3 rounded-lg text-base font-medium text-gray-300 hover:text-white hover:bg-gray-800/50 transition-all"
                >
                  Bookmarks
                </Link>
                <button
                  onClick={() => {
                    setMobileMenuOpen(false)
                    handleLogout()
                  }}
                  className="block w-full text-left px-4 py-3 rounded-lg text-base font-medium text-red-400 hover:text-red-300 hover:bg-gray-800/50 transition-all"
                >
                  Logout
                </button>
              </div>
            )}

            {!isAuthenticated && (
              <div className="pt-4 border-t border-gray-700 space-y-2">
                <Link href="/auth/login" onClick={() => setMobileMenuOpen(false)}>
                  <Button variant="outline" size="sm" fullWidth>
                    Sign in
                  </Button>
                </Link>
                <Link href="/auth/register" onClick={() => setMobileMenuOpen(false)}>
                  <Button variant="primary" size="sm" fullWidth>
                    Sign up
                  </Button>
                </Link>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Click outside to close dropdown */}
      {userMenuOpen && (
        <div 
          className="fixed inset-0 z-40" 
          onClick={() => setUserMenuOpen(false)}
        />
      )}
    </header>
  )
}

export default Header