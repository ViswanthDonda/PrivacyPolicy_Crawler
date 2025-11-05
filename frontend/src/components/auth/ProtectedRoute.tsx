import React from 'react'
import { useRouter } from 'next/router'
import { useAuth } from '@/hooks/useAuth'
import { Spinner } from '@/components/ui'

interface ProtectedRouteProps {
  children: React.ReactNode
  fallback?: React.ReactNode
}

const ProtectedRoute: React.FC<ProtectedRouteProps> = ({ 
  children, 
  fallback 
}) => {
  const { user, loading } = useAuth()
  const router = useRouter()
  const isAuthenticated = !!user

  // Redirect to login if user is not authenticated
  React.useEffect(() => {
    if (!loading && !isAuthenticated) {
      router.push('/auth/login')
    }
  }, [isAuthenticated, loading, router])

  // Show loading spinner while checking authentication
  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-black">
        <Spinner size="lg" label="Loading..." />
      </div>
    )
  }

  // Show fallback or redirect message if not authenticated
  if (!isAuthenticated) {
    return fallback || (
      <div className="min-h-screen flex items-center justify-center bg-black">
        <Spinner size="lg" label="Redirecting to login..." />
      </div>
    )
  }

  // Render protected content if authenticated
  return <>{children}</>
}

export default ProtectedRoute