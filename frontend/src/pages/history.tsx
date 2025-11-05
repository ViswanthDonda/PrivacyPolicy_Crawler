import React, { useEffect, useState } from 'react'
import Layout from '@/components/layout/Layout'
import ProtectedRoute from '@/components/auth/ProtectedRoute'
import { Card, CardContent, CardHeader, CardTitle, Spinner, Badge } from '@/components/ui'
import { useRouter } from 'next/router'
import { apiService } from '@/services'
import { CrawlSession } from '@/types'
import { 
  History, 
  ExternalLink, 
  CheckCircle, 
  Clock, 
  XCircle, 
  FileText,
  ArrowRight,
  Search,
  Trash2,
  ArrowLeft
} from 'lucide-react'
import toast from 'react-hot-toast'

const HistoryPage: React.FC = () => {
  const router = useRouter()
  const [sessions, setSessions] = useState<CrawlSession[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    loadHistory()
  }, [])

  const loadHistory = async () => {
    setIsLoading(true)
    setError(null)
    try {
      const response = await apiService.getCrawlHistory({
        page: 1,
        limit: 50
      })
      
      if (response.success && response.data) {
        // Backend returns array of sessions directly (List[CrawlStatusResponse])
        // Or wrapped in data field
        let historyData: any[] = []
        
        if (Array.isArray(response.data)) {
          historyData = response.data
        } else if (response.data && (response.data as any).sessions) {
          historyData = (response.data as any).sessions
        } else if (response.data && Array.isArray((response.data as any).data)) {
          historyData = (response.data as any).data
        }
        
        // Map backend format to frontend CrawlSession format
        const mappedSessions: CrawlSession[] = historyData.map((session: any) => {
          // Handle both direct session object and nested data
          const sessionData = session.data || session
          
          return {
            id: String(sessionData.id || sessionData.session_id || session.id),
            user_id: 'current-user',
            url: sessionData.url || session.url || '',
            document_types: [], // Will be populated if available
            status: sessionData.status || session.status || 'pending',
            progress: sessionData.analyzed_count && sessionData.document_count
              ? (sessionData.analyzed_count / sessionData.document_count) * 100
              : session.analyzed_count && session.document_count
              ? (session.analyzed_count / session.document_count) * 100
              : 0,
            documents_found: sessionData.document_count || session.document_count || 0,
            documents_analyzed: sessionData.analyzed_count || session.analyzed_count || 0,
            error_message: sessionData.error_message || session.error_message,
            created_at: sessionData.created_at || session.created_at || new Date().toISOString(),
            completed_at: sessionData.completed_at || session.completed_at
          }
        })
        
        setSessions(mappedSessions)
      } else {
        setError(response.error?.message || 'Failed to load history')
      }
    } catch (err) {
      console.error('Error loading history:', err)
      setError('Failed to load analysis history')
    } finally {
      setIsLoading(false)
    }
  }

  const handleViewResults = (sessionId: string) => {
    router.push(`/crawler/results/${sessionId}`)
  }

  const handleDeleteSession = async (sessionId: string, e: React.MouseEvent) => {
    e.stopPropagation() // Prevent card click
    
    if (!confirm('Are you sure you want to delete this analysis? This action cannot be undone.')) {
      return
    }

    try {
      const response = await apiService.deleteCrawlSession(sessionId)
      
      if (response.success) {
        toast.success('Analysis deleted successfully')
        // Remove from local state
        setSessions(sessions.filter(s => s.id !== sessionId))
      } else {
        toast.error(response.error?.message || 'Failed to delete analysis')
      }
    } catch (err) {
      console.error('Error deleting session:', err)
      toast.error('Failed to delete analysis')
    }
  }

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'completed':
        return <CheckCircle className="h-5 w-5 text-green-500" />
      case 'processing':
        return <Clock className="h-5 w-5 text-yellow-500" />
      case 'failed':
        return <XCircle className="h-5 w-5 text-red-500" />
      default:
        return <Clock className="h-5 w-5 text-gray-500" />
    }
  }

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'completed':
        return <Badge variant="success">Completed</Badge>
      case 'processing':
        return <Badge variant="primary">Processing</Badge>
      case 'pending':
        return <Badge variant="warning">Pending</Badge>
      case 'failed':
        return <Badge variant="error">Failed</Badge>
      default:
        return <Badge variant="default">Unknown</Badge>
    }
  }

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleString()
  }

  return (
    <ProtectedRoute>
      <Layout title="Analysis History - TOS Crawler">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          {/* Header */}
          <div className="mb-8">
            <div className="flex items-center space-x-4 mb-4">
              <button
                onClick={() => router.push('/crawler')}
                className="flex items-center space-x-2 text-gray-300 hover:text-white transition-colors"
              >
                <ArrowLeft className="h-5 w-5" />
                <span>Back to Analyzer</span>
              </button>
            </div>
            <div className="flex items-center space-x-3 mb-4">
              <History className="h-8 w-8 text-white" />
              <h1 className="text-4xl font-bold text-white">
                Analysis History
              </h1>
            </div>
            <p className="text-gray-300 text-lg">
              View all your analyzed websites and access detailed results
            </p>
          </div>

          {/* Loading State */}
          {isLoading && (
            <div className="flex items-center justify-center py-20">
              <Spinner size="lg" label="Loading history..." />
            </div>
          )}

          {/* Error State */}
          {error && !isLoading && (
            <Card className="bg-gray-800/80 backdrop-blur-sm border-gray-700">
              <CardContent className="p-8 text-center">
                <XCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
                <h3 className="text-xl font-semibold text-white mb-2">
                  Error Loading History
                </h3>
                <p className="text-gray-300 mb-4">{error}</p>
                <button
                  onClick={loadHistory}
                  className="text-blue-400 hover:text-blue-300 font-medium"
                >
                  Try Again
                </button>
              </CardContent>
            </Card>
          )}

          {/* Sessions List */}
          {!isLoading && !error && (
            <>
              {sessions.length === 0 ? (
                <Card className="bg-gray-800/80 backdrop-blur-sm border-gray-700">
                  <CardContent className="p-12 text-center">
                    <Search className="h-16 w-16 text-gray-500 mx-auto mb-4" />
                    <h3 className="text-xl font-semibold text-white mb-2">
                      No Analysis History
                    </h3>
                    <p className="text-gray-400 mb-6">
                      Start analyzing websites to see your history here.
                    </p>
                    <button
                      onClick={() => router.push('/crawler')}
                      className="text-blue-400 hover:text-blue-300 font-medium"
                    >
                      Go to Analyzer →
                    </button>
                  </CardContent>
                </Card>
              ) : (
                <div className="space-y-4">
                  {sessions.map((session) => (
                    <Card 
                      key={session.id}
                      className="bg-gray-800/80 backdrop-blur-sm border-gray-700 hover:border-gray-600 transition-all cursor-pointer"
                      onClick={() => session.status === 'completed' && handleViewResults(session.id)}
                    >
                      <CardContent className="p-6">
                        <div className="flex items-start justify-between">
                          {/* Left Side - Info */}
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center space-x-3 mb-3">
                              {getStatusIcon(session.status)}
                              <div className="flex-1 min-w-0">
                                <a
                                  href={session.url}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  onClick={(e) => e.stopPropagation()}
                                  className="text-lg font-semibold text-white hover:text-blue-400 transition-colors flex items-center space-x-2"
                                >
                                  <span className="truncate">{session.url}</span>
                                  <ExternalLink className="h-4 w-4 flex-shrink-0" />
                                </a>
                              </div>
                            </div>

                            <div className="flex items-center space-x-4 text-sm text-gray-400 mb-3">
                              <span className="flex items-center space-x-1">
                                <FileText className="h-4 w-4" />
                                <span>{session.documents_found || 0} documents</span>
                              </span>
                              <span>•</span>
                              <span>{formatDate(session.created_at)}</span>
                            </div>

                            {/* Progress Bar for processing/pending */}
                            {(session.status === 'processing' || session.status === 'pending') && (
                              <div className="mb-3">
                                <div className="flex justify-between text-xs text-gray-400 mb-1">
                                  <span>Progress</span>
                                  <span>{Math.round(session.progress)}%</span>
                                </div>
                                <div className="w-full bg-gray-700 rounded-full h-2">
                                  <div
                                    className={`h-2 rounded-full transition-all ${
                                      session.progress < 30 ? 'bg-red-500' :
                                      session.progress < 70 ? 'bg-yellow-500' : 'bg-green-500'
                                    }`}
                                    style={{ width: `${session.progress}%` }}
                                  />
                                </div>
                              </div>
                            )}

                            {/* Error Message */}
                            {session.error_message && (
                              <div className="mt-3 p-3 bg-red-900/30 border border-red-700 rounded-lg">
                                <p className="text-sm text-red-300">{session.error_message}</p>
                              </div>
                            )}
                          </div>

                          {/* Right Side - Status & Action */}
                          <div className="flex flex-col items-end space-y-3 ml-4">
                            <div className="flex items-center space-x-3">
                              {getStatusBadge(session.status)}
                              <button
                                onClick={(e) => handleDeleteSession(session.id, e)}
                                className="p-2 text-red-400 hover:text-red-300 hover:bg-red-900/30 rounded-lg transition-colors"
                                title="Delete analysis"
                              >
                                <Trash2 className="h-5 w-5" />
                              </button>
                            </div>
                            
                            {session.status === 'completed' && (
                              <button
                                onClick={(e) => {
                                  e.stopPropagation()
                                  handleViewResults(session.id)
                                }}
                                className="flex items-center space-x-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg transition-colors text-sm font-medium"
                              >
                                <span>View Results</span>
                                <ArrowRight className="h-4 w-4" />
                              </button>
                            )}
                          </div>
                        </div>
                      </CardContent>
                    </Card>
                  ))}
                </div>
              )}
            </>
          )}
        </div>
      </Layout>
    </ProtectedRoute>
  )
}

export default HistoryPage

