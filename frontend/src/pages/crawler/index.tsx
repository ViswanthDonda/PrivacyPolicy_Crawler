import React, { useEffect, useState } from 'react'
import Layout from '@/components/layout/Layout'
import ProtectedRoute from '@/components/auth/ProtectedRoute'
import UrlInputForm from '@/components/crawler/UrlInputForm'
import SimpleAnalysisDisplay from '@/components/analysis/SimpleAnalysisDisplay'
import { Card, CardContent, CardHeader, CardTitle, Spinner, Button } from '@/components/ui'
import { useCrawler, useCrawlerActions } from '@/store/crawlerStore'
import { useRouter } from 'next/router'
import { History, ArrowRight, CheckCircle, FileText, RefreshCw } from 'lucide-react'
import { auth } from '@/lib/firebase'
import { getIdToken } from 'firebase/auth'
import { apiService } from '@/services'

const CrawlerPage: React.FC = () => {
  const { sessions } = useCrawler()
  const { getCrawlHistory } = useCrawlerActions()
  const router = useRouter()
  const [latestResults, setLatestResults] = useState<any>(null)
  const [loadingResults, setLoadingResults] = useState(false)

  useEffect(() => {
    // Get Firebase ID token and store in localStorage for API authentication
    // THEN fetch crawl history after token is set
    const setupTokenAndFetchHistory = async () => {
      if (auth.currentUser) {
        try {
          // First, get and set the token
          const token = await getIdToken(auth.currentUser)
          if (typeof window !== 'undefined') {
            localStorage.setItem('auth_token', token)
          }
          
          // Only fetch history AFTER token is set
          // This prevents 401 errors from redirecting to login
          await getCrawlHistory()
        } catch (err) {
          console.error('Error getting token:', err)
          // Don't fetch history if token retrieval fails
        }
      }
    }
    
    setupTokenAndFetchHistory()
    
    // Also refresh token periodically (every 50 minutes)
    // And before it expires (tokens expire after 1 hour)
    const tokenRefreshInterval = setInterval(() => {
      if (auth.currentUser) {
        getIdToken(auth.currentUser, true) // Force refresh
        .then((tkn) => {
          if (typeof window !== 'undefined') {
            localStorage.setItem('auth_token', tkn)
          }
        })
        .catch((err) => {
          console.error('Error refreshing token:', err)
          // If token refresh fails, try to get a new one
          if (auth.currentUser) {
            getIdToken(auth.currentUser)
              .then((tkn) => {
                if (typeof window !== 'undefined') {
                  localStorage.setItem('auth_token', tkn)
                }
              })
              .catch((refreshErr) => console.error('Error getting new token:', refreshErr))
          }
        })
      }
    }, 50 * 60 * 1000) // 50 minutes
    
    return () => clearInterval(tokenRefreshInterval)
  }, [getCrawlHistory])

  // Get the most recent completed session for Load button
  const getLatestCompletedSession = () => {
    const validSessions = sessions.filter(s => s && s.id && s.status)
    return validSessions.find(s => s.status === 'completed')
  }

  const loadSessionResults = async (sessionId: string) => {
    setLoadingResults(true)
    try {
      const response = await apiService.getCrawlResults(sessionId)
      if (response.success && response.data) {
        setLatestResults(response.data)
      }
    } catch (error) {
      console.error('Error loading results:', error)
    } finally {
      setLoadingResults(false)
    }
  }

  const handleViewSession = (sessionId: string) => {
    router.push(`/crawler/results/${sessionId}`)
  }

  const handleRefresh = () => {
    // Refresh logic will be handled by the component
  }

  return (
    <ProtectedRoute>
      <Layout title="Analyze Documents - TOS Analyzer">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          {/* Header */}
          <div className="mb-8">
            <h1 className="text-3xl font-bold text-white mb-2">
              Analyze Terms of Service & Privacy Policy
            </h1>
            <p className="text-gray-300">
              Enter a website URL to discover and analyze its Terms of Service and Privacy Policy documents
            </p>
          </div>

          {/* Main Content */}
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
            {/* Input Form */}
            <div className="lg:col-span-2">
              <UrlInputForm />
            </div>

            {/* Sidebar */}
            <div className="space-y-6" id="recent-activity">
              <Card className="bg-gray-800/80 backdrop-blur-sm border-gray-700">
                <CardHeader>
                  <CardTitle className="flex items-center space-x-2 text-white">
                    <History className="h-5 w-5" />
                    <span>Recent Reports Analyzed</span>
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {sessions.length > 0 ? (
                    <div className="space-y-3">
                      {sessions
                        .filter(session => session && session.id && session.url) // Filter out invalid sessions
                        .slice(0, 3)
                        .map((session) => (
                        <div
                          key={session.id}
                          className="flex items-center justify-between p-2 hover:bg-gray-800/50 rounded cursor-pointer transition-colors"
                          onClick={() => handleViewSession(session.id)}
                        >
                          <div className="flex-1 min-w-0">
                            <p className="text-sm font-medium text-gray-200 truncate">
                              {session.url}
                            </p>
                            <p className="text-xs text-gray-400">
                              {new Date(session.created_at).toLocaleDateString()}
                            </p>
                          </div>
                          <ArrowRight className="h-4 w-4 text-gray-400" />
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="text-sm text-gray-400">
                      No recent reports. Start analyzing to see your reports here.
                    </p>
                  )}
                </CardContent>
              </Card>
            </div>
          </div>

          {/* Load Button Section */}
          {getLatestCompletedSession() && !latestResults && !loadingResults && (
            <div className="mt-12">
              <Card className="bg-gray-800/80 backdrop-blur-sm border-gray-700">
                <CardContent className="p-8 text-center">
                  <p className="text-gray-300 mb-4">
                    Analysis completed! Click the button below to load the results.
                  </p>
                  <Button
                    variant="primary"
                    size="lg"
                    onClick={() => {
                      const session = getLatestCompletedSession()
                      if (session && session.id) {
                        loadSessionResults(session.id)
                      }
                    }}
                    leftIcon={<RefreshCw className="h-5 w-5" />}
                  >
                    Load Analysis Results
                  </Button>
                </CardContent>
              </Card>
            </div>
          )}

          {/* Analysis Results Section - Inline Display */}
          {loadingResults ? (
            <div className="mt-12">
              <Card className="bg-gray-800/80 backdrop-blur-sm border-gray-700">
                <CardContent className="p-8 text-center">
                  <Spinner size="lg" label="Loading analysis results..." />
                  <p className="text-gray-300 mt-4">
                    Please wait while we fetch the analysis results...
                  </p>
                </CardContent>
              </Card>
            </div>
          ) : latestResults && latestResults.documents && latestResults.documents.length > 0 ? (
            <div className="mt-12">
              <Card className="bg-gray-800/80 backdrop-blur-sm border-gray-700">
                <CardHeader>
                  <div className="flex items-center justify-between">
                    <CardTitle className="flex items-center space-x-2 text-white">
                      <CheckCircle className="h-5 w-5 text-green-500" />
                      <span>Analysis Results</span>
                    </CardTitle>
                    <div className="text-sm text-gray-400">
                      {latestResults.url}
                    </div>
                  </div>
                </CardHeader>
                <CardContent className="!bg-gray-800/90 p-6">
                  <div className="space-y-8">
                    {latestResults.documents.map((document: any, index: number) => (
                      <div key={document.document_id || index}>
                        <div className="mb-4 pb-4 border-b border-gray-700">
                          <h3 className="text-lg font-semibold text-white mb-2">
                            {document.title || `Document ${index + 1}`}
                          </h3>
                          <p className="text-sm text-gray-400">
                            <span className="capitalize">{document.document_type}</span>
                            {document.word_count && ` • ${document.word_count.toLocaleString()} words`}
                          </p>
                        </div>
                        {document.analysis && (
                          <SimpleAnalysisDisplay analysis={document.analysis} />
                        )}
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            </div>
          ) : null}
        </div>
      </Layout>
    </ProtectedRoute>
  )
}

export default CrawlerPage