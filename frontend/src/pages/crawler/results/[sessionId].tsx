import React, { useEffect, useState } from 'react'
import { useRouter } from 'next/router'
import Layout from '@/components/layout/Layout'
import ProtectedRoute from '@/components/auth/ProtectedRoute'
import CrawlStatusCard from '@/components/crawler/CrawlStatusCard'
import SimpleAnalysisDisplay from '@/components/analysis/SimpleAnalysisDisplay'
import { Card, CardContent, CardHeader, CardTitle, Spinner } from '@/components/ui'
import { useCrawler } from '@/store/crawlerStore'
import { CrawlSession } from '@/types'
import { ArrowLeft, RefreshCw, FileText, Download } from 'lucide-react'
import { apiService } from '@/services'
import { generatePDFReport } from '@/utils/pdfGenerator'

const CrawlResultsPage: React.FC = () => {
  const router = useRouter()
  const { sessionId } = router.query
  const { sessions } = useCrawler()
  
  const [session, setSession] = useState<CrawlSession | null>(null)
  const [documents, setDocuments] = useState<any[]>([])
  const [analyses, setAnalyses] = useState<any[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (sessionId && typeof sessionId === 'string') {
      // Validate UUID format before making request
      const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
      if (uuidRegex.test(sessionId)) {
        loadSessionData(sessionId)
      } else {
        console.error('Invalid session ID format:', sessionId)
        setError(`Invalid session ID format: ${sessionId}`)
        setIsLoading(false)
      }
    }
  }, [sessionId])

  const loadSessionData = async (id: string) => {
    try {
      setIsLoading(true)
      setError(null)
      
      console.log('Loading session data for ID:', id) // Debug log

      // Fetch from API
      const response = await apiService.getCrawlResults(id)
      
      console.log('Session response:', response) // Debug log
      
      if (response.success && response.data) {
        const sessionData = response.data
        
        // Map backend response to frontend format
        const mappedSession: CrawlSession = {
          id: sessionData.session_id,
          user_id: 'current-user',
          url: sessionData.url,
          document_types: [],
          status: sessionData.status,
          progress: sessionData.analyzed_count / Math.max(sessionData.document_count, 1),
          documents_found: sessionData.document_count,
          documents_analyzed: sessionData.analyzed_count,
          created_at: sessionData.created_at
        }
        
        setSession(mappedSession)
        
        // Extract documents and analyses
        const docs = sessionData.documents || []
        setDocuments(docs)
        
        // Extract analyses
        const analysisList = docs
          .filter((doc: any) => doc.analysis)
          .map((doc: any) => ({
            ...doc.analysis,
            document_id: doc.document_id
          }))
        setAnalyses(analysisList)
        
      } else {
        setError(response.error?.message || 'Failed to load session')
      }
    } catch (err) {
      console.error('Error loading session:', err)
      setError('Failed to load session data')
    } finally {
      setIsLoading(false)
    }
  }

  const handleRefresh = () => {
    if (sessionId && typeof sessionId === 'string') {
      loadSessionData(sessionId)
    }
  }

  const handleViewDocument = (documentId: string) => {
    router.push(`/documents/${documentId}`)
  }

  if (isLoading) {
    return (
      <ProtectedRoute>
        <Layout title="Loading Results - TOS Crawler">
          <div className="min-h-screen flex items-center justify-center">
            <Spinner size="lg" label="Loading analysis results..." />
          </div>
        </Layout>
      </ProtectedRoute>
    )
  }

  if (error || !session) {
    return (
      <ProtectedRoute>
        <Layout title="Error - TOS Crawler">
          <div className="min-h-screen flex items-center justify-center">
          <div className="text-center">
            <h1 className="text-2xl font-bold text-white mb-4">
              {error || 'Session not found'}
            </h1>
            <p className="text-gray-300 mb-6">
              The requested analysis session could not be found or loaded.
            </p>
            <button
              onClick={() => router.push('/crawler')}
              className="text-blue-400 hover:text-blue-300 font-medium"
            >
              Back to Analyzer
            </button>
          </div>
          </div>
        </Layout>
      </ProtectedRoute>
    )
  }

  return (
    <ProtectedRoute>
      <Layout title="Analysis Results - TOS Crawler">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          {/* Header */}
          <div className="mb-8">
            <div className="flex items-center space-x-4 mb-4">
              <button
                onClick={() => router.back()}
                className="flex items-center space-x-2 text-gray-300 hover:text-white transition-colors"
              >
                <ArrowLeft className="h-4 w-4" />
                <span>Back</span>
              </button>
              
              <button
                onClick={handleRefresh}
                className="flex items-center space-x-2 text-gray-300 hover:text-white transition-colors"
              >
                <RefreshCw className="h-4 w-4" />
                <span>Refresh</span>
              </button>
            </div>
            
            <h1 className="text-4xl font-bold text-white mb-2">
              Analysis Results
            </h1>
            <p className="text-gray-300">
              Results for {session.url}
            </p>
          </div>

          {/* Session Status */}
          <div className="mb-8">
            <CrawlStatusCard session={session} />
          </div>

          {/* Results */}
          {session.status === 'completed' && documents.length > 0 ? (
            <div className="space-y-8">
              {documents.map((document, index) => {
                const analysis = document.analysis
                
                return (
                  <div key={document.document_id || index}>
                    <Card className="bg-gray-800/80 backdrop-blur-sm border-gray-700">
                      <CardContent className="p-6">
                        <div className="mb-4">
                          <h2 className="text-2xl font-bold text-white mb-2">
                            {document.title || 'Document ' + (index + 1)}
                          </h2>
                          <p className="text-gray-300">
                            <span className="inline-flex items-center space-x-2">
                              <FileText className="h-4 w-4" />
                              <span className="capitalize">{document.document_type}</span>
                            </span>
                            {' • '}
                            <a href={document.url} target="_blank" rel="noopener noreferrer" className="text-blue-400 hover:text-blue-300 hover:underline">
                              {document.url}
                            </a>
                          </p>
                          {document.word_count && (
                            <p className="text-sm text-gray-400 mt-1">
                              Word count: {document.word_count.toLocaleString()}
                            </p>
                          )}
                        </div>
                      </CardContent>
                    </Card>
                    
                    {analysis ? (
                      <div className="mt-4">
                        <div className="mb-4 flex justify-end">
                          <button
                            onClick={() => {
                              // Convert analysis to match AnalysisResult type
                              const analysisResult = {
                                document_id: document.document_id || '',
                                summary_100: analysis.summary_100_words || '',
                                summary_sentence: analysis.summary_one_sentence || '',
                                word_frequency: analysis.word_frequency || {},
                                measurements: analysis.measurements || {},
                                created_at: analysis.created_at || new Date().toISOString()
                              }
                              
                              // Convert document to match Document type
                              const docForPDF = {
                                id: document.document_id || '',
                                url: document.url || '',
                                domain: document.url ? new URL(document.url).hostname : '',
                                document_type: (document.document_type === 'terms_of_service' ? 'tos' : 'privacy') as 'tos' | 'privacy',
                                title: document.title || '',
                                content: '',
                                word_count: document.word_count || 0,
                                sentence_count: analysis.measurements?.sentence_count || 0,
                                created_at: document.created_at || new Date().toISOString(),
                                updated_at: document.updated_at || new Date().toISOString()
                              }
                              
                              generatePDFReport({
                                analysis: analysisResult,
                                document: docForPDF,
                                sessionUrl: document.url
                              })
                            }}
                            className="flex items-center space-x-2 px-4 py-2 text-sm bg-blue-600 hover:bg-blue-700 text-white rounded-lg transition-colors"
                          >
                            <Download className="h-4 w-4" />
                            <span>Download PDF Report</span>
                          </button>
                        </div>
                        <SimpleAnalysisDisplay analysis={analysis} />
                      </div>
                    ) : (
                      <Card className="bg-gray-800/80 backdrop-blur-sm border-gray-700">
                        <CardContent className="p-8 text-center">
                          <p className="text-gray-300">No analysis available for this document.</p>
                        </CardContent>
                      </Card>
                    )}
                  </div>
                )
              })}
            </div>
          ) : session.status === 'processing' ? (
            <Card className="bg-gray-800/80 backdrop-blur-sm border-gray-700">
              <CardContent className="p-8 text-center">
                <Spinner size="lg" label="Processing documents..." />
                <p className="text-gray-300 mt-4">
                  This may take a few minutes. The page will update automatically.
                </p>
              </CardContent>
            </Card>
          ) : session.status === 'failed' ? (
            <Card className="bg-gray-800/80 backdrop-blur-sm border-gray-700">
              <CardContent className="p-8 text-center">
                <h3 className="text-lg font-semibold text-white mb-2">
                  Analysis Failed
                </h3>
                <p className="text-gray-300 mb-4">
                  {session.error_message || 'An error occurred during analysis.'}
                </p>
                <button
                  onClick={() => router.push('/crawler')}
                  className="text-blue-400 hover:text-blue-300 font-medium"
                >
                  Try Again
                </button>
              </CardContent>
            </Card>
          ) : (
            <Card className="bg-gray-800/80 backdrop-blur-sm border-gray-700">
              <CardContent className="p-8 text-center">
                <Spinner size="lg" label="Starting analysis..." />
                <p className="text-gray-300 mt-4">
                  Initializing document analysis...
                </p>
              </CardContent>
            </Card>
          )}
        </div>
      </Layout>
    </ProtectedRoute>
  )
}

export default CrawlResultsPage