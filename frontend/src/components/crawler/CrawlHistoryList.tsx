import React, { useEffect } from 'react'
import { Card, CardContent, CardHeader, CardTitle, Badge, Spinner } from '@/components/ui'
import { useCrawler, useCrawlerActions } from '@/store/crawlerStore'
import { CrawlSession } from '@/types'
import { 
  Clock, 
  CheckCircle, 
  XCircle, 
  AlertCircle,
  Globe,
  FileText,
  ExternalLink,
  RefreshCw
} from 'lucide-react'

interface CrawlHistoryListProps {
  onViewSession?: (sessionId: string) => void
  onRefresh?: () => void
}

const CrawlHistoryList: React.FC<CrawlHistoryListProps> = ({ 
  onViewSession,
  onRefresh 
}) => {
  const { sessions, isLoading, error } = useCrawler()
  const { getCrawlHistory } = useCrawlerActions()

  // Fetch crawl history on component mount
  useEffect(() => {
    getCrawlHistory()
  }, [getCrawlHistory])

  // Return appropriate icon based on crawl status
  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'completed':
        return <CheckCircle className="h-4 w-4 text-green-500" />
      case 'processing':
        return <Spinner size="sm" color="primary" />
      case 'pending':
        return <Clock className="h-4 w-4 text-yellow-500" />
      case 'failed':
        return <XCircle className="h-4 w-4 text-red-500" />
      default:
        return <AlertCircle className="h-4 w-4 text-gray-500" />
    }
  }

  // Return styled badge based on crawl status
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

  // Format date string to localized date format
  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    })
  }

  // Calculate and return relative time string (e.g., "5m ago", "2h ago")
  const getRelativeTime = (dateString: string) => {
    const now = new Date()
    const date = new Date(dateString)
    const diffInMinutes = Math.floor((now.getTime() - date.getTime()) / (1000 * 60))
    
    if (diffInMinutes < 1) return 'Just now'
    if (diffInMinutes < 60) return `${diffInMinutes}m ago`
    if (diffInMinutes < 1440) return `${Math.floor(diffInMinutes / 60)}h ago`
    return `${Math.floor(diffInMinutes / 1440)}d ago`
  }

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-8">
        <Spinner size="lg" label="Loading crawl history..." />
      </div>
    )
  }

  if (error) {
    return (
      <div className="text-center py-8">
        <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
        <p className="text-red-600 mb-4">{error}</p>
        <button
          onClick={onRefresh}
          className="text-primary-600 hover:text-primary-700 font-medium"
        >
          Try again
        </button>
      </div>
    )
  }

  if (sessions.length === 0) {
    return (
      <div className="text-center py-8">
        <Globe className="h-12 w-12 text-gray-400 mx-auto mb-4" />
        <h3 className="text-lg font-medium text-gray-900 mb-2">
          No crawl history yet
        </h3>
        <p className="text-gray-500 mb-4">
          Start analyzing websites to see your crawl history here
        </p>
        <button
          onClick={onRefresh}
          className="text-primary-600 hover:text-primary-700 font-medium"
        >
          Refresh
        </button>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-gray-900">
          Crawl History ({sessions.length})
        </h2>
        <button
          onClick={onRefresh}
          className="flex items-center space-x-2 text-gray-600 hover:text-gray-900 transition-colors"
        >
          <RefreshCw className="h-4 w-4" />
          <span>Refresh</span>
        </button>
      </div>

      <div className="space-y-3">
        {sessions.map((session) => (
          <Card key={session.id} className="hover:shadow-md transition-shadow">
            <CardContent className="p-4">
              <div className="flex items-start justify-between">
                <div className="flex-1 min-w-0">
                  <div className="flex items-center space-x-3 mb-2">
                    {getStatusIcon(session.status)}
                    <div className="flex-1 min-w-0">
                      <a
                        href={session.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-sm font-medium text-gray-900 hover:text-primary-600 transition-colors truncate"
                      >
                        {session.url}
                        <ExternalLink className="inline-block h-3 w-3 ml-1" />
                      </a>
                      <p className="text-xs text-gray-500">
                        {formatDate(session.created_at)} • {getRelativeTime(session.created_at)}
                      </p>
                    </div>
                  </div>

                  <div className="flex items-center space-x-4 mb-3">
                    <div className="flex items-center space-x-2">
                      <FileText className="h-3 w-3 text-gray-400" />
                      <span className="text-xs text-gray-600">
                        {session.document_types.map(type => 
                          type === 'tos' ? 'TOS' : 'Privacy'
                        ).join(', ')}
                      </span>
                    </div>
                    <div className="flex items-center space-x-2">
                      <span className="text-xs text-gray-600">
                        {session.documents_found} found, {session.documents_analyzed} analyzed
                      </span>
                    </div>
                  </div>

                  {session.error_message && (
                    <div className="p-2 bg-red-50 border border-red-200 rounded text-xs text-red-700">
                      {session.error_message}
                    </div>
                  )}
                </div>

                <div className="flex flex-col items-end space-y-2 ml-4">
                  {getStatusBadge(session.status)}
                  
                  {session.status === 'completed' && onViewSession && (
                    <button
                      onClick={() => onViewSession(session.id)}
                      className="text-xs text-primary-600 hover:text-primary-700 font-medium"
                    >
                      View Results
                    </button>
                  )}
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  )
}

export default CrawlHistoryList