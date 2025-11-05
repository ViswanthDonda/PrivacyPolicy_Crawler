import React from 'react'
import { Card, CardContent, CardHeader, CardTitle, Badge, Spinner } from '@/components/ui'
import { CrawlSession } from '@/types'
import { 
  CheckCircle, 
  Clock, 
  AlertCircle, 
  XCircle, 
  Globe, 
  FileText,
  ExternalLink 
} from 'lucide-react'

interface CrawlStatusCardProps {
  session: CrawlSession
  onViewResults?: (sessionId: string) => void
}

const CrawlStatusCard: React.FC<CrawlStatusCardProps> = ({ 
  session, 
  onViewResults 
}) => {
  // Return appropriate icon based on crawl status
  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'completed':
        return <CheckCircle className="h-5 w-5 text-green-500" />
      case 'processing':
        return <Spinner size="sm" color="primary" />
      case 'pending':
        return <Clock className="h-5 w-5 text-yellow-500" />
      case 'failed':
        return <XCircle className="h-5 w-5 text-red-500" />
      default:
        return <AlertCircle className="h-5 w-5 text-gray-500" />
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

  // Format date string to localized date/time format
  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleString()
  }

  // Determine progress bar color based on completion percentage
  const getProgressColor = (progress: number) => {
    if (progress < 30) return 'bg-red-500'
    if (progress < 70) return 'bg-yellow-500'
    return 'bg-green-500'
  }

  return (
    <Card className="w-full bg-gray-800/80 backdrop-blur-sm border-gray-700">
      <CardHeader>
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-3">
            {getStatusIcon(session.status)}
            <div>
              <CardTitle className="text-lg text-white">
                <a 
                  href={session.url} 
                  target="_blank" 
                  rel="noopener noreferrer"
                  className="text-blue-400 hover:text-blue-300 transition-colors"
                >
                  {session.url}
                  <ExternalLink className="inline-block h-4 w-4 ml-1" />
                </a>
              </CardTitle>
              <p className="text-sm text-gray-400 mt-1">
                Started {formatDate(session.created_at)}
              </p>
            </div>
          </div>
          {getStatusBadge(session.status)}
        </div>
      </CardHeader>

      <CardContent>
        <div className="space-y-4">
          {/* Progress Bar */}
          {(session.status === 'processing' || session.status === 'pending') && (
            <div>
              <div className="flex justify-between text-sm text-gray-300 mb-1">
                <span>Progress</span>
                <span>{Math.round(session.progress)}%</span>
              </div>
              <div className="w-full bg-gray-700 rounded-full h-2">
                <div
                  className={`h-2 rounded-full transition-all duration-300 ${getProgressColor(session.progress)}`}
                  style={{ width: `${session.progress}%` }}
                />
              </div>
            </div>
          )}

          {/* Document Types */}
          {session.document_types && session.document_types.length > 0 && (
            <div>
              <p className="text-sm font-medium text-gray-300 mb-2">Document Types</p>
              <div className="flex flex-wrap gap-2">
                {session.document_types.map((type) => (
                  <Badge key={type} variant="secondary">
                    {type === 'tos' ? 'Terms of Service' : 'Privacy Policy'}
                  </Badge>
                ))}
              </div>
            </div>
          )}

          {/* Error Message */}
          {session.error_message && (
            <div className="p-3 bg-red-900/30 border border-red-700 rounded-lg">
              <div className="flex items-center space-x-2">
                <AlertCircle className="h-4 w-4 text-red-400" />
                <p className="text-sm text-red-300">{session.error_message}</p>
              </div>
            </div>
          )}

          {/* Stats */}
          <div className="grid grid-cols-2 gap-4 pt-4 border-t border-gray-700">
            <div className="text-center">
              <p className="text-2xl font-bold text-white">
                {session.documents_found || 0}
              </p>
              <p className="text-sm text-gray-400">Documents Found</p>
            </div>
            <div className="text-center">
              <p className="text-2xl font-bold text-white">
                {session.documents_analyzed || 0}
              </p>
              <p className="text-sm text-gray-400">Analyzed</p>
            </div>
          </div>

          {/* Actions */}
          {session.status === 'completed' && onViewResults && (
            <div className="pt-4 border-t border-gray-700">
              <button
                onClick={() => onViewResults(session.id)}
                className="w-full bg-blue-600 text-white py-2 px-4 rounded-lg hover:bg-blue-700 transition-colors"
              >
                View Analysis Results
              </button>
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  )
}

export default CrawlStatusCard