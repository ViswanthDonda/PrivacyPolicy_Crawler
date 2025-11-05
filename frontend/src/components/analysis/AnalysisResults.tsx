import React from 'react'
import { AnalysisResult, Document } from '@/types'
import SummaryCard from './SummaryCard'
import WordFrequencyChart from './WordFrequencyChart'
import MeasurementsTable from './MeasurementsTable'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui'
import { Download, Star, Share2, FileText } from 'lucide-react'

interface AnalysisResultsProps {
  analysis: AnalysisResult
  document: Document
  onDownload?: (format: 'pdf' | 'json') => void
  onToggleFavorite?: () => void
  isFavorite?: boolean
  className?: string
}

const AnalysisResults: React.FC<AnalysisResultsProps> = ({
  analysis,
  document: doc,
  onDownload,
  onToggleFavorite,
  isFavorite = false,
  className
}) => {
  // Handle download of analysis results in specified format
  const handleDownload = (format: 'pdf' | 'json') => {
    if (onDownload) {
      onDownload(format)
    } else {
      // Create download blob from analysis data
      const data = format === 'json' 
        ? JSON.stringify({ analysis, document: doc }, null, 2)
        : 'PDF download not implemented yet'
      
      const blob = new Blob([data], { 
        type: format === 'json' ? 'application/json' : 'application/pdf' 
      })
      
      const url = URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = `analysis-${doc.id}.${format}`
      document.body.appendChild(link)
      link.click()
      document.body.removeChild(link)
      URL.revokeObjectURL(url)
    }
  }

  return (
    <div className={`space-y-6 ${className}`}>
      {/* Header */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="flex items-center space-x-2">
                <FileText className="h-6 w-6" />
                <span>Analysis Results</span>
              </CardTitle>
              <p className="text-sm text-gray-600 mt-1">
                {doc.title} • {doc.domain}
              </p>
            </div>
            
            <div className="flex items-center space-x-2">
              <button
                onClick={() => handleDownload('json')}
                className="flex items-center space-x-2 px-3 py-2 text-sm text-gray-600 hover:text-gray-900 border border-gray-300 rounded-lg hover:border-gray-400 transition-colors"
              >
                <Download className="h-4 w-4" />
                <span>Download JSON</span>
              </button>
              
              <button
                onClick={() => handleDownload('pdf')}
                className="flex items-center space-x-2 px-3 py-2 text-sm text-gray-600 hover:text-gray-900 border border-gray-300 rounded-lg hover:border-gray-400 transition-colors"
              >
                <Download className="h-4 w-4" />
                <span>Download PDF</span>
              </button>
              
              <button
                onClick={onToggleFavorite}
                className={`flex items-center space-x-2 px-3 py-2 text-sm border rounded-lg transition-colors ${
                  isFavorite
                    ? 'text-yellow-600 border-yellow-300 bg-yellow-50 hover:bg-yellow-100'
                    : 'text-gray-600 border-gray-300 hover:border-gray-400 hover:text-gray-900'
                }`}
              >
                <Star className={`h-4 w-4 ${isFavorite ? 'fill-current' : ''}`} />
                <span>{isFavorite ? 'Favorited' : 'Add to Favorites'}</span>
              </button>
            </div>
          </div>
        </CardHeader>
      </Card>

      {/* Summary Cards */}
      <SummaryCard 
        analysis={analysis} 
        documentTitle={doc.title}
      />

      {/* Word Frequency Chart */}
      <WordFrequencyChart 
        analysis={analysis}
        maxWords={15}
      />

      {/* Measurements Table */}
      <MeasurementsTable 
        analysis={analysis}
      />

      {/* Document Details */}
      <Card>
        <CardHeader>
          <CardTitle>Document Details</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
              <h4 className="font-medium text-gray-900 mb-3">Basic Information</h4>
              <dl className="space-y-2">
                <div>
                  <dt className="text-sm font-medium text-gray-500">Document Type</dt>
                  <dd className="text-sm text-gray-900 capitalize">
                    {doc.document_type === 'tos' ? 'Terms of Service' : 'Privacy Policy'}
                  </dd>
                </div>
                <div>
                  <dt className="text-sm font-medium text-gray-500">Domain</dt>
                  <dd className="text-sm text-gray-900">{doc.domain}</dd>
                </div>
                <div>
                  <dt className="text-sm font-medium text-gray-500">URL</dt>
                  <dd className="text-sm text-gray-900 break-all">
                    <a 
                      href={doc.url} 
                      target="_blank" 
                      rel="noopener noreferrer"
                      className="text-primary-600 hover:text-primary-700"
                    >
                      {doc.url}
                    </a>
                  </dd>
                </div>
                <div>
                  <dt className="text-sm font-medium text-gray-500">Analysis Date</dt>
                  <dd className="text-sm text-gray-900">
                    {new Date(analysis.created_at).toLocaleString()}
                  </dd>
                </div>
              </dl>
            </div>
            
            <div>
              <h4 className="font-medium text-gray-900 mb-3">Content Statistics</h4>
              <dl className="space-y-2">
                <div>
                  <dt className="text-sm font-medium text-gray-500">Word Count</dt>
                  <dd className="text-sm text-gray-900">
                    {doc.word_count.toLocaleString()} words
                  </dd>
                </div>
                <div>
                  <dt className="text-sm font-medium text-gray-500">Sentence Count</dt>
                  <dd className="text-sm text-gray-900">
                    {doc.sentence_count.toLocaleString()} sentences
                  </dd>
                </div>
                <div>
                  <dt className="text-sm font-medium text-gray-500">Average Words per Sentence</dt>
                  <dd className="text-sm text-gray-900">
                    {analysis.measurements.avg_words_per_sentence.toFixed(1)}
                  </dd>
                </div>
                <div>
                  <dt className="text-sm font-medium text-gray-500">Reading Level</dt>
                  <dd className="text-sm text-gray-900">
                    Grade {analysis.measurements.flesch_kincaid_grade.toFixed(1)}
                  </dd>
                </div>
              </dl>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

export default AnalysisResults