import React from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui'
import { AnalysisResult } from '@/types'
import { FileText, Clock, AlertTriangle } from 'lucide-react'

interface SummaryCardProps {
  analysis: AnalysisResult
  documentTitle?: string
  className?: string
}

const SummaryCard: React.FC<SummaryCardProps> = ({ 
  analysis, 
  documentTitle,
  className 
}) => {
  // Determine risk level and styling based on risk score
  const getRiskLevel = (score: number) => {
    if (score >= 7) return { level: 'High', color: 'text-red-600', bg: 'bg-red-50' }
    if (score >= 4) return { level: 'Medium', color: 'text-yellow-600', bg: 'bg-yellow-50' }
    return { level: 'Low', color: 'text-green-600', bg: 'bg-green-50' }
  }

  const riskInfo = getRiskLevel(analysis.measurements.risk_indicator_score)

  return (
    <div className={`space-y-6 ${className}`}>
      {/* 100-word Summary */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center space-x-2">
            <FileText className="h-5 w-5" />
            <span>Summary</span>
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-gray-700 leading-relaxed">
            {analysis.summary_100}
          </p>
        </CardContent>
      </Card>

      {/* One-sentence Summary */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center space-x-2">
            <Clock className="h-5 w-5" />
            <span>Key Points</span>
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="p-4 bg-blue-50 border border-blue-200 rounded-lg">
            <p className="text-blue-900 font-medium">
              {analysis.summary_sentence}
            </p>
          </div>
        </CardContent>
      </Card>

      {/* Risk Assessment */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center space-x-2">
            <AlertTriangle className="h-5 w-5" />
            <span>Risk Assessment</span>
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className={`p-4 ${riskInfo.bg} border rounded-lg`}>
            <div className="flex items-center justify-between mb-2">
              <span className={`font-semibold ${riskInfo.color}`}>
                {riskInfo.level} Risk
              </span>
              <span className={`text-lg font-bold ${riskInfo.color}`}>
                {analysis.measurements.risk_indicator_score.toFixed(1)}/10
              </span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-2">
              <div
                className={`h-2 rounded-full transition-all duration-300 ${
                  riskInfo.level === 'High' ? 'bg-red-500' :
                  riskInfo.level === 'Medium' ? 'bg-yellow-500' : 'bg-green-500'
                }`}
                style={{ width: `${(analysis.measurements.risk_indicator_score / 10) * 100}%` }}
              />
            </div>
            <p className="text-sm text-gray-600 mt-2">
              Based on legal complexity, data collection practices, and user rights
            </p>
          </div>
        </CardContent>
      </Card>

      {/* Document Info */}
      {documentTitle && (
        <Card>
          <CardHeader>
            <CardTitle>Document Information</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <span className="font-medium text-gray-700">Title:</span>
                <p className="text-gray-600">{documentTitle}</p>
              </div>
              <div>
                <span className="font-medium text-gray-700">Analysis Date:</span>
                <p className="text-gray-600">
                  {new Date(analysis.created_at).toLocaleDateString()}
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}

export default SummaryCard