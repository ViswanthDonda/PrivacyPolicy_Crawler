import React from 'react'
import { Card, CardContent, CardHeader, CardTitle, Badge } from '@/components/ui'
import { AnalysisResult } from '@/types'
import { 
  FileText, 
  Hash, 
  BarChart3, 
  BookOpen, 
  TrendingUp,
  AlertTriangle,
  Target,
  Scale
} from 'lucide-react'

interface MeasurementsTableProps {
  analysis: AnalysisResult
  className?: string
}

const MeasurementsTable: React.FC<MeasurementsTableProps> = ({ 
  analysis, 
  className 
}) => {
  const measurements = analysis.measurements

  // Determine readability level and styling based on Flesch score
  const getReadabilityLevel = (score: number) => {
    if (score >= 80) return { level: 'Very Easy', color: 'text-green-600', bg: 'bg-green-50' }
    if (score >= 60) return { level: 'Easy', color: 'text-green-500', bg: 'bg-green-50' }
    if (score >= 40) return { level: 'Moderate', color: 'text-yellow-600', bg: 'bg-yellow-50' }
    if (score >= 20) return { level: 'Difficult', color: 'text-orange-600', bg: 'bg-orange-50' }
    return { level: 'Very Difficult', color: 'text-red-600', bg: 'bg-red-50' }
  }

  // Classify sentiment as positive, negative, or neutral
  const getSentimentLabel = (score: number) => {
    if (score > 0.1) return { label: 'Positive', color: 'text-green-600', bg: 'bg-green-50' }
    if (score < -0.1) return { label: 'Negative', color: 'text-red-600', bg: 'bg-red-50' }
    return { label: 'Neutral', color: 'text-gray-600', bg: 'bg-gray-50' }
  }

  // Determine risk level category based on score
  const getRiskLevel = (score: number) => {
    if (score >= 7) return { level: 'High', color: 'text-red-600', bg: 'bg-red-50' }
    if (score >= 4) return { level: 'Medium', color: 'text-yellow-600', bg: 'bg-yellow-50' }
    return { level: 'Low', color: 'text-green-600', bg: 'bg-green-50' }
  }

  const readabilityInfo = getReadabilityLevel(measurements.flesch_reading_ease)
  const sentimentInfo = getSentimentLabel(measurements.sentiment_score)
  const riskInfo = getRiskLevel(measurements.risk_indicator_score)

  const measurementGroups = [
    {
      title: 'Basic Statistics',
      icon: <FileText className="h-5 w-5" />,
      measurements: [
        {
          label: 'Word Count',
          value: measurements.word_count.toLocaleString(),
          icon: <Hash className="h-4 w-4" />,
          description: 'Total number of words in the document'
        },
        {
          label: 'Sentence Count',
          value: measurements.sentence_count.toLocaleString(),
          icon: <FileText className="h-4 w-4" />,
          description: 'Total number of sentences'
        },
        {
          label: 'Average Words per Sentence',
          value: measurements.avg_words_per_sentence.toFixed(1),
          icon: <BarChart3 className="h-4 w-4" />,
          description: 'Average sentence length'
        }
      ]
    },
    {
      title: 'Readability Scores',
      icon: <BookOpen className="h-5 w-5" />,
      measurements: [
        {
          label: 'Flesch Reading Ease',
          value: measurements.flesch_reading_ease.toFixed(1),
          icon: <BookOpen className="h-4 w-4" />,
          description: 'Higher scores indicate easier reading',
          badge: (
            <Badge variant="default" className={`${readabilityInfo.color} ${readabilityInfo.bg}`}>
              {readabilityInfo.level}
            </Badge>
          )
        },
        {
          label: 'Flesch-Kincaid Grade Level',
          value: measurements.flesch_kincaid_grade.toFixed(1),
          icon: <TrendingUp className="h-4 w-4" />,
          description: 'U.S. grade level required to understand'
        },
        {
          label: 'Automated Readability Index',
          value: measurements.automated_readability_index.toFixed(1),
          icon: <BarChart3 className="h-4 w-4" />,
          description: 'Another measure of readability'
        }
      ]
    },
    {
      title: 'Content Analysis',
      icon: <Target className="h-5 w-5" />,
      measurements: [
        {
          label: 'Sentiment Score',
          value: measurements.sentiment_score.toFixed(2),
          icon: <TrendingUp className="h-4 w-4" />,
          description: 'Overall sentiment of the text',
          badge: (
            <Badge variant="default" className={`${sentimentInfo.color} ${sentimentInfo.bg}`}>
              {sentimentInfo.label}
            </Badge>
          )
        },
        {
          label: 'Keyword Density',
          value: `${(measurements.keyword_density * 100).toFixed(2)}%`,
          icon: <Target className="h-4 w-4" />,
          description: 'Density of important keywords'
        },
        {
          label: 'Legal Clause Count',
          value: measurements.legal_clause_count.toString(),
          icon: <Scale className="h-4 w-4" />,
          description: 'Number of legal clauses identified'
        }
      ]
    },
    {
      title: 'Risk Assessment',
      icon: <AlertTriangle className="h-5 w-5" />,
      measurements: [
        {
          label: 'Risk Indicator Score',
          value: `${measurements.risk_indicator_score.toFixed(1)}/10`,
          icon: <AlertTriangle className="h-4 w-4" />,
          description: 'Overall risk level of the document',
          badge: (
            <Badge variant="default" className={`${riskInfo.color} ${riskInfo.bg}`}>
              {riskInfo.level} Risk
            </Badge>
          )
        }
      ]
    }
  ]

  return (
    <div className={`space-y-6 ${className}`}>
      {measurementGroups.map((group, groupIndex) => (
        <Card key={groupIndex}>
          <CardHeader>
            <CardTitle className="flex items-center space-x-2">
              {group.icon}
              <span>{group.title}</span>
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {group.measurements.map((measurement, index) => (
                <div
                  key={index}
                  className="p-4 border border-gray-200 rounded-lg hover:border-gray-300 transition-colors"
                >
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center space-x-2">
                      {measurement.icon}
                      <span className="font-medium text-gray-900">
                        {measurement.label}
                      </span>
                    </div>
                    {measurement.badge}
                  </div>
                  
                  <div className="text-2xl font-bold text-gray-900 mb-1">
                    {measurement.value}
                  </div>
                  
                  <p className="text-sm text-gray-600">
                    {measurement.description}
                  </p>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  )
}

export default MeasurementsTable