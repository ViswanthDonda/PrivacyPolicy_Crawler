import React from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui'
import { FileText, BarChart3 } from 'lucide-react'

interface SimpleAnalysisDisplayProps {
  analysis: {
    summary_100_words?: string
    summary_one_sentence?: string
    word_frequency?: Record<string, number>
    measurements?: Record<string, any>
  }
}

const SimpleAnalysisDisplay: React.FC<SimpleAnalysisDisplayProps> = ({ analysis }) => {
  // Extract and sort top 20 most frequent words
  const topWords = analysis.word_frequency
    ? Object.entries(analysis.word_frequency)
        .sort(([, a], [, b]) => (b as number) - (a as number))
        .slice(0, 20)
    : []

  return (
    <div className="space-y-6">
      {/* One Sentence Summary */}
      {analysis.summary_one_sentence && (
        <Card className="bg-gray-800/80 backdrop-blur-sm border-gray-700">
          <CardHeader>
            <CardTitle className="flex items-center space-x-2 text-lg text-white">
              <FileText className="h-5 w-5" />
              <span>One-Sentence Summary</span>
            </CardTitle>
          </CardHeader>
          <CardContent className="!bg-gray-800/80 p-6">
            <p className="text-gray-300 leading-relaxed text-base">
              {analysis.summary_one_sentence}
            </p>
          </CardContent>
        </Card>
      )}

      {/* 100-Word Summary */}
      {analysis.summary_100_words && (
        <Card className="bg-gray-800/80 backdrop-blur-sm border-gray-700">
          <CardHeader>
            <CardTitle className="flex items-center space-x-2 text-lg text-white">
              <FileText className="h-5 w-5" />
              <span>100-Word Summary</span>
            </CardTitle>
          </CardHeader>
          <CardContent className="!bg-gray-800/80 p-6">
            <p className="text-gray-300 leading-relaxed whitespace-pre-wrap text-base">
              {analysis.summary_100_words}
            </p>
          </CardContent>
        </Card>
      )}

      {/* Word Frequency */}
      {topWords.length > 0 && (
        <Card className="bg-gray-800/80 backdrop-blur-sm border-gray-700">
          <CardHeader>
            <CardTitle className="flex items-center space-x-2 text-lg text-white">
              <BarChart3 className="h-5 w-5" />
              <span>Top 20 Word Frequency</span>
            </CardTitle>
          </CardHeader>
          <CardContent className="!bg-gray-800/80 p-6">
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-3">
              {topWords.map(([word, count]) => (
                <div
                  key={word}
                  className="flex items-center justify-between p-3 bg-gray-900/50 rounded-lg border border-gray-700"
                >
                  <span className="text-sm font-medium text-gray-200 truncate">
                    {word}
                  </span>
                  <span className="text-xs text-gray-400 ml-2">
                    {count}
                  </span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Measurements Summary (if available) */}
      {analysis.measurements && (
        <Card className="bg-gray-800/80 backdrop-blur-sm border-gray-700">
          <CardHeader>
            <CardTitle className="text-lg text-white">Key Metrics</CardTitle>
          </CardHeader>
          <CardContent className="!bg-gray-800/80 p-6">
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              {analysis.measurements.word_count && (
                <div className="text-center p-3 bg-gray-900/50 rounded-lg border border-gray-700">
                  <div className="text-2xl font-bold text-white">
                    {analysis.measurements.word_count.toLocaleString()}
                  </div>
                  <div className="text-xs text-gray-400 mt-1">Words</div>
                </div>
              )}
              {analysis.measurements.sentence_count && (
                <div className="text-center p-3 bg-gray-900/50 rounded-lg border border-gray-700">
                  <div className="text-2xl font-bold text-white">
                    {analysis.measurements.sentence_count.toLocaleString()}
                  </div>
                  <div className="text-xs text-gray-400 mt-1">Sentences</div>
                </div>
              )}
              {analysis.measurements.sentiment_score !== undefined && (
                <div className="text-center p-3 bg-gray-900/50 rounded-lg border border-gray-700">
                  <div className="text-2xl font-bold text-white">
                    {analysis.measurements.sentiment_score.toFixed(2)}
                  </div>
                  <div className="text-xs text-gray-400 mt-1">Sentiment</div>
                </div>
              )}
              {analysis.measurements.risk_indicator_score !== undefined && (
                <div className="text-center p-3 bg-gray-900/50 rounded-lg border border-gray-700">
                  <div className="text-2xl font-bold text-white">
                    {Math.round(analysis.measurements.risk_indicator_score)}
                  </div>
                  <div className="text-xs text-gray-400 mt-1">Risk Score</div>
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}

export default SimpleAnalysisDisplay

