import React from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui'
import { AnalysisResult } from '@/types'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'

interface WordFrequencyChartProps {
  analysis: AnalysisResult
  maxWords?: number
  className?: string
}

const WordFrequencyChart: React.FC<WordFrequencyChartProps> = ({ 
  analysis, 
  maxWords = 20,
  className 
}) => {
  // Transform word frequency data into chart format, sorted by frequency
  const chartData = Object.entries(analysis.word_frequency)
    .sort(([, a], [, b]) => b - a)
    .slice(0, maxWords)
    .map(([word, count]) => ({
      word: word.length > 10 ? `${word.substring(0, 10)}...` : word,
      count,
      fullWord: word
    }))

  // Calculate total word count from frequency data
  const totalWords = Object.values(analysis.word_frequency).reduce((sum, count) => sum + count, 0)

  // Custom tooltip component for chart hover display
  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload
      return (
        <div className="bg-white p-3 border border-gray-200 rounded-lg shadow-lg">
          <p className="font-medium text-gray-900">{data.fullWord}</p>
          <p className="text-sm text-gray-600">
            Count: {data.count}
          </p>
          <p className="text-sm text-gray-600">
            Percentage: {((data.count / totalWords) * 100).toFixed(2)}%
          </p>
        </div>
      )
    }
    return null
  }

  return (
    <Card className={className}>
      <CardHeader>
        <CardTitle>Word Frequency Analysis</CardTitle>
        <p className="text-sm text-gray-600">
          Top {maxWords} most frequently used words
        </p>
      </CardHeader>
      <CardContent>
        <div className="h-80 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis 
                dataKey="word" 
                tick={{ fontSize: 12 }}
                angle={-45}
                textAnchor="end"
                height={80}
              />
              <YAxis 
                tick={{ fontSize: 12 }}
                label={{ value: 'Count', angle: -90, position: 'insideLeft' }}
              />
              <Tooltip content={<CustomTooltip />} />
              <Bar 
                dataKey="count" 
                fill="#3b82f6"
                radius={[4, 4, 0, 0]}
              />
            </BarChart>
          </ResponsiveContainer>
        </div>
        
        <div className="mt-4 grid grid-cols-2 gap-4 text-sm">
          <div>
            <span className="font-medium text-gray-700">Total Words:</span>
            <p className="text-gray-600">{totalWords.toLocaleString()}</p>
          </div>
          <div>
            <span className="font-medium text-gray-700">Unique Words:</span>
            <p className="text-gray-600">{Object.keys(analysis.word_frequency).length}</p>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

export default WordFrequencyChart