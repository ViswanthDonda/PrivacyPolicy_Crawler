export interface AnalysisResult {
  document_id: string
  summary_100: string
  summary_sentence: string
  word_frequency: Record<string, number>
  measurements: TextMiningMeasurements
  created_at: string
}

export interface TextMiningMeasurements {
  word_count: number
  sentence_count: number
  avg_words_per_sentence: number
  flesch_reading_ease: number
  flesch_kincaid_grade: number
  automated_readability_index: number
  sentiment_score: number
  keyword_density: number
  legal_clause_count: number
  risk_indicator_score: number
}

export interface AnalysisResponse {
  success: boolean
  data: AnalysisResult
  message?: string
}

export interface WordFrequencyItem {
  word: string
  count: number
  percentage: number
}

export interface RiskIndicator {
  level: 'low' | 'medium' | 'high'
  score: number
  description: string
  factors: string[]
}

export interface LegalClause {
  type: string
  content: string
  risk_level: 'low' | 'medium' | 'high'
  start_position: number
  end_position: number
}

export interface SentimentAnalysis {
  score: number
  magnitude: number
  label: 'positive' | 'negative' | 'neutral'
  confidence: number
}

export interface ReadabilityScores {
  flesch_reading_ease: number
  flesch_kincaid_grade: number
  automated_readability_index: number
  coleman_liau_index: number
  smog_index: number
  average_grade_level: number
}
