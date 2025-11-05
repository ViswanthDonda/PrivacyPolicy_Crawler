import { Pagination } from './common'

export type DocumentType = 'tos' | 'privacy'
export type CrawlStatus = 'pending' | 'processing' | 'completed' | 'failed'

export interface CrawlRequest {
  url: string
  document_types: DocumentType[]
  force_refresh?: boolean
}

export interface CrawlSession {
  id: string
  user_id: string
  url: string
  document_types: DocumentType[]
  status: CrawlStatus
  progress: number
  documents_found: number
  documents_analyzed: number
  error_message?: string
  created_at: string
  completed_at?: string
  documents?: CrawlDocument[]
}

export interface CrawlDocument {
  id: string
  type: DocumentType
  title: string
  url: string
  status: 'pending' | 'analyzed' | 'failed'
}

export interface CrawlResponse {
  success: boolean
  data: {
    session_id: string
    status: CrawlStatus
    url: string
    document_types: DocumentType[]
    estimated_time: string
  }
  message?: string
}

export interface CrawlStatusResponse {
  success: boolean
  data: CrawlSession
  message?: string
}

export interface CrawlHistoryResponse {
  success: boolean
  data: {
    sessions: CrawlSession[]
    pagination: Pagination
  }
  message?: string
}

