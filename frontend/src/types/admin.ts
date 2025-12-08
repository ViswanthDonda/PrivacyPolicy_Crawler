export interface GlobalDocument {
  id: string
  base_url: string
  document_url: string
  document_type: string
  title?: string
  word_count?: number
  last_crawled: string
  crawl_status: string
  version: number
  has_analysis: boolean
  created_at: string
}

export interface GlobalDocumentSearchResponse {
  total: number
  page: number
  limit: number
  documents: GlobalDocument[]
}

export interface DeleteDocumentRequest {
  document_url?: string
  document_id?: string
}

export interface DeleteDocumentResponse {
  success: boolean
  message: string
  deleted_document_url?: string
}

