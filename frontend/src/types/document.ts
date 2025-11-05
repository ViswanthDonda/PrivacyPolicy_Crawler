import { Pagination } from './common'

export interface Document {
  id: string
  url: string
  domain: string
  document_type: 'tos' | 'privacy'
  title: string
  content: string
  word_count: number
  sentence_count: number
  created_at: string
  updated_at: string
}

export interface DocumentResponse {
  success: boolean
  data: Document
  message?: string
}

export interface DocumentSearchParams {
  q?: string
  document_type?: 'tos' | 'privacy'
  domain?: string
  page?: number
  limit?: number
}

export interface DocumentSearchResponse {
  success: boolean
  data: {
    documents: Document[]
    pagination: Pagination
  }
  message?: string
}

export interface UserFavorite {
  id: string
  user_id: string
  document_id: string
  created_at: string
}

export interface FavoritesResponse {
  success: boolean
  data: {
    documents: (Document & { favorited_at: string })[]
    pagination: Pagination
  }
  message?: string
}

export interface FavoriteActionResponse {
  success: boolean
  message: string
}

