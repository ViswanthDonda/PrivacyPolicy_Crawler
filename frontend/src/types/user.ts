export interface UserProfile {
  id: string
  email: string
  created_at: string
  updated_at: string
  total_crawls: number
  total_documents: number
  total_favorites: number
  last_activity: string
}

export interface UserStats {
  total_crawls: number
  total_documents: number
  total_favorites: number
  documents_by_type: {
    tos: number
    privacy: number
  }
  recent_activity: ActivityItem[]
}

export interface ActivityItem {
  id: string
  type: 'crawl' | 'favorite' | 'download'
  description: string
  timestamp: string
  metadata?: Record<string, any>
}

export interface UserPreferences {
  theme: 'light' | 'dark' | 'system'
  language: string
  notifications: {
    email: boolean
    push: boolean
    crawl_complete: boolean
    analysis_ready: boolean
  }
  default_document_types: ('tos' | 'privacy')[]
  auto_favorite: boolean
}

export interface UserSettings {
  profile: UserProfile
  stats: UserStats
  preferences: UserPreferences
}

export interface UpdateUserPreferencesRequest {
  theme?: 'light' | 'dark' | 'system'
  language?: string
  notifications?: {
    email?: boolean
    push?: boolean
    crawl_complete?: boolean
    analysis_ready?: boolean
  }
  default_document_types?: ('tos' | 'privacy')[]
  auto_favorite?: boolean
}
