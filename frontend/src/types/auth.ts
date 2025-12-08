export interface User {
  id: string
  email: string
  username?: string
  displayName?: string
  firebase_uid?: string
  is_admin?: boolean
  created_at: string
  updated_at: string
  is_active: boolean
}

export interface AuthState {
  user: User | null
  token: string | null
  isAuthenticated: boolean
  isLoading: boolean
}

export interface LoginCredentials {
  email: string
  password: string
}

export interface RegisterCredentials {
  username: string
  email: string
  password: string
  confirm_password: string
}

export interface AuthResponse {
  success: boolean
  data: {
    user: User
    access_token: string
    token_type: string
    expires_in: number
  }
  message?: string
}

export interface GoogleAuthRequest {
  id_token: string
}

export interface RefreshTokenRequest {
  refresh_token: string
}

export interface RefreshTokenResponse {
  success: boolean
  data: {
    access_token: string
    token_type: string
    expires_in: number
  }
}
