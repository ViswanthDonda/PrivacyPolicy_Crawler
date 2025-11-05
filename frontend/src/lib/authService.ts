import {
  createUserWithEmailAndPassword,
  signInWithEmailAndPassword,
  signInWithPopup,
  GoogleAuthProvider,
  sendEmailVerification,
  sendPasswordResetEmail,
  signOut,
  onAuthStateChanged,
  User,
  updatePassword,
  EmailAuthProvider,
  reauthenticateWithCredential,
} from 'firebase/auth'
import { doc, setDoc, getDoc } from 'firebase/firestore'
import { auth, db } from './firebase'

// Password validation function
export const validatePassword = (password: string): { isValid: boolean; errors: string[] } => {
  const errors: string[] = []
  
  if (password.length < 8) {
    errors.push('Password must be at least 8 characters long')
  }
  
  if (!/[A-Z]/.test(password)) {
    errors.push('Password must contain at least one uppercase letter')
  }
  
  if (!/[a-z]/.test(password)) {
    errors.push('Password must contain at least one lowercase letter')
  }
  
  if (!/\d/.test(password)) {
    errors.push('Password must contain at least one number')
  }
  
  if (!/[!@#$%^&*()_+\-=\[\]{};':"\\|,.<>\/?]/.test(password)) {
    errors.push('Password must contain at least one special character')
  }
  
  return {
    isValid: errors.length === 0,
    errors
  }
}

// Generate unique user ID
const generateUserId = (): string => {
  return `user_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`
}

// Create user profile in Firestore
const createUserProfile = async (user: User, additionalData?: any) => {
  if (!user) return

  try {
    const userRef = doc(db, 'users', user.uid)
    const userSnap = await getDoc(userRef)

    if (!userSnap.exists()) {
      const { displayName, email } = user
      const createdAt = new Date()
      const userId = generateUserId()
      // Use displayName as username, or additionalData.username if provided
      const username = additionalData?.username || displayName || email?.split('@')[0] || 'User'

      await setDoc(userRef, {
        userId,
        username,
        displayName: displayName || username,
        email,
        createdAt,
        emailVerified: user.emailVerified,
        ...additionalData
      })
    } else {
      // Update existing profile if username is missing
      const existingData = userSnap.data()
      if (!existingData.username && (additionalData?.username || user.displayName)) {
        const username = additionalData?.username || user.displayName || existingData.email?.split('@')[0] || 'User'
        await setDoc(userRef, {
          ...existingData,
          username,
          displayName: user.displayName || username
        }, { merge: true })
      }
    }
  } catch (error) {
    console.error('Error creating user profile:', error)
    // Don't throw error - user creation should still succeed
    // Firestore might be offline or not enabled
  }

  return null
}

// Authentication service class
export class AuthService {
  // Sign up with email and password
  static async signUpWithEmail(email: string, password: string, username?: string) {
    try {
      // Validate password
      const passwordValidation = validatePassword(password)
      if (!passwordValidation.isValid) {
        throw new Error(passwordValidation.errors.join(', '))
      }

      // Create user
      const userCredential = await createUserWithEmailAndPassword(auth, email, password)
      const user = userCredential.user

      // Send email verification
      await sendEmailVerification(user)

      // Create user profile with username
      await createUserProfile(user, { username })

      // IMPORTANT: Sign out the user immediately after signup
      // User should NOT be logged in until they verify email and sign in
      await signOut(auth)

      return {
        success: true,
        user: null, // Return null instead of user to indicate they're not logged in
        message: 'Account created successfully! Please check your email to verify your account.'
      }
    } catch (error: any) {
      console.error('Sign up error:', error)
      // If user was created but something else failed, sign them out
      if (auth.currentUser) {
        try {
          await signOut(auth)
        } catch (signOutError) {
          console.error('Error signing out after failed signup:', signOutError)
        }
      }
      return {
        success: false,
        error: error.message || 'Failed to create account'
      }
    }
  }

  // Sign in with email and password
  static async signInWithEmail(email: string, password: string) {
    try {
      const userCredential = await signInWithEmailAndPassword(auth, email, password)
      const user = userCredential.user

      // Check if email is verified
      if (!user.emailVerified) {
        await signOut(auth) // Sign out if email not verified
        return {
          success: false,
          error: 'Please verify your email before signing in. Check your inbox for the verification email.'
        }
      }

      return {
        success: true,
        user,
        message: 'Signed in successfully!'
      }
    } catch (error: any) {
      console.error('Sign in error:', error)
      return {
        success: false,
        error: error.message || 'Failed to sign in'
      }
    }
  }

  // Sign in with Google
  static async signInWithGoogle() {
    try {
      const provider = new GoogleAuthProvider()
      const result = await signInWithPopup(auth, provider)
      const user = result.user

      // Create user profile
      await createUserProfile(user)

      return {
        success: true,
        user,
        message: 'Signed in with Google successfully!'
      }
    } catch (error: any) {
      console.error('Google sign in error:', error)
      return {
        success: false,
        error: error.message || 'Failed to sign in with Google'
      }
    }
  }

  // Send password reset email
  static async resetPassword(email: string) {
    try {
      await sendPasswordResetEmail(auth, email)
      return {
        success: true,
        message: 'Password reset email sent! Check your inbox.'
      }
    } catch (error: any) {
      console.error('Password reset error:', error)
      return {
        success: false,
        error: error.message || 'Failed to send password reset email'
      }
    }
  }

  // Update password (requires re-authentication)
  static async updateUserPassword(currentPassword: string, newPassword: string) {
    try {
      const user = auth.currentUser
      if (!user || !user.email) {
        throw new Error('No authenticated user found')
      }

      // Validate new password
      const passwordValidation = validatePassword(newPassword)
      if (!passwordValidation.isValid) {
        throw new Error(passwordValidation.errors.join(', '))
      }

      // Re-authenticate user
      const credential = EmailAuthProvider.credential(user.email, currentPassword)
      await reauthenticateWithCredential(user, credential)

      // Update password
      await updatePassword(user, newPassword)

      return {
        success: true,
        message: 'Password updated successfully!'
      }
    } catch (error: any) {
      console.error('Update password error:', error)
      return {
        success: false,
        error: error.message || 'Failed to update password'
      }
    }
  }

  // Resend email verification
  static async resendEmailVerification() {
    try {
      const user = auth.currentUser
      if (!user) {
        throw new Error('No authenticated user found')
      }

      await sendEmailVerification(user)
      return {
        success: true,
        message: 'Verification email sent! Check your inbox.'
      }
    } catch (error: any) {
      console.error('Resend verification error:', error)
      return {
        success: false,
        error: error.message || 'Failed to send verification email'
      }
    }
  }

  // Sign out
  static async signOut() {
    try {
      await signOut(auth)
      return {
        success: true,
        message: 'Signed out successfully!'
      }
    } catch (error: any) {
      console.error('Sign out error:', error)
      return {
        success: false,
        error: error.message || 'Failed to sign out'
      }
    }
  }

  // Get current user
  static getCurrentUser(): User | null {
    return auth.currentUser
  }

  // Listen to auth state changes
  static onAuthStateChanged(callback: (user: User | null) => void) {
    return onAuthStateChanged(auth, callback)
  }

  // Get user profile from Firestore
  static async getUserProfile(uid: string) {
    try {
      const userRef = doc(db, 'users', uid)
      const userSnap = await getDoc(userRef)
      
      if (userSnap.exists()) {
        return {
          success: true,
          profile: userSnap.data()
        }
      } else {
        return {
          success: false,
          error: 'User profile not found'
        }
      }
    } catch (error: any) {
      console.error('Get user profile error:', error)
      return {
        success: false,
        error: 'Firestore offline or not enabled'
      }
    }
  }
}

export default AuthService
