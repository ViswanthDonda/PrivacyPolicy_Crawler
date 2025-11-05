import React, { useState } from 'react'
import { useForm } from 'react-hook-form'
import Link from 'next/link'
import { useRouter } from 'next/router'
import { Button, Input } from '@/components/ui'
import { useAuth } from '@/hooks/useAuth'
import { LoginCredentials } from '@/types'
import { Eye, EyeOff, Mail, Lock, AlertCircle } from 'lucide-react'
import toast from 'react-hot-toast'

const LoginForm: React.FC = () => {
  const [showPassword, setShowPassword] = useState(false)
  const [showForgotPassword, setShowForgotPassword] = useState(false)
  const [forgotPasswordEmail, setForgotPasswordEmail] = useState('')
  const router = useRouter()
  const { signInWithEmail, signInWithGoogle, resetPassword, error, loading } = useAuth()
  
  const {
    register,
    handleSubmit,
    formState: { errors },
    setError: setFormError,
    watch
  } = useForm<LoginCredentials>()

  const email = watch('email')

  // Handle email/password login form submission
  const onSubmit = async (data: LoginCredentials) => {
    try {
      const result = await signInWithEmail(data.email, data.password)
      if (result.success) {
        toast.success(result.message || 'Signed in successfully!')
        router.push('/crawler')
      } else {
        toast.error(result.error || 'Failed to sign in')
        setFormError('root', {
          type: 'manual',
          message: result.error || 'Invalid email or password'
        })
      }
    } catch (error) {
      console.error('Login error:', error)
      toast.error('An unexpected error occurred')
      setFormError('root', {
        type: 'manual',
        message: 'An unexpected error occurred. Please try again.'
      })
    }
  }

  // Handle Google OAuth sign in
  const handleGoogleSignIn = async () => {
    try {
      const result = await signInWithGoogle()
      if (result.success) {
        toast.success(result.message || 'Signed in with Google successfully!')
        router.push('/crawler')
      } else {
        toast.error(result.error || 'Failed to sign in with Google')
      }
    } catch (error) {
      console.error('Google sign in error:', error)
      toast.error('An unexpected error occurred')
    }
  }

  // Handle password reset request
  const handleForgotPassword = async () => {
    if (!forgotPasswordEmail) {
      toast.error('Please enter your email address')
      return
    }

    try {
      const result = await resetPassword(forgotPasswordEmail)
      if (result.success) {
        toast.success(result.message || 'Password reset email sent! Check your inbox.')
        setShowForgotPassword(false)
        setForgotPasswordEmail('')
      } else {
        toast.error(result.error || 'Failed to send password reset email')
      }
    } catch (error) {
      console.error('Password reset error:', error)
      toast.error('An unexpected error occurred')
    }
  }

  if (showForgotPassword) {
    return (
      <div className="w-full max-w-md mx-auto">
        <div className="text-center mb-8">
          <h2 className="text-3xl font-bold text-white">Reset your password</h2>
          <p className="mt-2 text-sm text-gray-300">
            Enter your email address and we'll send you a link to reset your password
          </p>
        </div>

        <div className="space-y-6">
          <Input
            type="email"
            label="Email address"
            placeholder="Enter your email"
            value={forgotPasswordEmail}
            onChange={(e) => setForgotPasswordEmail(e.target.value)}
            leftIcon={<Mail className="h-4 w-4" />}
            fullWidth
          />

          <Button
            type="button"
            variant="primary"
            size="lg"
            onClick={handleForgotPassword}
            loading={loading}
            fullWidth
          >
            Send reset link
          </Button>

          <div className="text-center">
            <button
              type="button"
              onClick={() => setShowForgotPassword(false)}
              className="text-sm text-gray-400 hover:text-white"
            >
              Back to sign in
            </button>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="w-full max-w-md mx-auto">
      <div className="text-center mb-8">
        <h2 className="text-3xl font-bold text-white">Welcome back</h2>
        <p className="mt-2 text-sm text-gray-300">
          Sign in to your account to continue
        </p>
      </div>

      <form onSubmit={handleSubmit(onSubmit)} className="space-y-6">
        <Input
          {...register('email', {
            required: 'Email is required',
            pattern: {
              value: /^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$/i,
              message: 'Invalid email address'
            }
          })}
          type="email"
          label="Email address"
          placeholder="Enter your email"
          error={errors.email?.message}
          leftIcon={<Mail className="h-4 w-4" />}
          fullWidth
        />

        <Input
          {...register('password', {
            required: 'Password is required',
            minLength: {
              value: 6,
              message: 'Password must be at least 6 characters'
            }
          })}
          type={showPassword ? 'text' : 'password'}
          label="Password"
          placeholder="Enter your password"
          error={errors.password?.message}
          leftIcon={<Lock className="h-4 w-4" />}
          rightIcon={
            <button
              type="button"
              onClick={() => setShowPassword(!showPassword)}
              className="text-gray-400 hover:text-gray-600"
            >
              {showPassword ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
            </button>
          }
          fullWidth
        />

        {errors.root && (
          <div className="flex items-center space-x-2 text-sm text-red-400">
            <AlertCircle className="h-4 w-4" />
            <span>{errors.root.message}</span>
          </div>
        )}

        {error && (
          <div className="flex items-center space-x-2 text-sm text-red-400">
            <AlertCircle className="h-4 w-4" />
            <span>{error}</span>
          </div>
        )}

        <div className="flex items-center justify-between">
          <div className="flex items-center">
            <input
              id="remember-me"
              name="remember-me"
              type="checkbox"
              className="h-4 w-4 text-gray-400 focus:ring-gray-400 border-gray-600 rounded bg-gray-800"
            />
            <label htmlFor="remember-me" className="ml-2 block text-sm text-gray-300">
              Remember me
            </label>
          </div>

          <div className="text-sm">
            <button
              type="button"
              onClick={() => setShowForgotPassword(true)}
              className="font-medium text-gray-400 hover:text-white"
            >
              Forgot your password?
            </button>
          </div>
        </div>

        <Button
          type="submit"
          variant="primary"
          size="lg"
          loading={loading}
          fullWidth
        >
          Sign in
        </Button>
      </form>

      <div className="mt-6">
        <div className="relative">
          <div className="absolute inset-0 flex items-center">
            <div className="w-full border-t border-gray-600" />
          </div>
          <div className="relative flex justify-center text-sm">
            <span className="px-2 bg-black text-gray-400">Or continue with</span>
          </div>
        </div>

        <div className="mt-6">
          <Button
            type="button"
            variant="outline"
            size="lg"
            fullWidth
            onClick={handleGoogleSignIn}
            loading={loading}
            leftIcon={
              <svg className="w-5 h-5" viewBox="0 0 24 24">
                <path
                  fill="currentColor"
                  d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"
                />
                <path
                  fill="currentColor"
                  d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"
                />
                <path
                  fill="currentColor"
                  d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"
                />
                <path
                  fill="currentColor"
                  d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"
                />
              </svg>
            }
          >
            Continue with Google
          </Button>
        </div>
      </div>

      <div className="mt-6 text-center">
        <p className="text-sm text-gray-300">
          Don't have an account?{' '}
          <Link href="/auth/register" className="font-medium text-gray-400 hover:text-white">
            Sign up
          </Link>
        </p>
      </div>
    </div>
  )
}

export default LoginForm