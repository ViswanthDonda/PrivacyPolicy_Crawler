import React, { useState, useEffect } from 'react'
import { useForm } from 'react-hook-form'
import Link from 'next/link'
import { useRouter } from 'next/router'
import { Button, Input, Modal } from '@/components/ui'
import PasswordValidation from './PasswordValidation'
import { useAuth } from '@/hooks/useAuth'
import { RegisterCredentials } from '@/types'
import { Eye, EyeOff, Mail, Lock, User, CheckCircle, AlertCircle, Mail as MailIcon } from 'lucide-react'
import toast from 'react-hot-toast'

const RegisterForm: React.FC = () => {
  const [showPassword, setShowPassword] = useState(false)
  const [showConfirmPassword, setShowConfirmPassword] = useState(false)
  const [showPasswordValidation, setShowPasswordValidation] = useState(false)
  const [showVerificationModal, setShowVerificationModal] = useState(false)
  const [redirectCountdown, setRedirectCountdown] = useState(3)
  const router = useRouter()
  const { signUpWithEmail, signInWithGoogle, error, loading } = useAuth()
  
  const {
    register,
    handleSubmit,
    formState: { errors },
    watch,
    setError: setFormError
  } = useForm<RegisterCredentials>()

  const password = watch('password')
  const confirmPassword = watch('confirm_password')

  // Countdown timer for automatic redirect to login page
  useEffect(() => {
    if (showVerificationModal && redirectCountdown > 0) {
      const timer = setTimeout(() => {
        setRedirectCountdown(redirectCountdown - 1)
      }, 1000)
      return () => clearTimeout(timer)
    } else if (showVerificationModal && redirectCountdown === 0) {
      router.push('/auth/login')
    }
  }, [showVerificationModal, redirectCountdown, router])

  // Handle registration form submission
  const onSubmit = async (data: RegisterCredentials) => {
    try {
      const result = await signUpWithEmail(data.email, data.password, data.username)
      if (result.success) {
        setShowVerificationModal(true)
      } else {
        toast.error(result.error || 'Failed to create account')
        setFormError('root', {
          type: 'manual',
          message: result.error || 'Registration failed. Please try again.'
        })
      }
    } catch (error) {
      console.error('Registration error:', error)
      toast.error('An unexpected error occurred')
      setFormError('root', {
        type: 'manual',
        message: 'An unexpected error occurred. Please try again.'
      })
    }
  }

  // Handle Google OAuth registration
  const handleGoogleSignUp = async () => {
    try {
      const result = await signInWithGoogle()
      if (result.success) {
        toast.success(result.message || 'Signed up with Google successfully!')
        router.push('/crawler')
      } else {
        toast.error(result.error || 'Failed to sign up with Google')
      }
    } catch (error) {
      console.error('Google sign up error:', error)
      toast.error('An unexpected error occurred')
    }
  }

  return (
    <div className="w-full max-w-md mx-auto">
      <div className="text-center mb-8">
        <h2 className="text-3xl font-bold text-white">Create your account</h2>
        <p className="mt-2 text-sm text-gray-300">
          Get started with your free account today
        </p>
      </div>

      <form onSubmit={handleSubmit(onSubmit)} className="space-y-6">
        <Input
          {...register('username', {
            required: 'Username is required',
            minLength: {
              value: 3,
              message: 'Username must be at least 3 characters'
            },
            maxLength: {
              value: 30,
              message: 'Username must be less than 30 characters'
            },
            pattern: {
              value: /^[a-zA-Z0-9_-]+$/,
              message: 'Username can only contain letters, numbers, underscores, and hyphens'
            }
          })}
          type="text"
          label="Username"
          placeholder="Choose a username"
          error={errors.username?.message}
          leftIcon={<User className="h-4 w-4" />}
          fullWidth
        />

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

        <div>
          <Input
            {...register('password', {
              required: 'Password is required',
              minLength: {
                value: 8,
                message: 'Password must be at least 8 characters'
              },
              pattern: {
                value: /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[!@#$%^&*()_+\-=\[\]{};':"\\|,.<>\/?])/,
                message: 'Password must contain at least one uppercase letter, one lowercase letter, one number, and one special character'
              }
            })}
            type={showPassword ? 'text' : 'password'}
            label="Password"
            placeholder="Create a password"
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
            onFocus={() => setShowPasswordValidation(true)}
            onBlur={() => setShowPasswordValidation(false)}
            fullWidth
          />
          
          {showPasswordValidation && password && (
            <div className="mt-3">
              <PasswordValidation password={password} />
            </div>
          )}
        </div>

        <Input
          {...register('confirm_password', {
            required: 'Please confirm your password',
            validate: (value) => {
              if (value !== password) {
                return 'Passwords do not match'
              }
              return true
            }
          })}
          type={showConfirmPassword ? 'text' : 'password'}
          label="Confirm Password"
          placeholder="Confirm your password"
          error={errors.confirm_password?.message}
          leftIcon={<Lock className="h-4 w-4" />}
          rightIcon={
            <button
              type="button"
              onClick={() => setShowConfirmPassword(!showConfirmPassword)}
              className="text-gray-400 hover:text-gray-600"
            >
              {showConfirmPassword ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
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

        <Button
          type="submit"
          variant="primary"
          size="lg"
          loading={loading}
          fullWidth
        >
          Create account
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
            onClick={handleGoogleSignUp}
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
          Already have an account?{' '}
          <Link href="/auth/login" className="font-medium text-gray-400 hover:text-white">
            Sign in
          </Link>
        </p>
      </div>

      {/* Email Verification Modal */}
      <Modal
        isOpen={showVerificationModal}
        onClose={() => {}}
        size="md"
        className="bg-gray-900 border border-gray-700"
      >
        <div className="text-center py-6">
          <div className="w-16 h-16 bg-gray-800 rounded-full flex items-center justify-center mx-auto mb-4">
            <MailIcon className="h-8 w-8 text-gray-300" />
          </div>
          <h3 className="text-2xl font-bold text-white mb-2">
            Check Your Email
          </h3>
          <p className="text-gray-300 mb-6">
            We've sent a verification link to your email address.
          </p>
          <div className="bg-gray-800/50 rounded-lg p-4 mb-6 border border-gray-700">
            <p className="text-sm text-gray-300 mb-2">
              Please check your <span className="font-semibold text-white">inbox</span> or <span className="font-semibold text-white">spam folder</span> to complete your email verification.
            </p>
            <p className="text-xs text-gray-400">
              You'll be redirected to the sign in page in {redirectCountdown} second{redirectCountdown !== 1 ? 's' : ''}...
            </p>
          </div>
          <Button
            onClick={() => router.push('/auth/login')}
            variant="primary"
            fullWidth
          >
            Go to Sign In
          </Button>
        </div>
      </Modal>
    </div>
  )
}

export default RegisterForm