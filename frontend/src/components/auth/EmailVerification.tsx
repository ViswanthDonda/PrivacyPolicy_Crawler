import React, { useState } from 'react'
import { Button } from '@/components/ui'
import { useAuth } from '@/hooks/useAuth'
import { Mail, CheckCircle, AlertCircle, RefreshCw } from 'lucide-react'
import toast from 'react-hot-toast'

interface EmailVerificationProps {
  email: string
  onVerified?: () => void
}

const EmailVerification: React.FC<EmailVerificationProps> = ({ email, onVerified }) => {
  const [isResending, setIsResending] = useState(false)
  const { resendEmailVerification, user } = useAuth()

  // Resend email verification link
  const handleResendVerification = async () => {
    setIsResending(true)
    try {
      const result = await resendEmailVerification()
      if (result.success) {
        toast.success(result.message || 'Verification email sent! Check your inbox.')
      } else {
        toast.error(result.error || 'Failed to send verification email')
      }
    } catch (error) {
      console.error('Resend verification error:', error)
      toast.error('An unexpected error occurred')
    } finally {
      setIsResending(false)
    }
  }

  // Check if email has been verified
  const handleCheckVerification = () => {
    if (user?.emailVerified) {
      toast.success('Email verified successfully!')
      onVerified?.()
    } else {
      toast.error('Email not verified yet. Please check your inbox.')
    }
  }

  return (
    <div className="w-full max-w-md mx-auto text-center">
      <div className="mb-8">
        <div className="w-16 h-16 bg-gray-800 rounded-full flex items-center justify-center mx-auto mb-4">
          <Mail className="h-8 w-8 text-gray-300" />
        </div>
        <h2 className="text-3xl font-bold text-white mb-2">Verify your email</h2>
        <p className="text-gray-300">
          We've sent a verification link to{' '}
          <span className="text-white font-medium">{email}</span>
        </p>
      </div>

      <div className="space-y-4">
        <div className="bg-gray-800/50 rounded-lg p-4 border border-gray-700">
          <div className="flex items-center space-x-3">
            <AlertCircle className="h-5 w-5 text-yellow-400" />
            <div className="text-left">
              <p className="text-sm text-gray-300">
                Please check your email and click the verification link to activate your account.
              </p>
            </div>
          </div>
        </div>

        <div className="space-y-3">
          <Button
            type="button"
            variant="primary"
            size="lg"
            onClick={handleCheckVerification}
            fullWidth
            leftIcon={<CheckCircle className="h-4 w-4" />}
          >
            I've verified my email
          </Button>

          <Button
            type="button"
            variant="outline"
            size="lg"
            onClick={handleResendVerification}
            loading={isResending}
            fullWidth
            leftIcon={<RefreshCw className="h-4 w-4" />}
          >
            Resend verification email
          </Button>
        </div>

        <div className="text-sm text-gray-400">
          <p>Didn't receive the email? Check your spam folder or try resending.</p>
        </div>
      </div>
    </div>
  )
}

export default EmailVerification

