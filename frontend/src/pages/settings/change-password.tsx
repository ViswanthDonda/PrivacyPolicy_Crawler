import React, { useState } from 'react'
import Layout from '@/components/layout/Layout'
import ProtectedRoute from '@/components/auth/ProtectedRoute'
import { Button, Input } from '@/components/ui'
import { useAuth } from '@/hooks/useAuth'
import { Eye, EyeOff, Lock, Key, AlertCircle } from 'lucide-react'
import toast from 'react-hot-toast'
import { useRouter } from 'next/router'
import PasswordValidation from '@/components/auth/PasswordValidation'

const ChangePasswordPage: React.FC = () => {
  const [showCurrentPassword, setShowCurrentPassword] = useState(false)
  const [showNewPassword, setShowNewPassword] = useState(false)
  const [showConfirmPassword, setShowConfirmPassword] = useState(false)
  const [showPasswordValidation, setShowPasswordValidation] = useState(false)
  const [loading, setLoading] = useState(false)
  const { updatePassword } = useAuth()
  const router = useRouter()

  const [formData, setFormData] = useState({
    currentPassword: '',
    newPassword: '',
    confirmPassword: ''
  })

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()

    if (formData.newPassword !== formData.confirmPassword) {
      toast.error('New passwords do not match')
      return
    }

    if (formData.newPassword.length < 8) {
      toast.error('New password must be at least 8 characters')
      return
    }

    setLoading(true)
    try {
      const result = await updatePassword(formData.currentPassword, formData.newPassword)
      if (result.success) {
        toast.success(result.message || 'Password updated successfully!')
        router.push('/crawler')
      } else {
        toast.error(result.error || 'Failed to update password')
      }
    } catch (error) {
      console.error('Password update error:', error)
      toast.error('An unexpected error occurred')
    } finally {
      setLoading(false)
    }
  }

  return (
    <ProtectedRoute>
      <Layout title="Change Password - TOS Analyzer">
        <div className="min-h-screen flex items-center justify-center py-12 px-4 sm:px-6 lg:px-8">
          <div className="max-w-md w-full">
            <div className="bg-gray-900/80 backdrop-blur-sm rounded-2xl border border-gray-700 shadow-2xl p-8">
              <div className="text-center mb-8">
                <div className="w-16 h-16 bg-gray-800 rounded-full flex items-center justify-center mx-auto mb-4">
                  <Key className="h-8 w-8 text-gray-300" />
                </div>
                <h2 className="text-3xl font-bold text-white">Change Password</h2>
                <p className="mt-2 text-sm text-gray-300">
                  Update your password to keep your account secure
                </p>
              </div>

              <form onSubmit={handleSubmit} className="space-y-6">
                <Input
                  type={showCurrentPassword ? 'text' : 'password'}
                  label="Current Password"
                  placeholder="Enter your current password"
                  value={formData.currentPassword}
                  onChange={(e) => setFormData({ ...formData, currentPassword: e.target.value })}
                  required
                  leftIcon={<Lock className="h-4 w-4" />}
                  rightIcon={
                    <button
                      type="button"
                      onClick={() => setShowCurrentPassword(!showCurrentPassword)}
                      className="text-gray-400 hover:text-gray-600"
                    >
                      {showCurrentPassword ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                    </button>
                  }
                  fullWidth
                />

                <div>
                  <Input
                    type={showNewPassword ? 'text' : 'password'}
                    label="New Password"
                    placeholder="Enter your new password"
                    value={formData.newPassword}
                    onChange={(e) => setFormData({ ...formData, newPassword: e.target.value })}
                    onFocus={() => setShowPasswordValidation(true)}
                    onBlur={() => setShowPasswordValidation(false)}
                    required
                    leftIcon={<Lock className="h-4 w-4" />}
                    rightIcon={
                      <button
                        type="button"
                        onClick={() => setShowNewPassword(!showNewPassword)}
                        className="text-gray-400 hover:text-gray-600"
                      >
                        {showNewPassword ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                      </button>
                    }
                    fullWidth
                  />
                  
                  {showPasswordValidation && formData.newPassword && (
                    <div className="mt-3">
                      <PasswordValidation password={formData.newPassword} />
                    </div>
                  )}
                </div>

                <Input
                  type={showConfirmPassword ? 'text' : 'password'}
                  label="Confirm New Password"
                  placeholder="Confirm your new password"
                  value={formData.confirmPassword}
                  onChange={(e) => setFormData({ ...formData, confirmPassword: e.target.value })}
                  required
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

                {formData.confirmPassword && formData.newPassword !== formData.confirmPassword && (
                  <div className="flex items-center space-x-2 text-sm text-red-400">
                    <AlertCircle className="h-4 w-4" />
                    <span>Passwords do not match</span>
                  </div>
                )}

                <div className="flex space-x-4">
                  <Button
                    type="button"
                    variant="outline"
                    size="lg"
                    onClick={() => router.back()}
                    fullWidth
                  >
                    Cancel
                  </Button>
                  <Button
                    type="submit"
                    variant="primary"
                    size="lg"
                    loading={loading}
                    fullWidth
                  >
                    Update Password
                  </Button>
                </div>
              </form>
            </div>
          </div>
        </div>
      </Layout>
    </ProtectedRoute>
  )
}

export default ChangePasswordPage

