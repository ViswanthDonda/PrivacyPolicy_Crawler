import React from 'react'
import { validatePassword } from '@/lib/authService'

interface PasswordValidationProps {
  password: string
  className?: string
}

const PasswordValidation: React.FC<PasswordValidationProps> = ({ password, className = '' }) => {
  // Validate password against security requirements
  const validation = validatePassword(password)
  
  // Define password requirements with validation status
  const requirements = [
    { text: 'At least 8 characters', met: password.length >= 8 },
    { text: 'One uppercase letter', met: /[A-Z]/.test(password) },
    { text: 'One lowercase letter', met: /[a-z]/.test(password) },
    { text: 'One number', met: /\d/.test(password) },
    { text: 'One special character', met: /[!@#$%^&*()_+\-=\[\]{};':"\\|,.<>\/?]/.test(password) }
  ]

  // Don't render if password is empty
  if (!password) return null

  return (
    <div className={`space-y-2 ${className}`}>
      <p className="text-sm text-gray-300 font-medium">Password Requirements:</p>
      <ul className="space-y-1">
        {requirements.map((requirement, index) => (
          <li key={index} className="flex items-center space-x-2">
            <div className={`w-4 h-4 rounded-full flex items-center justify-center ${
              requirement.met ? 'bg-green-500' : 'bg-gray-600'
            }`}>
              {requirement.met && (
                <svg className="w-2 h-2 text-white" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
                </svg>
              )}
            </div>
            <span className={`text-sm ${
              requirement.met ? 'text-green-400' : 'text-gray-400'
            }`}>
              {requirement.text}
            </span>
          </li>
        ))}
      </ul>
    </div>
  )
}

export default PasswordValidation

