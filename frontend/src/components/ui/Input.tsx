import React from 'react'
import { clsx } from 'clsx'

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  label?: string
  error?: string
  helperText?: string
  leftIcon?: React.ReactNode
  rightIcon?: React.ReactNode
  fullWidth?: boolean
}

const Input = React.forwardRef<HTMLInputElement, InputProps>(
  ({
    className,
    label,
    error,
    helperText,
    leftIcon,
    rightIcon,
    fullWidth = false,
    id,
    ...props
  }, ref) => {
    const inputId = id || `input-${Math.random().toString(36).substr(2, 9)}`
    
    const baseClasses = 'block w-full rounded-lg border-2 border-gray-600 bg-gray-800 text-white placeholder-gray-400 shadow-lg focus:border-gray-400 focus:ring-2 focus:ring-gray-400 focus:ring-offset-2 focus:ring-offset-black sm:text-sm transition-all duration-300'
    const errorClasses = error ? 'border-red-500 focus:border-red-500 focus:ring-red-500' : ''
    const iconClasses = leftIcon ? 'pl-10' : rightIcon ? 'pr-10' : ''
    const widthClasses = fullWidth ? 'w-full' : ''
    
    return (
      <div className={clsx('space-y-1', fullWidth && 'w-full')}>
        {label && (
          <label
            htmlFor={inputId}
            className="block text-sm font-medium text-gray-300"
          >
            {label}
          </label>
        )}
        
        <div className="relative">
          {leftIcon && (
            <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
              <span className="text-gray-400 sm:text-sm">
                {leftIcon}
              </span>
            </div>
          )}
          
          <input
            id={inputId}
            className={clsx(
              baseClasses,
              errorClasses,
              iconClasses,
              widthClasses,
              className
            )}
            ref={ref}
            {...props}
          />
          
          {rightIcon && (
            <div className="absolute inset-y-0 right-0 pr-3 flex items-center">
              <span className="text-gray-400 sm:text-sm">
                {rightIcon}
              </span>
            </div>
          )}
        </div>
        
        {error && (
          <p className="text-sm text-red-400">{error}</p>
        )}
        
        {helperText && !error && (
          <p className="text-sm text-gray-400">{helperText}</p>
        )}
      </div>
    )
  }
)

Input.displayName = 'Input'

export default Input