import React from 'react'
import Layout from '@/components/layout/Layout'
import RegisterForm from '@/components/auth/RegisterForm'
import { Card, CardContent } from '@/components/ui'

const RegisterPage: React.FC = () => {
  return (
    <Layout title="Sign Up - TOS Crawler">
      <div className="min-h-screen flex items-center justify-center py-12 px-4 sm:px-6 lg:px-8">
        <div className="max-w-md w-full">
          <div className="bg-gray-900/80 backdrop-blur-sm rounded-2xl border border-gray-700 shadow-2xl p-8">
            <RegisterForm />
          </div>
        </div>
      </div>
    </Layout>
  )
}

export default RegisterPage