import React from 'react'
import Layout from '@/components/layout/Layout'
import Link from 'next/link'
import {
  Search,
  BarChart3,
  Shield,
  Zap,
  History,
  FileText,
  Bookmark,
  User,
  Settings
} from 'lucide-react'

const FeaturesPage: React.FC = () => {
  const features = [
    {
      icon: <Search className="h-6 w-6" />,
      title: 'Smart Document Discovery',
      description: 'Automatically finds and extracts Terms of Service and Privacy Policy documents from any website with intelligent parsing and content extraction.'
    },
    {
      icon: <BarChart3 className="h-6 w-6" />,
      title: 'Advanced Analysis',
      description: 'Text mining and NLP to understand document complexity, readability, and key legal points. Word frequency charts and measurements tables.'
    },
    {
      icon: <Shield className="h-6 w-6" />,
      title: 'Risk Assessment',
      description: 'Identify potential risks and legal implications with risk scoring and legal clause identification.'
    },
    {
      icon: <Zap className="h-6 w-6" />,
      title: 'Real-time Processing',
      description: 'Fast processing pipeline with live progress and status updates.'
    },
    {
      icon: <History className="h-6 w-6" />,
      title: 'Crawl History',
      description: 'View all past crawl sessions with status, documents found, and quick access to results.'
    },
    {
      icon: <FileText className="h-6 w-6" />,
      title: 'Detailed Results & PDF Export',
      description: 'Per-document analysis summaries and generate PDF reports for sharing or records.'
    },
    {
      icon: <Bookmark className="h-6 w-6" />,
      title: 'Bookmarks',
      description: 'Save documents and analyses for quick access (coming soon).'
    },
    {
      icon: <User className="h-6 w-6" />,
      title: 'User Accounts',
      description: 'Firebase authentication with registration, login, and secure sessions.'
    },
    {
      icon: <Settings className="h-6 w-6" />,
      title: 'Settings',
      description: 'Change password and manage account settings.'
    }
  ]

  return (
    <Layout title="Features - TOS Crawler">
      <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-16">
        <h1 className="text-4xl font-bold text-white mb-4">Application Features</h1>
        <p className="text-xl text-gray-300 mb-12">
          Everything TOS Crawler offers to help you analyze Terms of Service and Privacy Policy documents.
        </p>
        <div className="space-y-8">
          {features.map((feature, index) => (
            <div
              key={index}
              className="flex gap-4 p-6 bg-gray-900/50 rounded-xl border border-gray-700"
            >
              <div className="flex-shrink-0 w-12 h-12 bg-gray-800 rounded-lg flex items-center justify-center text-gray-300">
                {feature.icon}
              </div>
              <div>
                <h2 className="text-xl font-semibold text-white mb-2">{feature.title}</h2>
                <p className="text-gray-300">{feature.description}</p>
              </div>
            </div>
          ))}
        </div>
        <div className="mt-12 text-center">
          <Link href="/crawler" className="btn-primary px-6 py-3 inline-flex items-center gap-2">
            Start Analyzing
            <Zap className="h-4 w-4" />
          </Link>
        </div>
      </div>
    </Layout>
  )
}

export default FeaturesPage
