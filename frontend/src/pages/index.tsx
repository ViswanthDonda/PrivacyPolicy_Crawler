import React, { useState, useEffect } from 'react'
import Link from 'next/link'
import { useRouter } from 'next/router'
import Layout from '@/components/layout/Layout'
import { Button, Modal } from '@/components/ui'
import { useAuth } from '@/hooks/useAuth'
import { 
  Search, 
  BarChart3, 
  Shield, 
  Zap,
  ArrowRight,
  CheckCircle,
  FileText,
  Target,
  Clock,
  Star,
  LogOut,
  AlertCircle
} from 'lucide-react'
import toast from 'react-hot-toast'

const HomePage: React.FC = () => {
  const [flippedCards, setFlippedCards] = useState<Set<number>>(new Set())
  const [showLogoutWarning, setShowLogoutWarning] = useState(false)
  const router = useRouter()
  const { user, loading, signOut } = useAuth()

  // Check if user is logged in and show warning
  useEffect(() => {
    if (!loading && user) {
      setShowLogoutWarning(true)
    } else if (!loading && !user) {
      setShowLogoutWarning(false)
    }
  }, [user, loading])

  const handleLogoutAndStay = async () => {
    try {
      const result = await signOut()
      if (result.success) {
        toast.success('Signed out successfully')
        setShowLogoutWarning(false)
        // User stays on landing page
      } else {
        toast.error(result.error || 'Failed to sign out')
      }
    } catch (error) {
      console.error('Logout error:', error)
      toast.error('An unexpected error occurred')
    }
  }

  const handleStayLoggedIn = () => {
    setShowLogoutWarning(false)
    // Redirect to dashboard
    router.push('/crawler')
  }

  const features = [
    {
      icon: <Search className="h-8 w-8" />,
      title: 'Smart Document Discovery',
      description: 'Automatically finds and extracts Terms of Service and Privacy Policy documents from any website with intelligent parsing and content extraction.'
    },
    {
      icon: <BarChart3 className="h-8 w-8" />,
      title: 'Advanced Analysis',
      description: 'Comprehensive text mining and natural language processing to understand document complexity, readability, and key legal points.'
    },
    {
      icon: <Shield className="h-8 w-8" />,
      title: 'Risk Assessment',
      description: 'Identify potential risks and legal implications with our comprehensive risk scoring system and legal clause identification.'
    },
    {
      icon: <Zap className="h-8 w-8" />,
      title: 'Real-time Processing',
      description: 'Get instant analysis results with our fast and efficient processing pipeline powered by modern cloud infrastructure.'
    }
  ]

  const benefits = [
    'Understand complex legal documents in plain English',
    'Identify potential risks and red flags',
    'Save time with automated analysis',
    'Make informed decisions with detailed insights',
    'Access comprehensive document summaries',
    'Track changes and updates over time'
  ]

  const howItWorks = [
    {
      step: '1',
      title: 'Enter URL',
      description: 'Simply paste the website URL you want to analyze',
      icon: <Target className="h-6 w-6" />
    },
    {
      step: '2',
      title: 'Automatic Discovery',
      description: 'Our system finds and extracts relevant documents',
      icon: <Search className="h-6 w-6" />
    },
    {
      step: '3',
      title: 'Get Results',
      description: 'Receive detailed analysis and insights instantly',
      icon: <BarChart3 className="h-6 w-6" />
    }
  ]

  const toggleCard = (index: number) => {
    const newFlippedCards = new Set(flippedCards)
    if (newFlippedCards.has(index)) {
      newFlippedCards.delete(index)
    } else {
      newFlippedCards.add(index)
    }
    setFlippedCards(newFlippedCards)
  }

  return (
    <Layout>
      {/* Starfield Background */}
      <div className="starfield"></div>
      
      {/* Hero Section */}
      <section className="relative min-h-screen flex items-center justify-center px-4 sm:px-6 lg:px-8">
        <div className="max-w-4xl mx-auto text-center">
          <div className="animate-fade-in">
            <h1 className="text-5xl sm:text-6xl lg:text-7xl font-bold text-white mb-6">
              Analyze Terms of Service & 
              <span className="block text-gray-300">
                Privacy Policies
              </span>
            </h1>
            <p className="text-2xl sm:text-3xl text-gray-300 mb-8 max-w-3xl mx-auto">
              Unlock the secrets hidden in legal documents with our powerful analysis tool. 
              Get instant insights, risk assessments, and plain-English summaries.
            </p>
            <div className="flex flex-col sm:flex-row gap-4 justify-center">
              <Link href="/crawler">
                <Button size="lg" className="text-lg px-8 py-4">
                  Start Analyzing
                  <ArrowRight className="ml-2 h-5 w-5" />
                </Button>
              </Link>
            </div>
          </div>
        </div>
      </section>

      {/* Features Section */}
      <section className="py-20 px-4 sm:px-6 lg:px-8">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-16">
            <h2 className="text-4xl sm:text-5xl font-bold text-white mb-4">
              Powerful Features
            </h2>
            <p className="text-2xl text-gray-300 max-w-2xl mx-auto">
              Click on any feature card to learn more about our capabilities
            </p>
          </div>
          
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8">
            {features.map((feature, index) => (
              <div
                key={index}
                className={`card-flip ${flippedCards.has(index) ? 'flipped' : ''}`}
                onClick={() => toggleCard(index)}
              >
                <div className="card-front">
                  <div className="text-center">
                    <div className="text-gray-300 mb-4 flex justify-center">
                      {feature.icon}
                    </div>
                    <h3 className="text-xl font-semibold text-white">
                      {feature.title}
                    </h3>
                  </div>
                </div>
                <div className="card-back">
                  <div className="text-center">
                    <div className="text-gray-300 mb-4 flex justify-center">
                      {feature.icon}
                    </div>
                    <p className="text-gray-300 text-sm leading-relaxed">
                      {feature.description}
                    </p>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* How It Works Section */}
      <section className="py-20 px-4 sm:px-6 lg:px-8 bg-gray-900/50">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-16">
            <h2 className="text-4xl sm:text-5xl font-bold text-white mb-4">
              How It Works
            </h2>
            <p className="text-2xl text-gray-300 max-w-2xl mx-auto">
              Simple, fast, and powerful document analysis in three easy steps
            </p>
          </div>
          
          <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
            {howItWorks.map((step, index) => (
              <div key={index} className="text-center group">
                <div className="relative mb-6">
                  <div className="w-16 h-16 bg-gradient-to-br from-gray-600 to-gray-800 rounded-full flex items-center justify-center mx-auto shadow-lg group-hover:shadow-gray-500/50 transition-all duration-300 border border-gray-500">
                    <span className="text-2xl font-bold text-white">{step.step}</span>
                  </div>
                  <div className="absolute -top-2 -right-2 w-8 h-8 bg-gray-700 rounded-full flex items-center justify-center border border-gray-500">
                    {step.icon}
                  </div>
                </div>
                <h3 className="text-xl font-semibold text-white mb-3">
                  {step.title}
                </h3>
                <p className="text-gray-300">
                  {step.description}
                </p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Benefits Section */}
      <section className="py-20 px-4 sm:px-6 lg:px-8">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-16">
            <h2 className="text-4xl sm:text-5xl font-bold text-white mb-4">
              Why Choose Our Tool
            </h2>
            <p className="text-2xl text-gray-300 max-w-2xl mx-auto">
              Experience the power of intelligent document analysis
            </p>
          </div>
          
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {benefits.map((benefit, index) => (
              <div key={index} className="flex items-start space-x-3 p-6 bg-gray-900/50 rounded-xl border border-gray-700 hover:border-gray-500/50 transition-all duration-300 group">
                <div className="flex-shrink-0 w-6 h-6 bg-gradient-to-br from-gray-600 to-gray-800 rounded-full flex items-center justify-center mt-1 border border-gray-500">
                  <CheckCircle className="h-4 w-4 text-white" />
                </div>
                <p className="text-gray-300 group-hover:text-white transition-colors">
                  {benefit}
                </p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* CTA Section */}
      <section className="py-20 px-4 sm:px-6 lg:px-8">
        <div className="max-w-4xl mx-auto text-center">
          <div className="bg-gradient-to-r from-gray-900/50 to-gray-800/50 rounded-2xl p-12 border border-gray-700">
            <h2 className="text-4xl sm:text-5xl font-bold text-white mb-6">
              Ready to Analyze Documents?
            </h2>
            <p className="text-2xl text-gray-300 mb-8 max-w-2xl mx-auto">
              Start analyzing Terms of Service and Privacy Policy documents today. 
              Get instant insights and make informed decisions.
            </p>
            <Link href="/crawler">
              <Button size="lg" className="text-lg px-8 py-4">
                Start Analyzing Now
                <ArrowRight className="ml-2 h-5 w-5" />
              </Button>
            </Link>
          </div>
        </div>
      </section>

      {/* Logout Warning Modal for Logged-in Users */}
      <Modal
        isOpen={showLogoutWarning}
        onClose={handleStayLoggedIn}
        size="md"
        className="bg-gray-900 border border-gray-700"
      >
        <div className="text-center py-6">
          <div className="w-16 h-16 bg-gray-800 rounded-full flex items-center justify-center mx-auto mb-4">
            <AlertCircle className="h-8 w-8 text-yellow-400" />
          </div>
          <h3 className="text-2xl font-bold text-white mb-2">
            You're Already Logged In
          </h3>
          <p className="text-gray-300 mb-6">
            The landing page is only available for non-authenticated users. Would you like to logout to view the landing page?
          </p>
          <div className="flex space-x-4">
            <Button
              onClick={handleStayLoggedIn}
              variant="outline"
              fullWidth
            >
              No, Go to Dashboard
            </Button>
            <Button
              onClick={handleLogoutAndStay}
              variant="danger"
              fullWidth
              leftIcon={<LogOut className="h-4 w-4" />}
            >
              Yes, Logout
            </Button>
          </div>
        </div>
      </Modal>
    </Layout>
  )
}

export default HomePage