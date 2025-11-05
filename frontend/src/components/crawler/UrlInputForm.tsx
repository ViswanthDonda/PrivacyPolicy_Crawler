import React, { useState } from 'react'
import { useForm } from 'react-hook-form'
import { Button, Input, Card, CardContent } from '@/components/ui'
import { useCrawler, useCrawlerActions, useDocumentTypes } from '@/store/crawlerStore'
import { CrawlRequest } from '@/types'
import { Globe, Search, AlertCircle } from 'lucide-react'
import toast from 'react-hot-toast'

const UrlInputForm: React.FC = () => {
  const [selectedTypes, setSelectedTypes] = useState<string[]>(['tos', 'privacy'])
  const { isLoading, error } = useCrawler()
  const { startCrawl } = useCrawlerActions()
  const documentTypes = useDocumentTypes()
  
  const {
    register,
    handleSubmit,
    formState: { errors },
    setError: setFormError,
    watch
  } = useForm<{ url: string }>()

  const url = watch('url')

  // Handle form submission to start crawl analysis
  const onSubmit = async (data: { url: string }) => {
    if (selectedTypes.length === 0) {
      setFormError('root', {
        type: 'manual',
        message: 'Please select at least one document type'
      })
      return
    }

    const request: CrawlRequest = {
      url: data.url,
      document_types: selectedTypes as ('tos' | 'privacy')[],
      force_refresh: false
    }

    const success = await startCrawl(request)
    
    if (success) {
      toast.success('Analysis started! Report is being saved...', {
        duration: 3000,
        position: 'top-center',
        icon: '📊',
      })
    } else {
      toast.error('Failed to start analysis. Please try again.', {
        duration: 3000,
        position: 'top-center',
      })
      setFormError('root', {
        type: 'manual',
        message: 'Failed to start crawl. Please try again.'
      })
    }
  }

  // Toggle document type selection
  const handleTypeToggle = (type: string) => {
    setSelectedTypes(prev => 
      prev.includes(type) 
        ? prev.filter(t => t !== type)
        : [...prev, type]
    )
  }

  // Validate URL format
  const isValidUrl = (url: string) => {
    try {
      new URL(url)
      return true
    } catch {
      return false
    }
  }

  return (
    <Card className="w-full max-w-2xl mx-auto bg-gray-800/80 backdrop-blur-sm border-gray-700">
      <CardContent className="p-8">

        <form onSubmit={handleSubmit(onSubmit)} className="space-y-6">
          <div>
            <Input
              {...register('url', {
                required: 'URL is required',
                validate: (value) => 
                  isValidUrl(value) || 'Please enter a valid URL'
              })}
              type="url"
              label="Website URL"
              placeholder="https://example.com"
              error={errors.url?.message}
              leftIcon={<Globe className="h-4 w-4" />}
              fullWidth
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-300 mb-3">
              Document Types to Analyze
            </label>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              {documentTypes.map((type) => (
                <div
                  key={type.value}
                  className={`relative flex items-start p-4 border rounded-lg cursor-pointer transition-colors ${
                    selectedTypes.includes(type.value)
                      ? 'border-blue-500 bg-blue-500/20'
                      : 'border-gray-600 hover:border-gray-500 bg-gray-900/50'
                  }`}
                  onClick={() => handleTypeToggle(type.value)}
                >
                  <div className="flex items-center h-5">
                    <input
                      type="checkbox"
                      checked={selectedTypes.includes(type.value)}
                      onChange={() => handleTypeToggle(type.value)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-600 rounded bg-gray-800"
                    />
                  </div>
                  <div className="ml-3">
                    <label className={`text-sm font-medium ${selectedTypes.includes(type.value) ? 'text-white' : 'text-gray-300'}`}>
                      {type.label}
                    </label>
                    <p className={`text-xs ${selectedTypes.includes(type.value) ? 'text-gray-300' : 'text-gray-400'}`}>
                      {type.description}
                    </p>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {errors.root && (
            <div className="flex items-center text-sm text-red-400">
              <AlertCircle className="h-4 w-4 mr-2" />
              {errors.root.message}
            </div>
          )}

          {error && (
            <div className="flex items-center text-sm text-red-400">
              <AlertCircle className="h-4 w-4 mr-2" />
              {error}
            </div>
          )}

          <div className="flex items-center justify-end pt-4">
            <Button
              type="submit"
              variant="primary"
              size="lg"
              loading={isLoading}
              disabled={!url || selectedTypes.length === 0}
              leftIcon={<Search className="h-4 w-4" />}
            >
              Start Analysis
            </Button>
          </div>
          
          <div className="text-xs text-gray-400 space-y-1 mt-4">
            <p>• Analysis typically takes 30-60 seconds</p>
            <p>• We'll search for TOS and Privacy Policy links</p>
            <p>• Results will be saved to your account</p>
          </div>
        </form>
      </CardContent>
    </Card>
  )
}

export default UrlInputForm