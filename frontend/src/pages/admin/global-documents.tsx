import React, { useState, useEffect } from 'react'
import Layout from '@/components/layout/Layout'
import ProtectedRoute from '@/components/auth/ProtectedRoute'
import { Card, CardContent, CardHeader, CardTitle, Button, Input, Spinner } from '@/components/ui'
import { apiService } from '@/services'
import { useAuthStore } from '@/store/authStore'
import { GlobalDocument, GlobalDocumentSearchResponse } from '@/types'
import { Search, Trash2, AlertCircle, Shield, ExternalLink } from 'lucide-react'
import toast from 'react-hot-toast'
import { useRouter } from 'next/router'

const AdminGlobalDocumentsPage: React.FC = () => {
  const router = useRouter()
  const { user } = useAuthStore()
  const [documents, setDocuments] = useState<GlobalDocument[]>([])
  const [loading, setLoading] = useState(false)
  const [deleting, setDeleting] = useState<string | null>(null)
  const [searchQuery, setSearchQuery] = useState('')
  const [page, setPage] = useState(1)
  const [total, setTotal] = useState(0)
  const [limit] = useState(20)
  const [isAdmin, setIsAdmin] = useState(false)

  // Check if user is admin
  useEffect(() => {
    const checkAdmin = async () => {
      if (user?.is_admin) {
        setIsAdmin(true)
      } else {
        // Fetch user info to check admin status
        try {
          const response = await apiService.getCurrentUser()
          if (response.success && response.data) {
            const isUserAdmin = (response.data as any).is_admin || false
            setIsAdmin(isUserAdmin)
            if (!isUserAdmin) {
              toast.error('Admin access required')
              router.push('/crawler')
            }
          }
        } catch (error: any) {
          console.error('Error checking admin status:', error)
          if (error.response?.status === 403) {
            toast.error('Admin access required')
          }
          router.push('/crawler')
        }
      }
    }
    checkAdmin()
  }, [user, router])

  // Fetch documents
  const fetchDocuments = async () => {
    if (!isAdmin) return
    
    setLoading(true)
    try {
      const response = await apiService.searchGlobalDocuments({
        search: searchQuery || undefined,
        page,
        limit
      })
      
      if (response.success && response.data) {
        setDocuments(response.data.documents)
        setTotal(response.data.total)
      } else {
        toast.error('Failed to fetch documents')
      }
    } catch (error: any) {
      console.error('Error fetching documents:', error)
      if (error.response?.status === 403) {
        toast.error('Admin access required')
        router.push('/crawler')
      } else {
        toast.error('Failed to fetch documents')
      }
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    if (isAdmin) {
      fetchDocuments()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isAdmin, page])
  
  // Separate effect for search to avoid too many calls
  useEffect(() => {
    if (isAdmin && searchQuery !== '') {
      const timeoutId = setTimeout(() => {
        setPage(1)
        fetchDocuments()
      }, 500) // Debounce search
      return () => clearTimeout(timeoutId)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchQuery])

  // Handle search
  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault()
    setPage(1)
    fetchDocuments()
  }

  // Handle delete
  const handleDelete = async (documentId: string, documentUrl: string) => {
    if (!confirm(`Are you sure you want to delete this document?\n\n${documentUrl}\n\nThis will also delete the associated analysis.`)) {
      return
    }

    setDeleting(documentId)
    try {
      const response = await apiService.deleteGlobalDocument(documentId)
      
      if (response.success && response.data) {
        toast.success('Document deleted successfully')
        // Refresh list
        fetchDocuments()
      } else {
        toast.error(response.error?.message || 'Failed to delete document')
      }
    } catch (error: any) {
      console.error('Error deleting document:', error)
      if (error.response?.status === 403) {
        toast.error('Admin access required')
      } else {
        toast.error('Failed to delete document')
      }
    } finally {
      setDeleting(null)
    }
  }

  // Show loading while checking admin status or if not admin
  if (!user || !isAdmin) {
    return (
      <Layout>
        <div className="flex items-center justify-center min-h-screen">
          <Spinner />
        </div>
      </Layout>
    )
  }

  const totalPages = Math.ceil(total / limit)

  return (
    <Layout>
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="mb-8">
          <div className="flex items-center space-x-3 mb-2">
            <Shield className="h-8 w-8 text-blue-500" />
            <h1 className="text-3xl font-bold text-white">Admin Panel</h1>
          </div>
          <p className="text-gray-400">Manage global document cache</p>
        </div>

        <Card className="bg-gray-800/80 backdrop-blur-sm border-gray-700 mb-6">
          <CardContent className="p-6">
            <form onSubmit={handleSearch} className="flex space-x-4">
              <div className="flex-1">
                <Input
                  type="text"
                  placeholder="Search by base URL or document URL (e.g., netflix.com)"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  leftIcon={<Search className="h-4 w-4" />}
                  fullWidth
                />
              </div>
              <Button
                type="submit"
                variant="primary"
                loading={loading}
                leftIcon={<Search className="h-4 w-4" />}
              >
                Search
              </Button>
            </form>
          </CardContent>
        </Card>

        {loading && documents.length === 0 ? (
          <div className="flex items-center justify-center py-12">
            <Spinner />
          </div>
        ) : documents.length === 0 ? (
          <Card className="bg-gray-800/80 backdrop-blur-sm border-gray-700">
            <CardContent className="p-12 text-center">
              <AlertCircle className="h-12 w-12 text-gray-500 mx-auto mb-4" />
              <p className="text-gray-400 text-lg">No documents found</p>
              {searchQuery && (
                <p className="text-gray-500 text-sm mt-2">
                  Try a different search term
                </p>
              )}
            </CardContent>
          </Card>
        ) : (
          <>
            <Card className="bg-gray-800/80 backdrop-blur-sm border-gray-700 mb-4">
              <CardHeader>
                <CardTitle className="text-white">
                  Global Documents ({total} total)
                </CardTitle>
              </CardHeader>
              <CardContent className="p-0">
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead className="bg-gray-900/50 border-b border-gray-700">
                      <tr>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-300 uppercase tracking-wider">
                          Base URL
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-300 uppercase tracking-wider">
                          Document URL
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-300 uppercase tracking-wider">
                          Type
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-300 uppercase tracking-wider">
                          Title
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-300 uppercase tracking-wider">
                          Word Count
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-300 uppercase tracking-wider">
                          Has Analysis
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-300 uppercase tracking-wider">
                          Last Crawled
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-300 uppercase tracking-wider">
                          Actions
                        </th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-700">
                      {documents.map((doc) => (
                        <tr key={doc.id} className="hover:bg-gray-900/50 transition-colors">
                          <td className="px-6 py-4 whitespace-nowrap">
                            <div className="text-sm text-gray-300">{doc.base_url}</div>
                          </td>
                          <td className="px-6 py-4">
                            <div className="text-sm text-blue-400 max-w-md truncate" title={doc.document_url}>
                              {doc.document_url}
                            </div>
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap">
                            <span className="px-2 py-1 text-xs font-medium rounded-full bg-blue-500/20 text-blue-400">
                              {doc.document_type}
                            </span>
                          </td>
                          <td className="px-6 py-4">
                            <div className="text-sm text-gray-300 max-w-xs truncate" title={doc.title}>
                              {doc.title || 'N/A'}
                            </div>
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap">
                            <div className="text-sm text-gray-300">
                              {doc.word_count?.toLocaleString() || 'N/A'}
                            </div>
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap">
                            {doc.has_analysis ? (
                              <span className="px-2 py-1 text-xs font-medium rounded-full bg-green-500/20 text-green-400">
                                Yes
                              </span>
                            ) : (
                              <span className="px-2 py-1 text-xs font-medium rounded-full bg-gray-500/20 text-gray-400">
                                No
                              </span>
                            )}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap">
                            <div className="text-sm text-gray-400">
                              {new Date(doc.last_crawled).toLocaleDateString()}
                            </div>
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap">
                            <Button
                              variant="danger"
                              size="sm"
                              onClick={() => handleDelete(doc.id, doc.document_url)}
                              loading={deleting === doc.id}
                              leftIcon={<Trash2 className="h-4 w-4" />}
                            >
                              Delete
                            </Button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>

            {/* Pagination */}
            {totalPages > 1 && (
              <div className="flex items-center justify-between">
                <div className="text-sm text-gray-400">
                  Page {page} of {totalPages} ({total} total documents)
                </div>
                <div className="flex space-x-2">
                  <Button
                    variant="secondary"
                    onClick={() => setPage(p => Math.max(1, p - 1))}
                    disabled={page === 1 || loading}
                  >
                    Previous
                  </Button>
                  <Button
                    variant="secondary"
                    onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                    disabled={page === totalPages || loading}
                  >
                    Next
                  </Button>
                </div>
              </div>
            )}
          </>
        )}
      </div>
    </Layout>
  )
}

const AdminGlobalDocumentsPageWithAuth = () => {
  return (
    <ProtectedRoute>
      <AdminGlobalDocumentsPage />
    </ProtectedRoute>
  )
}

export default AdminGlobalDocumentsPageWithAuth

