import React from 'react'
import Layout from '@/components/layout/Layout'
import ProtectedRoute from '@/components/auth/ProtectedRoute'
import { Card, CardContent } from '@/components/ui'
import { Bookmark, ExternalLink, Trash2, Search } from 'lucide-react'
import Link from 'next/link'

const BookmarksPage: React.FC = () => {
  // Bookmarks functionality will be implemented when favorites API is available
  const bookmarks: any[] = []

  return (
    <ProtectedRoute>
      <Layout title="Bookmarks - TOS Analyzer">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          {/* Header */}
          <div className="mb-8">
            <div className="flex items-center space-x-3 mb-4">
              <div className="w-12 h-12 bg-gray-800 rounded-lg flex items-center justify-center">
                <Bookmark className="h-6 w-6 text-gray-300" />
              </div>
              <div>
                <h1 className="text-3xl font-bold text-white">My Bookmarks</h1>
                <p className="text-gray-300 mt-1">
                  Saved documents and analyses for quick access
                </p>
              </div>
            </div>
          </div>

          {/* Bookmarks List */}
          {bookmarks.length > 0 ? (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {bookmarks.map((bookmark) => (
                <Card key={bookmark.id} className="hover-lift">
                  <CardContent className="p-6">
                    <div className="flex items-start justify-between mb-4">
                      <div className="flex-1">
                        <h3 className="text-lg font-semibold text-white mb-2 truncate">
                          {bookmark.title || bookmark.url}
                        </h3>
                        <p className="text-sm text-gray-400 mb-3 line-clamp-2">
                          {bookmark.summary || bookmark.description}
                        </p>
                        <div className="flex items-center space-x-4 text-xs text-gray-500">
                          <span>{new Date(bookmark.created_at || bookmark.favorited_at).toLocaleDateString()}</span>
                          {bookmark.document_type && (
                            <span className="px-2 py-1 bg-gray-800 rounded text-gray-300">
                              {bookmark.document_type}
                            </span>
                          )}
                        </div>
                      </div>
                      <button className="p-2 text-gray-400 hover:text-red-400 transition-colors ml-2">
                        <Trash2 className="h-4 w-4" />
                      </button>
                    </div>
                    
                    <div className="flex space-x-2 pt-4 border-t border-gray-700">
                      <Link
                        href={`/crawler/results/${bookmark.session_id || bookmark.id}`}
                        className="flex-1 btn-outline py-2 text-center text-sm"
                      >
                        View Analysis
                      </Link>
                      {bookmark.url && (
                        <a
                          href={bookmark.url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="p-2 text-gray-400 hover:text-white transition-colors"
                        >
                          <ExternalLink className="h-4 w-4" />
                        </a>
                      )}
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          ) : (
            <Card>
              <CardContent className="p-12 text-center">
                <div className="w-16 h-16 bg-gray-800 rounded-full flex items-center justify-center mx-auto mb-4">
                  <Search className="h-8 w-8 text-gray-400" />
                </div>
                <h3 className="text-xl font-semibold text-white mb-2">No Bookmarks Yet</h3>
                <p className="text-gray-400 mb-6">
                  Start analyzing documents and bookmark your favorites for quick access
                </p>
                <Link href="/crawler">
                  <button className="btn-primary px-6 py-3">
                    Start Analyzing
                  </button>
                </Link>
              </CardContent>
            </Card>
          )}
        </div>
      </Layout>
    </ProtectedRoute>
  )
}

export default BookmarksPage

