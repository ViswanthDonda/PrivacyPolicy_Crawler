import React from 'react'
import Head from 'next/head'
import Header from './Header'
import Footer from './Footer'
import { useTheme } from '@/store/uiStore'

interface LayoutProps {
  children: React.ReactNode
  title?: string
  description?: string
  className?: string
}

const Layout: React.FC<LayoutProps> = ({
  children,
  title = 'TOS Analyzer - Terms of Service & Privacy Policy Analysis',
  description = 'Analyze and understand Terms of Service and Privacy Policy documents with AI-powered text mining and legal analysis.',
  className = ''
}) => {
  const { theme } = useTheme()

  return (
    <>
      <Head>
        <title>{title}</title>
        <meta name="description" content={description} />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
        
        {/* Open Graph / Facebook */}
        <meta property="og:type" content="website" />
        <meta property="og:title" content={title} />
        <meta property="og:description" content={description} />
        <meta property="og:site_name" content="TOS Analyzer" />
        
        {/* Twitter */}
        <meta property="twitter:card" content="summary_large_image" />
        <meta property="twitter:title" content={title} />
        <meta property="twitter:description" content={description} />
        
        {/* Theme */}
        <meta name="theme-color" content="#0a0a0a" />
        <meta name="color-scheme" content="dark" />
        <script
          dangerouslySetInnerHTML={{
            __html: `
              document.documentElement.classList.add('dark');
            `,
          }}
        />
      </Head>
      
      <div className={`min-h-screen relative ${className}`}>
        <Header />
        
        <main className="flex-1 relative z-10">
          {children}
        </main>
        
        <Footer />
      </div>
    </>
  )
}

export default Layout