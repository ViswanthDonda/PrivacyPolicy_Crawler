import React from 'react'
import Link from 'next/link'
import { 
  Github, 
  Twitter, 
  Linkedin, 
  Mail,
  ExternalLink
} from 'lucide-react'

const Footer: React.FC = () => {
  const currentYear = new Date().getFullYear()

  const footerLinks = {
    product: [
      { name: 'Features', href: '/features' },
      { name: 'Documentation', href: '/docs' }
    ],
    // Removed legal section per user request
    resources: [
      { name: 'Help Center', href: '/help' },
      { name: 'Contact', href: '/contact' }
    ] 
  }

  const socialLinks = [
    {
      name: 'GitHub',
      href: 'https://github.com/ViswanthDonda/PrivacyPolicy_Crawler',
      icon: <Github className="h-5 w-5" />
    },
    
    /* {
      name: 'Email',
      href: '#',
      icon: <Mail className="h-5 w-5" />
    } */
  ]

  return (
    <footer className="bg-gray-900/80 backdrop-blur-sm border-t border-gray-700">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
          {/* Brand */}
          <div className="lg:col-span-1">
            <div className="flex items-center space-x-2 mb-4">
              <div className="w-8 h-8 bg-gradient-to-br from-gray-600 to-gray-800 rounded-lg flex items-center justify-center border border-gray-500">
                <span className="text-white font-bold text-sm">T</span>
              </div>
              <span className="text-xl font-bold text-white">
                TOS Crawler
              </span>
            </div>
            <p className="text-sm text-gray-300 mb-4">
              Analyze Terms of Service and Privacy Policy documents with ease. 
              Get instant insights and understand legal documents.
            </p>
            <div className="flex space-x-4">
              {socialLinks.map((link) => (
                <a
                  key={link.name}
                  href={link.href}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-gray-400 hover:text-white transition-colors"
                  aria-label={link.name}
                >
                  {link.icon}
                </a>
              ))}
            </div>
          </div>

         {/* Product Links */}
          <div>
            <h3 className="text-sm font-semibold text-white uppercase tracking-wider mb-4">
              Product
            </h3>
            <ul className="space-y-3">
              {footerLinks.product.map((link) => (
                <li key={link.name}>
                  <Link
                    href={link.href}
                    className="text-sm text-gray-300 hover:text-white transition-colors"
                  >
                    {link.name}
                  </Link>
                </li>
              ))}
            </ul> 
          </div>

          {/* Resources Links */}
          <div>
            <h3 className="text-sm font-semibold text-white uppercase tracking-wider mb-4">
              Resources
            </h3>
            <ul className="space-y-3">
              {footerLinks.resources.map((link) => (
                <li key={link.name}>
                  <Link
                    href={link.href}
                    className="text-sm text-gray-300 hover:text-white transition-colors"
                  >
                    {link.name}
                  </Link>
                </li>
              ))}
            </ul>
          </div>
        </div>

        {/* Bottom Section */}
        <div className="mt-12 pt-8 border-t border-gray-700">
          <div className="flex flex-col md:flex-row justify-between items-center">
            <div className="text-sm text-gray-300">
              © {currentYear} TOS Crawler. All rights reserved.
            </div>
            
            <div className="mt-4 md:mt-0 flex items-center space-x-6">
              <a
                href="https://github.com"
                target="_blank"
                rel="noopener noreferrer"
                className="text-sm text-gray-300 hover:text-white transition-colors flex items-center space-x-1"
              >
                <span>Open Source</span>
                <ExternalLink className="h-3 w-3" />
              </a>
            </div>
          </div>
        </div>
      </div>
    </footer>
  )
}

export default Footer