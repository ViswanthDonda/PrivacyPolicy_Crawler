import jsPDF from 'jspdf'
import autoTable from 'jspdf-autotable'
import { AnalysisResult, Document } from '@/types'

interface PDFData {
  analysis: AnalysisResult
  document: Document
  sessionUrl?: string
}

export const generatePDFReport = (data: PDFData): void => {
  const { analysis, document, sessionUrl } = data
  const doc = new jsPDF()
  
  // Colors
  const primaryColor = [41, 128, 185] // Blue
  const successColor = [46, 204, 113] // Green
  const warningColor = [241, 196, 15] // Yellow
  const dangerColor = [231, 76, 60] // Red
  
  // Helper function to get risk color
  const getRiskColor = (score: number): number[] => {
    if (score >= 7) return dangerColor
    if (score >= 4) return warningColor
    return successColor
  }
  
  let yPosition = 20
  
  // Header
  doc.setFillColor(...primaryColor)
  doc.rect(0, 0, 210, 40, 'F')
  doc.setTextColor(255, 255, 255)
  doc.setFontSize(20)
  doc.setFont('helvetica', 'bold')
  doc.text('Document Analysis Report', 105, 20, { align: 'center' })
  
  doc.setFontSize(12)
  doc.setFont('helvetica', 'normal')
  doc.text('TOS Privacy Policy Crawler', 105, 30, { align: 'center' })
  
  yPosition = 50
  doc.setTextColor(0, 0, 0)
  
  // Document Information
  doc.setFontSize(16)
  doc.setFont('helvetica', 'bold')
  doc.text('Document Information', 14, yPosition)
  yPosition += 10
  
  doc.setFontSize(10)
  doc.setFont('helvetica', 'normal')
  
  // Handle document type - check both possible field names
  const docType = document.document_type === 'tos' 
    ? 'Terms of Service' 
    : document.document_type === 'privacy' || document.document_type === 'privacy_policy'
    ? 'Privacy Policy'
    : document.document_type || 'Unknown'
  
  // Handle title - check multiple possible fields
  const docTitle = document.title || 'N/A'
  
  // Handle URL
  const docUrl = document.url || sessionUrl || 'N/A'
  
  // Handle word count - check multiple possible fields
  const wordCount = document.word_count || analysis.measurements?.word_count || 0
  
  const docInfo = [
    ['Document Type:', docType],
    ['Title:', docTitle],
    ['URL:', docUrl],
    ['Word Count:', wordCount.toLocaleString()],
    ['Analysis Date:', new Date(analysis.created_at).toLocaleString()]
  ]
  
  docInfo.forEach(([label, value]) => {
    doc.setFont('helvetica', 'bold')
    doc.text(label, 14, yPosition)
    doc.setFont('helvetica', 'normal')
    const textWidth = doc.getTextWidth(value)
    if (textWidth > 150) {
      const lines = doc.splitTextToSize(value, 150)
      doc.text(lines[0], 70, yPosition)
      if (lines.length > 1) {
        yPosition += 5
        doc.text(lines.slice(1).join(' '), 70, yPosition)
      }
    } else {
      doc.text(value, 70, yPosition)
    }
    yPosition += 7
  })
  
  yPosition += 5
  
  // Summary Section
  doc.setFontSize(16)
  doc.setFont('helvetica', 'bold')
  doc.text('Summary', 14, yPosition)
  yPosition += 10
  
  // Handle summary fields - check both possible field names
  const summary100 = analysis.summary_100 || analysis.summary_100_words || ''
  const summarySentence = analysis.summary_sentence || analysis.summary_one_sentence || ''
  
  if (summary100) {
    doc.setFontSize(11)
    doc.setFont('helvetica', 'bold')
    doc.text('100-Word Summary:', 14, yPosition)
    yPosition += 7
    
    doc.setFontSize(10)
    doc.setFont('helvetica', 'normal')
    const summaryLines = doc.splitTextToSize(summary100, 180)
    doc.text(summaryLines, 14, yPosition)
    yPosition += summaryLines.length * 5 + 5
  }
  
  if (summarySentence) {
    doc.setFontSize(11)
    doc.setFont('helvetica', 'bold')
    doc.text('One-Sentence Summary:', 14, yPosition)
    yPosition += 7
    
    doc.setFontSize(10)
    doc.setFont('helvetica', 'normal')
    const sentenceLines = doc.splitTextToSize(summarySentence, 180)
    doc.text(sentenceLines, 14, yPosition)
    yPosition += sentenceLines.length * 5 + 10
  }
  
  // Check if we need a new page
  if (yPosition > 250) {
    doc.addPage()
    yPosition = 20
  }
  
  // Text Mining Measurements
  if (analysis.measurements) {
    doc.setFontSize(16)
    doc.setFont('helvetica', 'bold')
    doc.text('Text Mining Measurements', 14, yPosition)
    yPosition += 10
    
    const measurements = analysis.measurements
    const measurementsData = [
      ['Word Count', (measurements.word_count || 0).toLocaleString()],
      ['Sentence Count', (measurements.sentence_count || 0).toLocaleString()],
      ['Avg Words/Sentence', (measurements.avg_words_per_sentence || 0).toFixed(2)],
      ['Flesch Reading Ease', (measurements.flesch_reading_ease || 0).toFixed(2)],
      ['Flesch-Kincaid Grade', (measurements.flesch_kincaid_grade || 0).toFixed(2)],
      ['Automated Readability', (measurements.automated_readability_index || 0).toFixed(2)],
      ['Sentiment Score', (measurements.sentiment_score || 0).toFixed(2)],
      ['Keyword Density', `${(measurements.keyword_density || 0).toFixed(2)}%`],
      ['Legal Clauses', (measurements.legal_clause_count || 0).toString()],
      ['Risk Indicator Score', (measurements.risk_indicator_score || 0).toFixed(2)]
    ]
    
    autoTable(doc, {
      startY: yPosition,
      head: [['Metric', 'Value']],
      body: measurementsData,
      theme: 'striped',
      headStyles: { fillColor: primaryColor, textColor: 255 },
      styles: { fontSize: 9 },
      margin: { left: 14, right: 14 }
    })
    
    yPosition = (doc as any).lastAutoTable.finalY + 15
  }
  
  // Word Frequency
  if (analysis.word_frequency && Object.keys(analysis.word_frequency).length > 0) {
    // Check if we need a new page
    if (yPosition > 250) {
      doc.addPage()
      yPosition = 20
    }
    
    doc.setFontSize(16)
    doc.setFont('helvetica', 'bold')
    doc.text('Top Word Frequency', 14, yPosition)
    yPosition += 10
    
    // Get top 20 words
    const wordFreqEntries = Object.entries(analysis.word_frequency)
      .sort(([, a], [, b]) => (b as number) - (a as number))
      .slice(0, 20)
      .map(([word, count]) => [word, count.toString()])
    
    autoTable(doc, {
      startY: yPosition,
      head: [['Word', 'Frequency']],
      body: wordFreqEntries,
      theme: 'striped',
      headStyles: { fillColor: primaryColor, textColor: 255 },
      styles: { fontSize: 9 },
      margin: { left: 14, right: 14 }
    })
    
    yPosition = (doc as any).lastAutoTable.finalY + 15
  }
  
  // Risk Assessment
  if (analysis.measurements?.risk_indicator_score !== undefined) {
    if (yPosition > 250) {
      doc.addPage()
      yPosition = 20
    }
    
    doc.setFontSize(16)
    doc.setFont('helvetica', 'bold')
    doc.text('Risk Assessment', 14, yPosition)
    yPosition += 10
    
    const riskScore = analysis.measurements.risk_indicator_score
    const riskLevel = riskScore >= 7 ? 'High' : riskScore >= 4 ? 'Medium' : 'Low'
    const riskColor = getRiskColor(riskScore)
    
    doc.setFontSize(11)
    doc.setFont('helvetica', 'normal')
    doc.text(`Risk Level: ${riskLevel}`, 14, yPosition)
    yPosition += 7
    
    doc.text(`Risk Score: ${riskScore.toFixed(2)}/10`, 14, yPosition)
    yPosition += 7
    
    // Risk indicator bar
    doc.setFillColor(...riskColor)
    const barWidth = (riskScore / 10) * 180
    doc.rect(14, yPosition, barWidth, 8, 'F')
    doc.setDrawColor(200, 200, 200)
    doc.rect(14, yPosition, 180, 8, 'S')
    yPosition += 15
  }
  
  // Footer
  const pageCount = doc.getNumberOfPages()
  for (let i = 1; i <= pageCount; i++) {
    doc.setPage(i)
    doc.setFontSize(8)
    doc.setTextColor(128, 128, 128)
    doc.text(
      `Page ${i} of ${pageCount} • Generated on ${new Date().toLocaleString()}`,
      105,
      290,
      { align: 'center' }
    )
  }
  
  // Generate filename
  const docTypeSlug = document.document_type === 'tos' ? 'tos' : 'privacy'
  const filename = `analysis-report-${docTypeSlug}-${new Date().toISOString().split('T')[0]}.pdf`
  
  // Save PDF
  doc.save(filename)
}

