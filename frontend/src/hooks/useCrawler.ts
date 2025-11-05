import { useEffect } from 'react'
import { useCrawler as useCrawlerStore, useCrawlerActions } from '@/store/crawlerStore'

export const useCrawler = () => {
  const crawlerState = useCrawlerStore()
  const crawlerActions = useCrawlerActions()

  // Fetch user's crawl history when component mounts
  useEffect(() => {
    crawlerActions.getCrawlHistory()
  }, [crawlerActions])

  return {
    ...crawlerState,
    ...crawlerActions
  }
}

export default useCrawler
