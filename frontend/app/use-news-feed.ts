"use client"

import { useEffect, useRef, useState } from "react"

import { fetchNews, fetchTickers } from "@/lib/api"
import { NewsItem, TickerItem, Watchlist } from "@/lib/types"

import { buildFetchParams, dedupeById } from "./page-helpers"

const AUTO_REFRESH_MS = 30_000
const AUTO_REFRESH_DEDUPE_MS = 2_000

export function useNewsFeed({
  activeWatchlist,
  activeWatchlistId,
  provider,
  searchQuery,
  ticker,
}: {
  activeWatchlist?: Watchlist
  activeWatchlistId: string
  provider: string
  searchQuery: string
  ticker: string
}) {
  const [tickers, setTickers] = useState<TickerItem[]>([])
  const [globalTrackedIds, setGlobalTrackedIds] = useState<number[]>([])
  const [totalCount, setTotalCount] = useState(0)
  const [items, setItems] = useState<NewsItem[]>([])
  const [nextCursor, setNextCursor] = useState<string | null>(null)
  const [pendingNewItems, setPendingNewItems] = useState<NewsItem[]>([])
  const [loading, setLoading] = useState(false)
  const [loadingMore, setLoadingMore] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [refreshTick, setRefreshTick] = useState(0)
  const [runtimeReady, setRuntimeReady] = useState(false)

  const feedGenerationRef = useRef(0)
  const feedContainerRef = useRef<HTMLElement | null>(null)
  const refreshReasonRef = useRef<"auto" | "manual">("manual")
  const itemsRef = useRef<NewsItem[]>([])
  const pendingNewItemsRef = useRef<NewsItem[]>([])
  const lastQueryKeyRef = useRef("")
  const lastAutoRefreshAtRef = useRef(0)

  useEffect(() => {
    itemsRef.current = items
  }, [items])

  useEffect(() => {
    pendingNewItemsRef.current = pendingNewItems
  }, [pendingNewItems])

  const buildCurrentNewsRequest = (
    overrides: Partial<Parameters<typeof fetchNews>[0]> = {}
  ): Parameters<typeof fetchNews>[0] => {
    const { fetchTickers, includeGeneralFromProvider } = buildFetchParams(
      activeWatchlist,
      activeWatchlistId,
      ticker
    )

    return {
      tickers: fetchTickers,
      provider: provider || undefined,
      includeUnmappedFromProvider: includeGeneralFromProvider,
      q: searchQuery || undefined,
      ...overrides,
    }
  }

  const triggerRefresh = (reason: "auto" | "manual") => {
    refreshReasonRef.current = reason
    setRefreshTick((previous) => previous + 1)
  }

  useEffect(() => {
    const controller = new AbortController()
    fetchTickers(controller.signal)
      .then((data) => setTickers(data.items))
      .catch(() => setTickers([]))

    return () => controller.abort()
  }, [])

  useEffect(() => {
    // Defer past synchronous effects so localStorage hydration completes first
    const id = setTimeout(() => setRuntimeReady(true), 0)
    return () => clearTimeout(id)
  }, [])

  useEffect(() => {
    if (!runtimeReady) return

    let timer: ReturnType<typeof setInterval> | null = null

    const refreshNow = () => {
      const now = Date.now()
      if (now - lastAutoRefreshAtRef.current < AUTO_REFRESH_DEDUPE_MS) {
        return
      }
      lastAutoRefreshAtRef.current = now
      triggerRefresh("auto")
    }

    const startTimer = () => {
      if (timer !== null) return
      timer = setInterval(refreshNow, AUTO_REFRESH_MS)
    }

    const stopTimer = () => {
      if (timer === null) return
      clearInterval(timer)
      timer = null
    }

    const onVisibilityChange = () => {
      if (document.visibilityState === "visible") {
        refreshNow()
        startTimer()
      } else {
        stopTimer()
      }
    }

    const onWindowFocus = () => {
      refreshNow()
    }

    if (document.visibilityState === "visible") {
      startTimer()
    }

    document.addEventListener("visibilitychange", onVisibilityChange)
    window.addEventListener("focus", onWindowFocus)

    return () => {
      stopTimer()
      document.removeEventListener("visibilitychange", onVisibilityChange)
      window.removeEventListener("focus", onWindowFocus)
    }
  }, [runtimeReady])

  useEffect(() => {
    if (!runtimeReady) return
    if (!("serviceWorker" in navigator)) return

    const onMessage = (event: MessageEvent) => {
      const payload = event.data
      if (!payload || typeof payload !== "object") return

      if (payload.type === "push-notification-click") {
        triggerRefresh("manual")
        return
      }

      if (payload.type === "push-notification") {
        const feedEl = feedContainerRef.current
        const scrolledDeep = !!feedEl && feedEl.scrollTop > 160 && itemsRef.current.length > 40
        triggerRefresh(scrolledDeep ? "auto" : "manual")
      }
    }

    navigator.serviceWorker.addEventListener("message", onMessage)
    return () => {
      navigator.serviceWorker.removeEventListener("message", onMessage)
    }
  }, [runtimeReady])

  useEffect(() => {
    const queryKey = JSON.stringify({
      watchlistId: activeWatchlistId,
      watchlistTickers: [...(activeWatchlist?.tickers || [])].sort(),
      watchlistProvider: activeWatchlist?.provider || "",
      watchlistQuery: activeWatchlist?.q || "",
      ticker,
      provider,
      searchQuery,
    })
    const sameQueryAsPrevious = lastQueryKeyRef.current === queryKey
    const refreshReason = refreshReasonRef.current
    refreshReasonRef.current = "manual"

    const feedEl = feedContainerRef.current
    const isReadingDeep = !!feedEl && feedEl.scrollTop > 160 && itemsRef.current.length > 40
    const isAutoRefresh = sameQueryAsPrevious && refreshReason === "auto"

    if (isAutoRefresh && isReadingDeep) {
      const bgGeneration = feedGenerationRef.current
      const controller = new AbortController()

      fetchNews(buildCurrentNewsRequest({
        includeGlobalSummary: true,
        limit: 40,
        signal: controller.signal,
      }))
        .then((data) => {
          if (bgGeneration !== feedGenerationRef.current) return
          if (data.global_summary) {
            setGlobalTrackedIds(data.global_summary.tracked_ids)
            setTotalCount(data.global_summary.total)
          }
          const fetchedItems = data.items
          const newOnes = dedupeById(itemsRef.current, fetchedItems)
          const freshOnes = dedupeById(pendingNewItemsRef.current, newOnes)
          if (freshOnes.length > 0) {
            setPendingNewItems((previous) => {
              const uniqueFresh = dedupeById(previous, freshOnes)
              return uniqueFresh.length > 0 ? [...uniqueFresh, ...previous] : previous
            })
          }
        })
        .catch(() => {})

      return () => controller.abort()
    }

    if (!sameQueryAsPrevious) {
      setPendingNewItems([])
    }

    lastQueryKeyRef.current = queryKey
    const controller = new AbortController()
    const showLoading = !isAutoRefresh
    if (showLoading) {
      setLoading(true)
    }
    setError(null)

    const requestGeneration = ++feedGenerationRef.current

    fetchNews(buildCurrentNewsRequest({
      includeGlobalSummary: true,
      limit: 40,
      signal: controller.signal,
    }))
      .then((data) => {
        if (requestGeneration !== feedGenerationRef.current) return
        if (data.global_summary) {
          setGlobalTrackedIds(data.global_summary.tracked_ids)
          setTotalCount(data.global_summary.total)
        }
        const fetchedItems = data.items
        setPendingNewItems([])
        if (isAutoRefresh && itemsRef.current.length > data.items.length) {
          setItems((previous) => {
            const prepend = dedupeById(previous, fetchedItems)
            return prepend.length > 0 ? [...prepend, ...previous] : previous
          })
          setNextCursor((previous) => previous ?? data.next_cursor)
          return
        }
        setItems(fetchedItems)
        setNextCursor(data.next_cursor)
      })
      .catch((err: unknown) => {
        if (requestGeneration !== feedGenerationRef.current) return
        if (err instanceof DOMException && err.name === "AbortError") return
        const message = err instanceof Error ? err.message : "Failed to load feed"
        setError(message)
      })
      .finally(() => {
        if (showLoading && requestGeneration === feedGenerationRef.current) {
          setLoading(false)
        }
      })

    return () => controller.abort()
  }, [
    activeWatchlist,
    activeWatchlistId,
    provider,
    refreshTick,
    searchQuery,
    ticker,
  ])

  const loadMore = async () => {
    if (!nextCursor || loadingMore) return

    setLoadingMore(true)
    setError(null)
    const requestGeneration = feedGenerationRef.current

    try {
      const data = await fetchNews(buildCurrentNewsRequest({
        limit: 40,
        cursor: nextCursor,
      }))
      if (requestGeneration !== feedGenerationRef.current) return
      const fetchedItems = data.items
      setItems((previous) => {
        const append = dedupeById(previous, fetchedItems)
        return append.length > 0 ? [...previous, ...append] : previous
      })
      setNextCursor(data.next_cursor)
    } catch (err: unknown) {
      if (requestGeneration !== feedGenerationRef.current) return
      const message = err instanceof Error ? err.message : "Failed to load more"
      setError(message)
    } finally {
      setLoadingMore(false)
    }
  }

  const loadPendingArticles = () => {
    setItems((previous) => {
      const newOnes = dedupeById(previous, pendingNewItems)
      return newOnes.length > 0 ? [...newOnes, ...previous] : previous
    })
    setPendingNewItems([])
    feedContainerRef.current?.scrollTo({ top: 0, behavior: "smooth" })
  }

  return {
    error,
    feedContainerRef,
    globalTrackedIds,
    items,
    loadMore,
    loadPendingArticles,
    loading,
    loadingMore,
    nextCursor,
    pendingNewItems,
    tickers,
    totalCount,
    triggerRefresh,
  }
}
