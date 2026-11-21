"use client"

import { useCallback, useEffect, useRef, useState } from "react"

import { buildNewsRequestParams, fetchNews, fetchTickers } from "@/lib/api"
import { buildApiUrl } from "@/lib/api-base"
import { NewsItem, NewsResponse, TickerItem, Watchlist } from "@/lib/types"
import { useEventSource } from "@/hooks/useEventSource"
import { deliveryCheck } from "@/lib/delivery-check"

import { buildNewsFilters, dedupeById, mergeUniqueNewsItems } from "./page-helpers"

const AUTO_REFRESH_POLL_MS = 30_000
const AUTO_REFRESH_SSE_MS = 120_000
const AUTO_REFRESH_DEDUPE_MS = 2_000
const NEWS_REQUEST_TIMEOUT_MS = 15_000

function startFeedRequest() {
  const controller = new AbortController()
  const timeout = setTimeout(() => {
    controller.abort(new DOMException("Feed request timed out", "TimeoutError"))
  }, NEWS_REQUEST_TIMEOUT_MS)
  const clearDeadline = () => clearTimeout(timeout)
  controller.signal.addEventListener("abort", clearDeadline, { once: true })
  return { controller, clearDeadline }
}

function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError"
}

function getErrorMessage(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback
}

function refreshExistingItems(previous: NewsItem[], fetched: NewsItem[]): NewsItem[] {
  const latestById = new Map(fetched.map((item) => [item.id, item]))
  return previous.map((item) => latestById.get(item.id) ?? item)
}

type FeedState = {
  items: NewsItem[]
  pendingNewItems: NewsItem[]
  nextCursor: string | null
  pendingCursor: string | null
  paginated: boolean
}

const EMPTY_FEED: FeedState = {
  items: [], pendingNewItems: [], nextCursor: null, pendingCursor: null, paginated: false,
}

function showPending(feed: FeedState, head: NewsItem[] = []): FeedState {
  // A queued snapshot with its own cursor starts a new contiguous history.
  const tail = feed.pendingCursor ? [] : feed.items
  return {
    ...EMPTY_FEED,
    items: mergeUniqueNewsItems(head, feed.pendingNewItems, tail),
    nextCursor: feed.pendingCursor ?? feed.nextCursor,
    paginated: true,
  }
}

function mergeLivePage(feed: FeedState, page: NewsResponse, queue: boolean): FeedState {
  if (page.next_cursor === null) {
    // This is the entire filtered feed. While reading, hold new headlines
    // above the first surviving article behind the banner, in server order.
    const visibleIds = new Set(feed.items.map(item => item.id))
    const firstVisible = queue ? page.items.findIndex(item => visibleIds.has(item.id)) : 0
    if (firstVisible > 0) {
      return { ...EMPTY_FEED, items: page.items.slice(firstVisible), pendingNewItems: page.items.slice(0, firstVisible) }
    }
    return { ...EMPTY_FEED, items: page.items }
  }
  const ids = new Set(page.items.map(item => item.id))
  const overlaps = feed.pendingNewItems.some(item => ids.has(item.id))
    || (!feed.pendingCursor && feed.items.some(item => ids.has(item.id)))
  if (!overlaps) {
    // A long outage can move every loaded row off the first page. Reusing
    // the old cursor here would silently skip everything between snapshots.
    return queue
      ? { ...feed, pendingNewItems: page.items, pendingCursor: page.next_cursor }
      : { ...EMPTY_FEED, items: page.items, nextCursor: page.next_cursor }
  }
  if (!queue) return showPending(feed, page.items)
  return {
    ...feed,
    items: refreshExistingItems(feed.items, page.items),
    pendingNewItems: mergeUniqueNewsItems(dedupeById(feed.items, page.items), feed.pendingNewItems),
  }
}

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
  const [globalTrackedReadKeys, setGlobalTrackedReadKeys] = useState<string[]>([])
  const [totalCount, setTotalCount] = useState(0)
  const [{ items, pendingNewItems, nextCursor }, setFeed] = useState(EMPTY_FEED)
  const [loading, setLoading] = useState(false)
  const [loadingMore, setLoadingMore] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [tickerError, setTickerError] = useState<string | null>(null)
  const [refreshTick, setRefreshTick] = useState(0)
  const [runtimeReady, setRuntimeReady] = useState(false)

  const pageControllerRef = useRef<{ controller: AbortController; cursor: string } | null>(null)
  const feedContainerRef = useRef<HTMLElement | null>(null)
  const refreshReasonRef = useRef<"auto" | "manual">("manual")
  const lastQueryKeyRef = useRef("")
  const lastRefreshAtRef = useRef<number | null>(null)
  const pendingRefreshTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const activeRefreshControllerRef = useRef<AbortController | null>(null)
  const pendingAutoRefreshRef = useRef(false)
  const backgroundRefreshErrorLoggedRef = useRef(false)

  const filters = buildNewsFilters(activeWatchlistId, {
    tickers: ticker ? [ticker] : activeWatchlist?.tickers,
    provider,
    q: searchQuery,
  }, activeWatchlist?.provider)

  const triggerRefresh = useCallback(function refresh(reason: "auto" | "manual") {
    if (reason === "auto" && activeRefreshControllerRef.current !== null) {
      // Let a slow request finish. Repeated events must not abort every
      // response before it can reach the screen; keep one catch-up request.
      pendingAutoRefreshRef.current = true
      return
    }

    const now = Date.now()
    const lastRefreshAt = lastRefreshAtRef.current
    if (
      reason === "auto"
      && lastRefreshAt !== null
      && now - lastRefreshAt < AUTO_REFRESH_DEDUPE_MS
    ) {
      // Keep one trailing refresh: a later commit may not be in the snapshot
      // fetched by the first event in this burst.
      if (pendingRefreshTimerRef.current === null) {
        pendingRefreshTimerRef.current = setTimeout(() => {
          pendingRefreshTimerRef.current = null
          refresh("auto")
        }, AUTO_REFRESH_DEDUPE_MS - (now - lastRefreshAt))
      }
      return
    }

    if (pendingRefreshTimerRef.current !== null) {
      clearTimeout(pendingRefreshTimerRef.current)
      pendingRefreshTimerRef.current = null
    }
    // Manual refreshes always run immediately, but reset the gate so a nearby
    // automatic event does not start a duplicate fetch.
    pendingAutoRefreshRef.current = false
    lastRefreshAtRef.current = now
    refreshReasonRef.current = reason
    setRefreshTick((previous) => previous + 1)
  }, [])

  const finishRefresh = useCallback((controller: AbortController) => {
    if (activeRefreshControllerRef.current !== controller) return
    activeRefreshControllerRef.current = null
    if (pendingAutoRefreshRef.current) {
      pendingAutoRefreshRef.current = false
      triggerRefresh("auto")
    }
  }, [triggerRefresh])

  useEffect(() => () => {
    pageControllerRef.current?.controller.abort()
    pageControllerRef.current = null
    pendingAutoRefreshRef.current = false
    if (pendingRefreshTimerRef.current !== null) {
      clearTimeout(pendingRefreshTimerRef.current)
      pendingRefreshTimerRef.current = null
    }
  }, [])
  useEffect(() => {
    const request = pageControllerRef.current
    if (request && request.cursor !== nextCursor) {
      pageControllerRef.current = null
      request.controller.abort()
      setLoadingMore(false)
    }
  }, [nextCursor])
  const sseUrl = buildApiUrl("/api/v1/events/news")

  useEffect(() => {
    const controller = new AbortController()
    fetchTickers(controller.signal)
      .then((data) => {
        setTickers(data.items)
        setTickerError(null)
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) return
        setTickers([])
        setTickerError(getErrorMessage(error, "Failed to load ticker list"))
      })

    return () => controller.abort()
  }, [])

  useEffect(() => {
    // Defer past synchronous effects so localStorage hydration completes first
    const id = setTimeout(() => setRuntimeReady(true), 0)
    return () => clearTimeout(id)
  }, [])

  const sseConnected = useEventSource(sseUrl, () => { deliveryCheck.signal("sse"); triggerRefresh("auto") }, runtimeReady)

  useEffect(() => {
    if (!runtimeReady) return

    let timer: ReturnType<typeof setInterval> | null = null

    const refreshNow = () => triggerRefresh("auto")

    const startTimer = () => {
      if (timer !== null) return
      timer = setInterval(refreshNow, sseConnected ? AUTO_REFRESH_SSE_MS : AUTO_REFRESH_POLL_MS)
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
  }, [runtimeReady, sseConnected, triggerRefresh])

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
        triggerRefresh("auto")
      }
    }

    navigator.serviceWorker.addEventListener("message", onMessage)
    return () => {
      navigator.serviceWorker.removeEventListener("message", onMessage)
    }
  }, [runtimeReady, triggerRefresh])

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
    const isReadingDeep = !!feedEl && feedEl.scrollTop > 160 && items.length > 0
    const isAutoRefresh = sameQueryAsPrevious && refreshReason === "auto"
    const checkRequest = deliveryCheck.beginRequest(!sameQueryAsPrevious ? "initial"
      : isAutoRefresh ? isReadingDeep ? "queued" : "automatic" : "manual")

    // This request includes events received before it started. A new filter or
    // manual refresh also supersedes a queued update for the old request.
    pendingAutoRefreshRef.current = false
    if (pendingRefreshTimerRef.current !== null) {
      clearTimeout(pendingRefreshTimerRef.current)
      pendingRefreshTimerRef.current = null
    }
    const { controller, clearDeadline } = startFeedRequest()
    activeRefreshControllerRef.current = controller
    const cancelRequest = () => {
      if (activeRefreshControllerRef.current === controller) {
        activeRefreshControllerRef.current = null
      }
      controller.abort()
    }

    if (!sameQueryAsPrevious) {
      setFeed(EMPTY_FEED)
    }

    lastQueryKeyRef.current = queryKey
    const queueNewItems = isAutoRefresh && isReadingDeep
    if (!isAutoRefresh) {
      pageControllerRef.current?.controller.abort()
      pageControllerRef.current = null
      setLoadingMore(false)
      setLoading(true)
    }
    if (!queueNewItems) setError(null)

    fetchNews(buildNewsRequestParams(filters, {
      includeGlobalSummary: true,
      limit: 40,
      signal: controller.signal,
    }))
      .then((data) => {
        if (controller.signal.aborted) return
        deliveryCheck.response(checkRequest, data.items)
        backgroundRefreshErrorLoggedRef.current = false
        if (data.global_summary) {
          setGlobalTrackedReadKeys(data.global_summary.tracked_read_keys)
          setTotalCount(data.global_summary.total)
        }
        const fetchingOlderPage = pageControllerRef.current !== null
        setFeed((previous) => {
          const keepHistory = queueNewItems || fetchingOlderPage || previous.paginated || previous.pendingNewItems.length > 0
          if (isAutoRefresh && keepHistory) return mergeLivePage(previous, data, queueNewItems)
          return { ...EMPTY_FEED, items: data.items, nextCursor: data.next_cursor }
        })
      })
      .catch((err: unknown) => {
        if (activeRefreshControllerRef.current !== controller || isAbortError(err)) return
        if (queueNewItems) {
          if (!backgroundRefreshErrorLoggedRef.current) console.warn("Background feed refresh failed", err)
          backgroundRefreshErrorLoggedRef.current = true
          return
        }
        const message = getErrorMessage(err, "Failed to load feed")
        setError(message)
      })
      .finally(() => {
        clearDeadline()
        if (!isAutoRefresh && activeRefreshControllerRef.current === controller) {
          setLoading(false)
        }
        finishRefresh(controller)
      })

    return cancelRequest
  }, [
    activeWatchlist,
    activeWatchlistId,
    provider,
    refreshTick,
    searchQuery,
    ticker,
    finishRefresh,
  ])

  const loadMore = async () => {
    if (!nextCursor || loading || pageControllerRef.current) return
    const checkRequest = deliveryCheck.beginRequest("more")
    const { controller, clearDeadline } = startFeedRequest()
    pageControllerRef.current = { controller, cursor: nextCursor }

    setLoadingMore(true)
    setError(null)

    try {
      const data = await fetchNews(buildNewsRequestParams(filters, {
        limit: 40,
        cursor: nextCursor,
        signal: controller.signal,
      }))
      if (controller.signal.aborted) return
      deliveryCheck.response(checkRequest, data.items)
      setFeed((previous) => previous.nextCursor === nextCursor ? {
        ...previous, items: mergeUniqueNewsItems(previous.items, data.items), nextCursor: data.next_cursor, paginated: true,
      } : previous)
    } catch (err: unknown) {
      if (pageControllerRef.current?.controller !== controller || isAbortError(err)) return
      const message = getErrorMessage(err, "Failed to load more")
      setError(message)
    } finally {
      clearDeadline()
      if (pageControllerRef.current?.controller === controller) {
        pageControllerRef.current = null
        setLoadingMore(false)
      }
    }
  }

  const loadPendingArticles = () => {
    setFeed((previous) => showPending(previous))
    feedContainerRef.current?.scrollTo({ top: 0, behavior: "smooth" })
  }

  return {
    error,
    feedContainerRef,
    filters,
    globalTrackedReadKeys,
    items,
    loadMore,
    loadPendingArticles,
    loading,
    loadingMore,
    nextCursor,
    pendingNewItems,
    tickerError,
    tickers,
    totalCount,
    triggerRefresh,
  }
}
