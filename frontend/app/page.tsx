"use client"

import { FormEvent, MouseEvent, useEffect, useMemo, useRef, useState } from "react"
import { fetchNews, fetchNewsCount, fetchNewsIds, fetchTickers } from "@/lib/api"
import {
  disablePushNotifications,
  enablePushNotifications,
  getPushStatus,
  isPushSupported,
  syncPushScopes,
} from "@/lib/push"
import { NewsItem, PushAlertScopes, TickerItem } from "@/lib/types"

const STATIC_PROVIDERS = ["Yahoo Finance", "PR Newswire", "GlobeNewswire", "Business Wire"]
const AUTO_REFRESH_MS = 30_000
const AUTO_REFRESH_DEDUPE_MS = 2_000
const MAX_PERSISTED_READ_IDS = 20_000
const MAX_PERSISTED_STARRED_IDS = 10_000
const NEWS_IDS_PAGE_SIZE = 1000
const PUSH_SUBSCRIBED_STORAGE_KEY = "pushSubscribed"
const LEGACY_NOTIFICATIONS_STORAGE_KEY = "notificationsEnabled"

type Watchlist = {
  id: string
  name: string
  provider?: string
  q?: string
  tickers?: string[]
}

const DEFAULT_WATCHLISTS: Watchlist[] = [
  { id: "all", name: "All News" },
]

function trimIdSet(input: Set<number>, maxSize: number): Set<number> {
  if (input.size <= maxSize) return input
  const trimmed = Array.from(input).slice(input.size - maxSize)
  return new Set(trimmed)
}

function toSafeExternalUrl(url: string | null | undefined): string | null {
  if (!url) return null
  try {
    const parsed = new URL(url)
    if (parsed.protocol === "http:" || parsed.protocol === "https:") {
      return parsed.toString()
    }
  } catch {
    return null
  }
  return null
}

function persistJson(key: string, value: unknown): void {
  try {
    localStorage.setItem(key, JSON.stringify(value))
  } catch (err) {
    console.warn(`Failed to persist ${key} to localStorage`, err)
  }
}

function persistValue(key: string, value: string): void {
  try {
    localStorage.setItem(key, value)
  } catch (err) {
    console.warn(`Failed to persist ${key} to localStorage`, err)
  }
}

function removePersistedValue(key: string): void {
  try {
    localStorage.removeItem(key)
  } catch (err) {
    console.warn(`Failed to remove ${key} from localStorage`, err)
  }
}

function formatDetailedDate(iso: string | null | undefined): string {
  if (!iso) return "Unknown"
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return "Unknown"
  const dateStr = d.toDateString()
  const timeStr = d.toTimeString().split(' ')[0]
  
  const now = new Date().getTime()
  const then = d.getTime()
  const sec = Math.max(1, Math.floor((now - then) / 1000))
  let rel = ""
  
  if (sec < 60) {
    rel = `${sec} secs`
  } else {
    const min = Math.floor(sec / 60)
    if (min < 60) {
      rel = `${min} mins`
    } else {
      const hr = Math.floor(min / 60)
      if (hr < 24) {
        rel = `${hr} hour${hr !== 1 ? 's' : ''}`
      } else {
        const day = Math.floor(hr / 24)
        rel = `${day} day${day !== 1 ? 's' : ''}`
      }
    }
  }

  return `${dateStr} ${timeStr} (${rel})`
}

// Simple SVG Icons
const StarIcon = ({ fill = "none" }: { fill?: string }) => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill={fill} stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2" />
  </svg>
)

const CheckIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <polyline points="20 6 9 17 4 12" />
  </svg>
)

const BellIcon = ({ active = false }: { active?: boolean }) => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill={active ? "currentColor" : "none"} stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9" />
    <path d="M13.73 21a2 2 0 0 1-3.46 0" />
  </svg>
)

function mergeUniqueNewsItems(...groups: NewsItem[][]): NewsItem[] {
  const merged: NewsItem[] = []
  const seen = new Set<number>()
  for (const group of groups) {
    for (const item of group) {
      if (seen.has(item.id)) continue
      seen.add(item.id)
      merged.push(item)
    }
  }
  return merged
}

export default function Page() {
  const [tickers, setTickers] = useState<TickerItem[]>([])
  const [globalItems, setGlobalItems] = useState<NewsItem[]>([])
  const [totalCount, setTotalCount] = useState(0)
  const [items, setItems] = useState<NewsItem[]>([])
  const [nextCursor, setNextCursor] = useState<string | null>(null)

  // Watchlists state
  // Watchlists state
  const [customWatchlists, setCustomWatchlists] = useState<Watchlist[]>([])
  const [activeWatchlistId, setActiveWatchlistId] = useState<string>("all")
  
  const activeWatchlist = useMemo(() => {
    return DEFAULT_WATCHLISTS.find(w => w.id === activeWatchlistId) || 
           customWatchlists.find(w => w.id === activeWatchlistId)
  }, [activeWatchlistId, customWatchlists])

  // Create Watchlist State
  const [isCreatingWatchlist, setIsCreatingWatchlist] = useState(false)
  const [newWatchlistName, setNewWatchlistName] = useState("")
  const [selectedTickers, setSelectedTickers] = useState<Set<string>>(new Set())

  // Context menu state
  const [contextMenu, setContextMenu] = useState<{ watchlistId: string; x: number; y: number } | null>(null)
  const [renamingWatchlistId, setRenamingWatchlistId] = useState<string | null>(null)
  const [renameValue, setRenameValue] = useState("")

  // Search/Filter state
  const [ticker, setTicker] = useState("")
  const [provider, setProvider] = useState(activeWatchlist?.provider || "")
  const [searchInput, setSearchInput] = useState("")
  const [searchQuery, setSearchQuery] = useState(activeWatchlist?.q || "")
  const [refreshTick, setRefreshTick] = useState(0)

  // View Mode & Expansion state
  const [viewMode, setViewMode] = useState<"list" | "full">("list")
  const [expandedIds, setExpandedIds] = useState<Set<number>>(new Set())

  // Read/Unread state
  const [readIds, setReadIds] = useState<Set<number>>(new Set())
  const [starredIds, setStarredIds] = useState<Set<number>>(new Set())

  const [pendingNewItems, setPendingNewItems] = useState<NewsItem[]>([])

  const [loading, setLoading] = useState(false)
  const [loadingMore, setLoadingMore] = useState(false)
  const [error, setError] = useState<string | null>(null)
  
  const [mounted, setMounted] = useState(false)
  const [isPageVisible, setIsPageVisible] = useState<boolean>(
    () => typeof document === "undefined" || document.visibilityState === "visible"
  )
  const feedGenerationRef = useRef(0)
  const feedContainerRef = useRef<HTMLElement | null>(null)
  const refreshReasonRef = useRef<"auto" | "manual">("manual")
  const itemsRef = useRef<NewsItem[]>([])
  const pendingNewItemsRef = useRef<NewsItem[]>([])
  const lastQueryKeyRef = useRef("")
  const lastAutoRefreshAtRef = useRef(0)

  const [pushSubscribed, setPushSubscribed] = useState(false)
  const [, setPushSupported] = useState(false)
  const [alertIncludeAllNews, setAlertIncludeAllNews] = useState(true)
  const [alertWatchlistIds, setAlertWatchlistIds] = useState<Set<string>>(new Set())
  const alertWatchlistKey = useMemo(
    () => Array.from(alertWatchlistIds).sort().join(","),
    [alertWatchlistIds]
  )
  const activePushScopeNames = useMemo(() => {
    const names: string[] = []
    if (alertIncludeAllNews) names.push("All News")
    if (alertWatchlistIds.size > 0) {
      for (const watchlist of customWatchlists) {
        if (alertWatchlistIds.has(watchlist.id)) {
          names.push(watchlist.name)
        }
      }
    }
    return names
  }, [alertIncludeAllNews, alertWatchlistIds, customWatchlists])
  const activePushScopeCount = activePushScopeNames.length
  const pushScopes = useMemo<PushAlertScopes>(() => {
    const watchlists = Array.from(alertWatchlistIds)
      .map((watchlistId) => customWatchlists.find((watchlist) => watchlist.id === watchlistId))
      .filter((watchlist): watchlist is Watchlist => Boolean(watchlist))
      .map((watchlist) => ({
        id: watchlist.id,
        name: watchlist.name,
        tickers: watchlist.tickers && watchlist.tickers.length > 0 ? [...watchlist.tickers] : undefined,
        provider: watchlist.provider || undefined,
        q: watchlist.q || undefined,
      }))

    return {
      include_all_news: alertIncludeAllNews,
      watchlists,
    }
  }, [alertIncludeAllNews, alertWatchlistKey, customWatchlists])

  // Load state from local storage on mount
  useEffect(() => {
    try {
      const storedRead = localStorage.getItem("readNewsIds")
      if (storedRead) {
        const parsed = JSON.parse(storedRead)
        if (Array.isArray(parsed)) {
          const validIds = parsed.filter((id): id is number => Number.isInteger(id))
          setReadIds(trimIdSet(new Set(validIds), MAX_PERSISTED_READ_IDS))
        }
      }
      
      const storedStarred = localStorage.getItem("starredNewsIds")
      if (storedStarred) {
        const parsed = JSON.parse(storedStarred)
        if (Array.isArray(parsed)) {
          const validIds = parsed.filter((id): id is number => Number.isInteger(id))
          setStarredIds(trimIdSet(new Set(validIds), MAX_PERSISTED_STARRED_IDS))
        }
      }

      const storedWatchlists = localStorage.getItem("customWatchlists")
      if (storedWatchlists) {
        setCustomWatchlists(JSON.parse(storedWatchlists))
      }

      const storedViewMode = localStorage.getItem("newsViewMode")
      if (storedViewMode === "list" || storedViewMode === "full") {
        setViewMode(storedViewMode)
      }

      // Push subscriptions are now the only supported notification path.
      removePersistedValue(LEGACY_NOTIFICATIONS_STORAGE_KEY)

      const storedAlertIncludeAll = localStorage.getItem("alertIncludeAllNews")
      if (storedAlertIncludeAll !== null) {
        setAlertIncludeAllNews(storedAlertIncludeAll === "true")
      }

      const storedAlertWatchlistIds = localStorage.getItem("alertWatchlistIds")
      if (storedAlertWatchlistIds) {
        const parsed = JSON.parse(storedAlertWatchlistIds)
        if (Array.isArray(parsed)) {
          const validIds = parsed.filter((id): id is string => typeof id === "string" && id.length > 0)
          setAlertWatchlistIds(new Set(validIds))
        }
      }

    } catch (e) {
      console.error("Failed to parse local storage", e)
    } finally {
      setMounted(true)
    }
  }, [])

  useEffect(() => {
    let cancelled = false

    const refreshPushStatus = async () => {
      if (!isPushSupported()) {
        if (!cancelled) {
          setPushSupported(false)
          setPushSubscribed(false)
        }
        persistValue(PUSH_SUBSCRIBED_STORAGE_KEY, "false")
        return
      }
      if (!cancelled) {
        setPushSupported(true)
      }
      try {
        const status = await getPushStatus()
        if (cancelled) return
        setPushSupported(status.supported)
        setPushSubscribed(status.subscribed)
        persistValue(PUSH_SUBSCRIBED_STORAGE_KEY, status.subscribed ? "true" : "false")
      } catch {
        if (!cancelled) {
          setPushSubscribed(false)
        }
        persistValue(PUSH_SUBSCRIBED_STORAGE_KEY, "false")
      }
    }

    void refreshPushStatus()
    return () => {
      cancelled = true
    }
  }, [])

  // Save custom watchlists
  useEffect(() => {
    persistJson("customWatchlists", customWatchlists)
  }, [customWatchlists])

  useEffect(() => {
    persistValue("alertIncludeAllNews", String(alertIncludeAllNews))
  }, [alertIncludeAllNews])

  useEffect(() => {
    persistJson("alertWatchlistIds", Array.from(alertWatchlistIds))
  }, [alertWatchlistIds])

  useEffect(() => {
    const validIds = new Set(customWatchlists.map((wl) => wl.id))
    setAlertWatchlistIds((prev) => {
      const filtered = Array.from(prev).filter((id) => validIds.has(id))
      if (filtered.length === prev.size) return prev
      return new Set(filtered)
    })
  }, [customWatchlists])

  useEffect(() => {
    itemsRef.current = items
  }, [items])

  useEffect(() => {
    pendingNewItemsRef.current = pendingNewItems
  }, [pendingNewItems])

  const isPushScopeEnabled = (watchlistId: string): boolean => {
    if (watchlistId === "all") return alertIncludeAllNews
    return alertWatchlistIds.has(watchlistId)
  }

  const togglePushScope = (watchlistId: string) => {
    if (watchlistId === "all") {
      setAlertIncludeAllNews((prev) => !prev)
      return
    }
    setAlertWatchlistIds((prev) => {
      const next = new Set(prev)
      if (next.has(watchlistId)) next.delete(watchlistId)
      else next.add(watchlistId)
      return next
    })
  }

  const handleTogglePushScope = (watchlistId: string, e?: MouseEvent) => {
    e?.preventDefault()
    e?.stopPropagation()
    togglePushScope(watchlistId)
  }

  const triggerRefresh = (reason: "auto" | "manual") => {
    refreshReasonRef.current = reason
    setRefreshTick((prev) => prev + 1)
  }

  // Save state to local storage when changed
  useEffect(() => {
    persistJson("readNewsIds", Array.from(readIds))
  }, [readIds])

  useEffect(() => {
    persistJson("starredNewsIds", Array.from(starredIds))
  }, [starredIds])

  useEffect(() => {
    persistValue("newsViewMode", viewMode)
  }, [viewMode])

  // Fetch tickers once
  useEffect(() => {
    const controller = new AbortController()
    fetchTickers(controller.signal)
      .then((data) => setTickers(data.items))
      .catch(() => setTickers([]))
    return () => controller.abort()
  }, [])

  // Sync Watchlist -> Filters (reset all filters when switching watchlists)
  useEffect(() => {
    setTicker("")
    setProvider(activeWatchlist?.provider || "")
    setSearchQuery(activeWatchlist?.q || "")
    setSearchInput(activeWatchlist?.q || "")
  }, [activeWatchlist])

  // Fetch global baseline news for unread count
  useEffect(() => {
    const controller = new AbortController()
    // Fetch a baseline of the latest news to calculate global unread counts
    fetchNews({
      includeUnmappedFromProvider: "Business Wire",
      limit: 100, // Fetch a larger chunk for accurate unread calculations
      signal: controller.signal,
    })
      .then((data) => setGlobalItems(data.items))
      .catch(() => {}) // Ignore errors for background global fetch

    return () => controller.abort()
  }, [refreshTick])

  // Fetch lightweight total count from dedicated endpoint
  useEffect(() => {
    const controller = new AbortController()
    fetchNewsCount({
      includeUnmappedFromProvider: "Business Wire",
      signal: controller.signal,
    })
      .then((data) => setTotalCount(data.total))
      .catch(() => {}) // Ignore errors for background count fetch
    return () => controller.abort()
  }, [refreshTick])

  // Auto-refresh feed while tab is visible.
  useEffect(() => {
    if (!mounted) return

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
      const visible = document.visibilityState === "visible"
      setIsPageVisible(visible)
      if (visible) {
        refreshNow()
        startTimer()
      } else {
        stopTimer()
      }
    }

    const onWindowFocus = () => {
      setIsPageVisible(true)
      refreshNow()
    }

    if (document.visibilityState === "visible") {
      setIsPageVisible(true)
      startTimer()
    } else {
      setIsPageVisible(false)
    }

    document.addEventListener("visibilitychange", onVisibilityChange)
    window.addEventListener("focus", onWindowFocus)

    return () => {
      stopTimer()
      document.removeEventListener("visibilitychange", onVisibilityChange)
      window.removeEventListener("focus", onWindowFocus)
    }
  }, [mounted])

  useEffect(() => {
    if (!mounted) return
    if (!("serviceWorker" in navigator)) return

    const onMessage = (event: MessageEvent) => {
      const payload = event.data
      if (!payload || typeof payload !== "object") return

      if (payload.type === "push-notification-click") {
        // User explicitly clicked the notification — wants to see content
        triggerRefresh("manual")
        return
      }

      if (payload.type === "push-notification") {
        // Determine if user is scrolled deep in the feed
        const feedEl = feedContainerRef.current
        const scrolledDeep = !!feedEl && feedEl.scrollTop > 160 && itemsRef.current.length > 40
        // Scrolled deep → buffer into pending banner; at top → update inline
        triggerRefresh(scrolledDeep ? "auto" : "manual")
      }
    }

    navigator.serviceWorker.addEventListener("message", onMessage)
    return () => {
      navigator.serviceWorker.removeEventListener("message", onMessage)
    }
  }, [mounted])

  useEffect(() => {
    if (!mounted) return
    if (!pushSubscribed) return

    const run = async () => {
      try {
        await syncPushScopes(pushScopes)
      } catch {
        // Keep local UI responsive even if backend sync fails.
      }
    }
    void run()
  }, [mounted, pushSubscribed, pushScopes])

  // Fetch news when filters change
  useEffect(() => {
    const queryKey = JSON.stringify({
      watchlistId: activeWatchlistId,
      watchlistTickers: [...(activeWatchlist?.tickers || [])].sort(),
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

    // Combine active watchlist tickers with any individually selected ticker filter
    let fetchTickers: string[] | undefined = undefined
    if (activeWatchlist?.tickers && activeWatchlist.tickers.length > 0) {
      fetchTickers = [...activeWatchlist.tickers]
    }
    if (ticker) {
      if (fetchTickers) {
        if (!fetchTickers.includes(ticker)) fetchTickers.push(ticker)
      } else {
        fetchTickers = [ticker]
      }
    }

    const includeGeneralFromProvider =
      activeWatchlistId === "all" && (!fetchTickers || fetchTickers.length === 0)
        ? "Business Wire"
        : undefined

    // When user is scrolled deep, fetch in the background and buffer new items
    // instead of updating the feed directly.  We snapshot the current generation
    // WITHOUT incrementing it so that an in-flight loadMore is not invalidated.
    if (isAutoRefresh && isReadingDeep) {
      const bgGeneration = feedGenerationRef.current
      const controller = new AbortController()
      fetchNews({
        tickers: fetchTickers,
        provider: provider || undefined,
        includeUnmappedFromProvider: includeGeneralFromProvider,
        q: searchQuery || undefined,
        limit: 40,
        signal: controller.signal,
      })
        .then((data) => {
          // Discard if a filter change or manual refresh superseded this background fetch.
          if (bgGeneration !== feedGenerationRef.current) return
          const fetchedItems = data.items
          const currentIds = new Set(itemsRef.current.map((i) => i.id))
          const newOnes = fetchedItems.filter((i) => !currentIds.has(i.id))
          const pendingIds = new Set(pendingNewItemsRef.current.map((i) => i.id))
          const freshOnes = newOnes.filter((i) => !pendingIds.has(i.id))
          if (freshOnes.length > 0) {
            setPendingNewItems((prev) => {
              const prevIds = new Set(prev.map((i) => i.id))
              const uniqueFresh = freshOnes.filter((i) => !prevIds.has(i.id))
              return uniqueFresh.length > 0 ? [...uniqueFresh, ...prev] : prev
            })
          }
        })
        .catch(() => {}) // Silently ignore background fetch errors
      return () => controller.abort()
    }

    // Query changed (filter/watchlist switch) — discard stale pending items immediately.
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

    fetchNews({
      tickers: fetchTickers,
      provider: provider || undefined,
      includeUnmappedFromProvider: includeGeneralFromProvider,
      q: searchQuery || undefined,
      limit: 40,
      signal: controller.signal,
    })
      .then((data) => {
        if (requestGeneration !== feedGenerationRef.current) return
        const fetchedItems = data.items
        // Refresh succeeded — safe to clear pending banner.
        setPendingNewItems([])
        const currentIds = new Set(itemsRef.current.map((item) => item.id))
        const autoRefreshNewOnes = isAutoRefresh
          ? fetchedItems.filter((item) => !currentIds.has(item.id))
          : []
        if (isAutoRefresh && itemsRef.current.length > data.items.length) {
          // Keep previously loaded pages; prepend only genuinely new top stories.
          setItems((prev) => {
            const prevIds = new Set(prev.map((item) => item.id))
            const prepend = fetchedItems.filter((item) => !prevIds.has(item.id))
            return prepend.length > 0 ? [...prepend, ...prev] : prev
          })
          setNextCursor((prev) => prev ?? data.next_cursor)
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
    ticker,
    provider,
    searchQuery,
    activeWatchlist,
    activeWatchlistId,
    refreshTick,
  ])

  const onSearchSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setSearchQuery(searchInput.trim())
  }

  const loadMore = async () => {
    if (!nextCursor || loadingMore) return

    setLoadingMore(true)
    setError(null)
    const requestGeneration = feedGenerationRef.current
    
    let fetchTickers: string[] | undefined = undefined
    if (activeWatchlist?.tickers && activeWatchlist.tickers.length > 0) {
      fetchTickers = [...activeWatchlist.tickers]
    }
    if (ticker) {
      if (fetchTickers) {
        if (!fetchTickers.includes(ticker)) fetchTickers.push(ticker)
      } else {
        fetchTickers = [ticker]
      }
    }

    try {
      const includeGeneralFromProvider =
        activeWatchlistId === "all" && (!fetchTickers || fetchTickers.length === 0)
          ? "Business Wire"
          : undefined

      const data = await fetchNews({
        tickers: fetchTickers,
        provider: provider || undefined,
        includeUnmappedFromProvider: includeGeneralFromProvider,
        q: searchQuery || undefined,
        limit: 40,
        cursor: nextCursor,
      })
      if (requestGeneration !== feedGenerationRef.current) return
      const fetchedItems = data.items
      setItems((prev) => {
        const prevIds = new Set(prev.map((item) => item.id))
        const append = fetchedItems.filter((item) => !prevIds.has(item.id))
        return append.length > 0 ? [...prev, ...append] : prev
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
    setItems((prev) => {
      const prevIds = new Set(prev.map((i) => i.id))
      const newOnes = pendingNewItems.filter((i) => !prevIds.has(i.id))
      return newOnes.length > 0 ? [...newOnes, ...prev] : prev
    })
    setPendingNewItems([])
    feedContainerRef.current?.scrollTo({ top: 0, behavior: "smooth" })
  }

  const toggleRead = (id: number, e?: MouseEvent) => {
    if (e) {
      e.stopPropagation()
      e.preventDefault()
    }
    setReadIds(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return trimIdSet(next, MAX_PERSISTED_READ_IDS)
    })
  }

  const toggleStar = (id: number, e?: MouseEvent) => {
    if (e) {
      e.stopPropagation()
      e.preventDefault()
    }
    setStarredIds(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return trimIdSet(next, MAX_PERSISTED_STARRED_IDS)
    })
  }

  const markAsReadAndOpen = (item: NewsItem, e: MouseEvent) => {
    e.stopPropagation()
    const safeUrl = toSafeExternalUrl(item.url)
    if (!safeUrl) {
      e.preventDefault()
      return
    }
    setReadIds(prev => {
      const next = new Set(prev)
      next.add(item.id)
      return trimIdSet(next, MAX_PERSISTED_READ_IDS)
    })
  }

  const toggleSummary = (item: NewsItem) => {
    // Mark as read when expanded
    setReadIds(prev => {
      const next = new Set(prev)
      next.add(item.id)
      return trimIdSet(next, MAX_PERSISTED_READ_IDS)
    })

    if (viewMode === "full") return

    setExpandedIds(prev => {
      const next = new Set(prev)
      if (next.has(item.id)) next.delete(item.id)
      else next.add(item.id)
      return next
    })
  }

  const trackedUnreadItems = useMemo(
    () => mergeUniqueNewsItems(items, pendingNewItems, globalItems),
    [items, pendingNewItems, globalItems]
  )

  // Count read IDs verified against our tracked window, plus reads beyond the
  // tracked window (e.g. from bulk "Mark All Read" via the /news/ids endpoint).
  // Untracked reads are capped at the number of articles beyond the window to
  // avoid stale localStorage IDs inflating the count.
  const unreadCount = useMemo(() => {
    let validReadCount = 0
    for (const item of trackedUnreadItems) {
      if (readIds.has(item.id)) validReadCount++
    }
    if (totalCount > 0) {
      const untrackedReads = readIds.size - validReadCount
      const untrackedArticles = Math.max(0, totalCount - trackedUnreadItems.length)
      const totalReads = validReadCount + Math.min(untrackedReads, untrackedArticles)
      return Math.max(0, totalCount - totalReads)
    }
    return trackedUnreadItems.length - validReadCount
  }, [trackedUnreadItems, readIds, totalCount])

  const handleCreateWatchlist = (e: FormEvent) => {
    e.preventDefault()
    if (!newWatchlistName.trim() || selectedTickers.size === 0) return

    const newWl: Watchlist = {
      id: "cwl_" + Date.now().toString(),
      name: newWatchlistName.trim(),
      tickers: Array.from(selectedTickers)
    }

    setCustomWatchlists(prev => [...prev, newWl])
    setNewWatchlistName("")
    setSelectedTickers(new Set())
    setIsCreatingWatchlist(false)
    setActiveWatchlistId(newWl.id)
  }

  const handleDeleteWatchlist = (id: string) => {
    setCustomWatchlists(prev => prev.filter(w => w.id !== id))
    if (activeWatchlistId === id) {
      setActiveWatchlistId("all")
    }
  }

  const toggleTickerSelection = (symbol: string) => {
    setSelectedTickers(prev => {
      const next = new Set(prev)
      if (next.has(symbol)) next.delete(symbol)
      else next.add(symbol)
      return next
    })
  }

  // Context menu handlers
  const handleWatchlistContextMenu = (e: MouseEvent, wlId: string) => {
    e.preventDefault()
    e.stopPropagation()
    setContextMenu({ watchlistId: wlId, x: e.clientX, y: e.clientY })
  }

  const closeContextMenu = () => setContextMenu(null)

  const markReadByQuery = async (params: {
    tickers?: string[]
    includeUnmappedFromProvider?: string
  }) => {
    let cursor: string | undefined = undefined
    for (;;) {
      const data = await fetchNewsIds({
        ...params,
        limit: NEWS_IDS_PAGE_SIZE,
        cursor,
      })
      if (data.ids.length > 0) {
        setReadIds(prev => {
          const next = new Set(prev)
          data.ids.forEach(id => next.add(id))
          return trimIdSet(next, MAX_PERSISTED_READ_IDS)
        })
      }
      if (!data.next_cursor || data.ids.length === 0) {
        break
      }
      cursor = data.next_cursor
    }
  }

  const handleMarkAllRead = (wlId: string) => {
    closeContextMenu()

    if (wlId === "all") {
      // Fetch ALL article IDs from the backend so we mark everything read,
      // including articles beyond the current Load More window.
      markReadByQuery({ includeUnmappedFromProvider: "Business Wire" })
        .catch(() => {
          // Fallback: mark only tracked items if the fetch fails.
          setReadIds(prev => {
            const next = new Set(prev)
            trackedUnreadItems.forEach(i => next.add(i.id))
            return trimIdSet(next, MAX_PERSISTED_READ_IDS)
          })
        })
      return
    }

    const wl = customWatchlists.find(w => w.id === wlId)
    if (!wl) return
    // For watchlists, fetch IDs filtered by the watchlist tickers.
    const wlTickers = wl.tickers || []
    if (wlTickers.length > 0) {
      markReadByQuery({ tickers: wlTickers })
        .catch(() => {
          // Fallback: mark only tracked items matching this watchlist.
          const matchingIds = trackedUnreadItems
            .filter(i => i.tickers?.some(t => wlTickers.includes(t)))
            .map(i => i.id)
          setReadIds(prev => {
            const next = new Set(prev)
            matchingIds.forEach(id => next.add(id))
            return trimIdSet(next, MAX_PERSISTED_READ_IDS)
          })
        })
    }
  }

  const handleStartRename = (wlId: string) => {
    const wl = customWatchlists.find(w => w.id === wlId)
    if (!wl) return
    setRenamingWatchlistId(wlId)
    setRenameValue(wl.name)
    closeContextMenu()
  }

  const handleFinishRename = () => {
    if (renamingWatchlistId && renameValue.trim()) {
      setCustomWatchlists(prev =>
        prev.map(w => w.id === renamingWatchlistId ? { ...w, name: renameValue.trim() } : w)
      )
    }
    setRenamingWatchlistId(null)
    setRenameValue("")
  }

  const handleContextDelete = (wlId: string) => {
    handleDeleteWatchlist(wlId)
    closeContextMenu()
  }

  const handleUpdateFeeds = () => {
    triggerRefresh("manual")
    closeContextMenu()
  }

  const [pushError, setPushError] = useState<string | null>(null)

  const togglePushSubscription = async () => {
    if (pushSubscribed) {
      setPushSubscribed(false)
      persistValue(PUSH_SUBSCRIBED_STORAGE_KEY, "false")
      setPushError(null)
      try {
        await disablePushNotifications()
      } catch {
        // Ignore unsubscribe failures.
      }
      return
    }

    setPushError(null)
    try {
      const result = await enablePushNotifications(pushScopes)
      setPushSupported(isPushSupported())
      if (result.pushActive) {
        setPushSubscribed(true)
        persistValue(PUSH_SUBSCRIBED_STORAGE_KEY, "true")
        return
      }
      if (!result.permissionGranted) {
        setPushError("Browser notification permission denied")
      } else {
        setPushError(result.reason === "push-disabled"
          ? "Push not configured on server (VAPID keys missing)"
          : "Push notifications not supported in this browser")
      }
    } catch (err) {
      setPushError(err instanceof Error ? err.message : "Failed to enable push notifications")
    }
  }

  // Close context menu on outside click or Escape
  useEffect(() => {
    if (!contextMenu) return
    const handleClick = () => closeContextMenu()
    const handleKey = (e: KeyboardEvent) => { if (e.key === "Escape") closeContextMenu() }
    document.addEventListener("click", handleClick)
    document.addEventListener("contextmenu", handleClick)
    document.addEventListener("keydown", handleKey)
    return () => {
      document.removeEventListener("click", handleClick)
      document.removeEventListener("contextmenu", handleClick)
      document.removeEventListener("keydown", handleKey)
    }
  }, [contextMenu])

  return (
    <div className="deck-root">
      {/* Sidebar */}
      <aside className="sidebar">
        <div className="brand-header">
          <h1>CEF News</h1>
          <p>MARKET DATA</p>
        </div>

        <div
          className={`watchlist-item all-news-item ${activeWatchlistId === "all" ? "active" : ""}`}
          onClick={() => setActiveWatchlistId("all")}
          onContextMenu={(e) => handleWatchlistContextMenu(e, "all")}
          style={{ fontWeight: "bold", fontSize: "1.1rem", marginBottom: "1rem", display: "flex", justifyContent: "space-between", alignItems: "center" }}
        >
          <span>All News</span>
          <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
            <button
              className="icon-button"
              title={
                alertIncludeAllNews
                  ? "Disable push notifications for All News"
                  : "Enable push notifications for All News"
              }
              onClick={(e) => handleTogglePushScope("all", e)}
              style={{
                padding: "2px",
                color: alertIncludeAllNews ? "var(--accent-blue)" : "var(--text-muted)",
              }}
            >
              <BellIcon active={alertIncludeAllNews} />
            </button>
            {unreadCount > 0 && (
              <span style={{ 
                background: activeWatchlistId === "all" ? "var(--accent-blue)" : "rgba(255, 255, 255, 0.1)", 
                color: activeWatchlistId === "all" ? "white" : "var(--text-secondary)", 
                fontSize: "0.75rem", 
                padding: "2px 8px", 
                borderRadius: "12px", 
                fontWeight: "normal" 
              }}>
                {unreadCount}
              </span>
            )}
          </div>
        </div>

        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <h2>Watchlists</h2>
          {!isCreatingWatchlist && (
            <button 
              className="icon-button" 
              title="New Watchlist"
              onClick={() => setIsCreatingWatchlist(true)}
              style={{ padding: "4px", fontSize: "1.2rem" }}
            >
              +
            </button>
          )}
        </div>
        
        <div>
          {customWatchlists.map(wl => {
            const wlUnreadCount = trackedUnreadItems.filter(
              i => !readIds.has(i.id) && i.tickers?.some(t => (wl.tickers || []).includes(t))
            ).length;
            
            return (
              <div 
                key={wl.id} 
                className={`watchlist-item ${activeWatchlistId === wl.id ? "active" : ""}`}
                onClick={() => setActiveWatchlistId(wl.id)}
                onContextMenu={(e) => handleWatchlistContextMenu(e, wl.id)}
                style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}
              >
                {renamingWatchlistId === wl.id ? (
                  <input
                    autoFocus
                    className="rename-input"
                    value={renameValue}
                    onChange={(e) => setRenameValue(e.target.value)}
                    onFocus={(e) => e.target.select()}
                    onBlur={handleFinishRename}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") handleFinishRename()
                      if (e.key === "Escape") { setRenamingWatchlistId(null); setRenameValue("") }
                    }}
                    onClick={(e) => e.stopPropagation()}
                    style={{ flex: 1, fontSize: "0.9rem", padding: "2px 4px" }}
                  />
                ) : (
                  <span>{wl.name}</span>
                )}
                <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
                  <button
                    className="icon-button"
                    title={
                      isPushScopeEnabled(wl.id)
                        ? `Disable push notifications for ${wl.name}`
                        : `Enable push notifications for ${wl.name}`
                    }
                    onClick={(e) => handleTogglePushScope(wl.id, e)}
                    style={{
                      padding: "2px",
                      color: isPushScopeEnabled(wl.id) ? "var(--accent-blue)" : "var(--text-muted)",
                    }}
                  >
                    <BellIcon active={isPushScopeEnabled(wl.id)} />
                  </button>
                  {wlUnreadCount > 0 && (
                    <span style={{ 
                      background: activeWatchlistId === wl.id ? "var(--accent-blue)" : "rgba(255, 255, 255, 0.1)", 
                      color: activeWatchlistId === wl.id ? "white" : "var(--text-secondary)", 
                      fontSize: "0.75rem", 
                      padding: "2px 8px", 
                      borderRadius: "12px", 
                      fontWeight: "normal" 
                    }}>
                      {wlUnreadCount}
                    </span>
                  )}
                </div>
              </div>
            );
          })}
          {customWatchlists.length === 0 && !isCreatingWatchlist && (
            <p style={{ fontSize: "0.85rem", color: "var(--text-muted)", padding: "0 0.5rem" }}>
              No custom watchlists yet.
            </p>
          )}
        </div>

        {isCreatingWatchlist && (
          <form className="create-watchlist-form" onSubmit={handleCreateWatchlist} style={{ marginTop: "1rem", padding: "0.5rem", background: "var(--bg-layer-2)", borderRadius: "4px" }}>
            <input 
              autoFocus
              placeholder="Watchlist Name" 
              value={newWatchlistName}
              onChange={e => setNewWatchlistName(e.target.value)}
              style={{ width: "100%", marginBottom: "0.5rem" }}
            />
            <div className="ticker-selector" style={{ maxHeight: "150px", overflowY: "auto", marginBottom: "0.5rem", border: "1px solid var(--border)", borderRadius: "4px", padding: "4px" }}>
              {tickers.map(t => (
                <label key={t.symbol} style={{ display: "block", fontSize: "0.85rem", cursor: "pointer", padding: "2px 0" }}>
                  <input 
                    type="checkbox" 
                    checked={selectedTickers.has(t.symbol)} 
                    onChange={() => toggleTickerSelection(t.symbol)} 
                    style={{ marginRight: "6px" }}
                  />
                  {t.symbol}
                </label>
              ))}
            </div>
            <div style={{ display: "flex", gap: "0.5rem" }}>
              <button type="submit" className="primary" style={{ flex: 1, padding: "4px" }} disabled={!newWatchlistName.trim() || selectedTickers.size === 0}>Save</button>
              <button type="button" onClick={() => setIsCreatingWatchlist(false)} style={{ flex: 1, padding: "4px" }}>Cancel</button>
            </div>
          </form>
        )}

        {/* Context Menu */}
        {contextMenu && (
          <div
            className="context-menu"
            style={{ top: contextMenu.y, left: contextMenu.x }}
            onClick={(e) => e.stopPropagation()}
          >
            <div className="context-menu-item" onClick={() => { togglePushScope(contextMenu.watchlistId); closeContextMenu() }}>
              {isPushScopeEnabled(contextMenu.watchlistId) ? "Disable Push" : "Enable Push"}
            </div>
            <div className="context-menu-separator" />
            <div className="context-menu-item" onClick={() => handleMarkAllRead(contextMenu.watchlistId)}>
              Mark All Items as Read
            </div>
            {contextMenu.watchlistId !== "all" && (
              <>
                <div className="context-menu-item" onClick={() => handleStartRename(contextMenu.watchlistId)}>
                  Rename
                </div>
                <div className="context-menu-separator" />
                <div className="context-menu-item delete" onClick={() => handleContextDelete(contextMenu.watchlistId)}>
                  Delete
                </div>
              </>
            )}
            <div className="context-menu-separator" />
            <div className="context-menu-item" onClick={handleUpdateFeeds}>
              Update Feeds in Folder
            </div>
          </div>
        )}
      </aside>

      {/* Main Content */}
      <main className="main-content">
        <section className="filter-rack">
          <form onSubmit={onSearchSubmit} style={{ display: "flex", gap: "0.5rem", flex: 1 }}>
            <input
              value={searchInput}
              onChange={(event) => setSearchInput(event.target.value)}
              placeholder="Search news..."
              style={{ width: "300px" }}
            />
            <button className="primary" type="submit">Search</button>
          </form>

          <select value={ticker} onChange={(event) => setTicker(event.target.value)}>
            <option value="">All symbols</option>
            {tickers.map((item) => (
              <option key={item.symbol} value={item.symbol}>{item.symbol}</option>
            ))}
          </select>

          <select value={provider} onChange={(event) => setProvider(event.target.value)}>
            <option value="">All sources</option>
            {STATIC_PROVIDERS.map((item) => (
              <option key={item} value={item}>{item}</option>
            ))}
          </select>
        </section>

        <section className="status-strip" style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "1rem" }}>
            <span>
              {loading
                ? "Refreshing..."
                : `${unreadCount} Unread / ${totalCount} Total`}
              {items.length !== totalCount ? ` • ${items.length} shown` : ""}
            </span>
            {error && <span style={{ color: "#F23645" }}>Error: {error}</span>}
          </div>
          
          {mounted && (
            <div style={{ display: "flex", gap: "1rem", alignItems: "center" }}>
              <button 
                className="icon-button" 
                onClick={() => triggerRefresh("manual")}
                title="Refresh Data"
                style={{ cursor: "pointer", padding: "4px 12px", fontSize: "0.85rem", borderRadius: "4px", border: "1px solid var(--border-color)", background: "transparent", color: "var(--text-secondary)", display: "flex", alignItems: "center", gap: "0.4rem" }}
              >
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.59-9.21l5.67-1.16" />
                </svg>
                Refresh
              </button>
              <button
                className="icon-button"
                onClick={togglePushSubscription}
                title={
                  pushSubscribed
                    ? "Disable push notifications"
                    : "Enable push notifications"
                }
                style={{ cursor: "pointer", padding: "4px 12px", fontSize: "0.85rem", borderRadius: "4px", border: "1px solid var(--border-color)", background: pushSubscribed ? "var(--accent-blue)" : pushError ? "rgba(242, 54, 69, 0.15)" : "transparent", color: pushSubscribed ? "var(--text-solid)" : pushError ? "#F23645" : "var(--text-secondary)", display: "flex", alignItems: "center", gap: "0.4rem" }}
              >
                <BellIcon active={pushSubscribed} />
                {pushSubscribed ? "Push On" : pushError ? "Push Failed" : "Push"}
              </button>
              {pushError && !pushSubscribed && (
                <span style={{ fontSize: "0.75rem", color: "#F23645", maxWidth: "200px" }} title={pushError}>
                  {pushError}
                </span>
              )}
              <span
                title={
                  activePushScopeCount > 0
                    ? `Enabled push scopes: ${activePushScopeNames.join(", ")}`
                    : "No push scopes enabled"
                }
                style={{
                  padding: "4px 10px",
                  fontSize: "0.8rem",
                  borderRadius: "999px",
                  border: "1px solid var(--border-color)",
                  background: activePushScopeCount > 0 ? "rgba(41, 98, 255, 0.15)" : "transparent",
                  color: activePushScopeCount > 0 ? "var(--accent-blue)" : "var(--text-muted)",
                  whiteSpace: "nowrap",
                }}
              >
                Push scopes: {activePushScopeCount}
              </span>
              <div className="view-mode-toggle" style={{ display: "flex", gap: "0.25rem", zIndex: 10 }}>
              <button 
                className={`icon-button ${viewMode === "list" ? "active" : ""}`} 
                onClick={() => setViewMode("list")}
                title="List View"
                style={{ cursor: "pointer", padding: "4px 12px", fontSize: "0.75rem", borderRadius: "4px", border: "1px solid var(--border-color)", background: viewMode === "list" ? "var(--accent-blue)" : "transparent", color: viewMode === "list" ? "var(--text-solid)" : "var(--text-secondary)" }}
              >
                List
              </button>
              <button 
                className={`icon-button ${viewMode === "full" ? "active" : ""}`} 
                onClick={() => setViewMode("full")}
                title="Full View"
                style={{ cursor: "pointer", padding: "4px 12px", fontSize: "0.75rem", borderRadius: "4px", border: "1px solid var(--border-color)", background: viewMode === "full" ? "var(--accent-blue)" : "transparent", color: viewMode === "full" ? "var(--text-solid)" : "var(--text-secondary)" }}
              >
                Full
              </button>
            </div>
          </div>
          )}
        </section>

        <section className="feed-container" ref={feedContainerRef}>
          {pendingNewItems.length > 0 && (
            <div className="new-articles-banner" onClick={loadPendingArticles}>
              {pendingNewItems.length} new article{pendingNewItems.length !== 1 ? "s" : ""} available — click to load
            </div>
          )}
          {!loading && items.length === 0 && (
            <div style={{ padding: "3rem 1.5rem", textAlign: "center", color: "var(--text-secondary)" }}>
              <p style={{ fontSize: "1.1rem", marginBottom: "0.5rem" }}>No news found</p>
              <p style={{ fontSize: "0.85rem", color: "var(--text-muted)" }}>Try adjusting your filters or switching watchlists.</p>
            </div>
          )}
          {items.map((item) => {
            const isRead = readIds.has(item.id)
            const isStarred = starredIds.has(item.id)
            const isExpanded = viewMode === "full" || expandedIds.has(item.id)
            const safeItemUrl = toSafeExternalUrl(item.url)
             
            return (
              <article 
                key={item.id} 
                className={`feed-row-wrapper ${isRead ? "read" : "unread"}`}
              >
                <div 
                  className={`feed-row`}
                  onClick={() => toggleSummary(item)}
                >
                  <div className="main-col">
                    <div className="headline-content">
                      <div className="headline">
                        <a 
                          href={safeItemUrl ?? "#"}
                          target="_blank" 
                          rel="noreferrer" 
                          aria-disabled={!safeItemUrl}
                          onClick={(e) => markAsReadAndOpen(item, e)}
                        >
                          {item.title}
                        </a>
                      </div>
                    </div>
                    <div className="metadata-row">
                      <div className="source-and-tickers">
                        <span className="source" title={item.provider}>{item.provider}</span>
                        <div className="ticker-rack">
                          {item.tickers && item.tickers.length > 0 ? (
                            item.tickers.map(t => (
                              <span key={t} className="ticker-pill">{t}</span>
                            ))
                          ) : (
                            <span className="ticker-pill">GENERAL</span>
                          )}
                        </div>
                      </div>
                      <span className="stamp">
                        {formatDetailedDate(item.published_at)}
                      </span>
                    </div>
                  </div>

                  <div className="actions">
                    <button 
                      className="icon-button" 
                      title={isRead ? "Mark as unread" : "Mark as read"}
                      onClick={(e) => toggleRead(item.id, e)}
                    >
                      <CheckIcon />
                    </button>
                    <button 
                      className={`icon-button ${isStarred ? "starred" : ""}`} 
                      title="Star story"
                      onClick={(e) => toggleStar(item.id, e)}
                    >
                      <StarIcon fill={isStarred ? "currentColor" : "none"} />
                    </button>
                  </div>
                </div>

                {isExpanded && (
                  <div className="feed-row-details">
                    {item.summary && (
                      <div className="summary-text">{item.summary}</div>
                    )}
                    <p>
                      <strong>Seen by system:</strong> {formatDetailedDate(item.first_seen_at)}
                    </p>
                    <p>
                      <strong>Alert sent:</strong>{" "}
                      {item.alert_sent_at ? formatDetailedDate(item.alert_sent_at) : "Pending / not sent"}
                    </p>
                  </div>
                )}
              </article>
            )
          })}

          <div className="load-more-container">
            <button className="primary" disabled={loading || !nextCursor || loadingMore} onClick={loadMore}>
              {loadingMore ? "Loading..." : nextCursor ? "Load More" : "End of results"}
            </button>
          </div>
        </section>
      </main>
    </div>
  )
}
