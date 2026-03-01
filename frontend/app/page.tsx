"use client"

import { FormEvent, useEffect, useState, useMemo, useRef } from "react"
import { fetchNews, fetchNewsCount, fetchTickers } from "@/lib/api"
import { NewsItem, TickerItem } from "@/lib/types"

const STATIC_PROVIDERS = ["Yahoo Finance", "PR Newswire", "GlobeNewswire", "Business Wire"]
const AUTO_REFRESH_MS = 30_000

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

function timeAgo(iso: string): string {
  const now = new Date().getTime()
  const then = new Date(iso).getTime()
  const sec = Math.max(1, Math.floor((now - then) / 1000))
  if (sec < 60) return `${sec}s ago`
  const min = Math.floor(sec / 60)
  if (min < 60) return `${min}m ago`
  const hr = Math.floor(min / 60)
  if (hr < 24) return `${hr}h ago`
  const day = Math.floor(hr / 24)
  return `${day}d ago`
}

function formatDetailedDate(iso: string): string {
  const d = new Date(iso)
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
  const feedGenerationRef = useRef(0)
  const feedContainerRef = useRef<HTMLElement | null>(null)
  const refreshReasonRef = useRef<"auto" | "manual">("manual")
  const itemsRef = useRef<NewsItem[]>([])
  const lastQueryKeyRef = useRef("")

  // Load state from local storage on mount
  useEffect(() => {
    try {
      const storedRead = localStorage.getItem("readNewsIds")
      if (storedRead) setReadIds(new Set(JSON.parse(storedRead)))
      
      const storedStarred = localStorage.getItem("starredNewsIds")
      if (storedStarred) setStarredIds(new Set(JSON.parse(storedStarred)))

      const storedWatchlists = localStorage.getItem("customWatchlists")
      if (storedWatchlists) {
        setCustomWatchlists(JSON.parse(storedWatchlists))
      }

      const storedViewMode = localStorage.getItem("newsViewMode")
      if (storedViewMode === "list" || storedViewMode === "full") {
        setViewMode(storedViewMode)
      }
    } catch (e) {
      console.error("Failed to parse local storage", e)
    } finally {
      setMounted(true)
    }
  }, [])

  // Save custom watchlists
  useEffect(() => {
    localStorage.setItem("customWatchlists", JSON.stringify(customWatchlists))
  }, [customWatchlists])

  useEffect(() => {
    itemsRef.current = items
  }, [items])

  const triggerRefresh = (reason: "auto" | "manual") => {
    refreshReasonRef.current = reason
    setRefreshTick((prev) => prev + 1)
  }

  // Save state to local storage when changed
  useEffect(() => {
    localStorage.setItem("readNewsIds", JSON.stringify(Array.from(readIds)))
  }, [readIds])

  useEffect(() => {
    localStorage.setItem("starredNewsIds", JSON.stringify(Array.from(starredIds)))
  }, [starredIds])

  useEffect(() => {
    localStorage.setItem("newsViewMode", viewMode)
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

    const refreshNow = () => triggerRefresh("auto")
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

    const onWindowFocus = () => refreshNow()

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
  }, [mounted])

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
          const currentIds = new Set(itemsRef.current.map((i) => i.id))
          const newOnes = data.items.filter((i) => !currentIds.has(i.id))
          if (newOnes.length > 0) {
            setPendingNewItems((prev) => {
              const pendingIds = new Set(prev.map((i) => i.id))
              const freshOnes = newOnes.filter((i) => !pendingIds.has(i.id))
              return freshOnes.length > 0 ? [...freshOnes, ...prev] : prev
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
        // Refresh succeeded — safe to clear pending banner.
        setPendingNewItems([])
        if (isAutoRefresh && itemsRef.current.length > data.items.length) {
          // Keep previously loaded pages; prepend only genuinely new top stories.
          setItems((prev) => {
            const prevIds = new Set(prev.map((item) => item.id))
            const prepend = data.items.filter((item) => !prevIds.has(item.id))
            return prepend.length > 0 ? [...prepend, ...prev] : prev
          })
          setNextCursor((prev) => prev ?? data.next_cursor)
          return
        }
        setItems(data.items)
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
  }, [ticker, provider, searchQuery, activeWatchlist, refreshTick])

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
      setItems((prev) => [...prev, ...data.items])
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

  const toggleRead = (id: number, e?: React.MouseEvent) => {
    if (e) {
      e.stopPropagation()
      e.preventDefault()
    }
    setReadIds(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  const toggleStar = (id: number, e?: React.MouseEvent) => {
    if (e) {
      e.stopPropagation()
      e.preventDefault()
    }
    setStarredIds(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  const markAsReadAndOpen = (item: NewsItem, e: React.MouseEvent) => {
    e.stopPropagation()
    setReadIds(prev => {
      const next = new Set(prev)
      next.add(item.id)
      return next
    })
    window.open(item.url, "_blank", "noreferrer")
  }

  const toggleSummary = (item: NewsItem) => {
    // Mark as read when expanded
    setReadIds(prev => {
      const next = new Set(prev)
      next.add(item.id)
      return next
    })

    if (viewMode === "full") return

    setExpandedIds(prev => {
      const next = new Set(prev)
      if (next.has(item.id)) next.delete(item.id)
      else next.add(item.id)
      return next
    })
  }

  const unreadCount = useMemo(() => {
    return globalItems.filter((item) => !readIds.has(item.id)).length
  }, [globalItems, readIds])

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
  const handleWatchlistContextMenu = (e: React.MouseEvent, wlId: string) => {
    e.preventDefault()
    e.stopPropagation()
    setContextMenu({ watchlistId: wlId, x: e.clientX, y: e.clientY })
  }

  const closeContextMenu = () => setContextMenu(null)

  const handleMarkAllRead = (wlId: string) => {
    let matchingIds: number[]
    if (wlId === "all") {
      matchingIds = globalItems.map(i => i.id)
    } else {
      const wl = customWatchlists.find(w => w.id === wlId)
      if (!wl) return
      matchingIds = globalItems
        .filter(i => i.tickers?.some(t => (wl.tickers || []).includes(t)))
        .map(i => i.id)
    }
    setReadIds(prev => {
      const next = new Set(prev)
      matchingIds.forEach(id => next.add(id))
      return next
    })
    closeContextMenu()
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
            const wlUnreadCount = globalItems.filter(
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
                : `${unreadCount} Unread (latest ${globalItems.length}) / ${totalCount} Total`}
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
          {items.map((item, index) => {
            const isRead = readIds.has(item.id)
            const isStarred = starredIds.has(item.id)
            const isExpanded = viewMode === "full" || expandedIds.has(item.id)
            
            return (
              <article 
                key={`${item.id}-${index}`} 
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
                          href={item.url} 
                          target="_blank" 
                          rel="noreferrer" 
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
                      <div 
                        className="summary-text"
                        dangerouslySetInnerHTML={{ __html: item.summary }} 
                      />
                    )}
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
