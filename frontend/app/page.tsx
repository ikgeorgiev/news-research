"use client"

import { FormEvent, useEffect, useState, useMemo } from "react"
import { fetchNews, fetchTickers } from "@/lib/api"
import { NewsItem, TickerItem } from "@/lib/types"

const STATIC_PROVIDERS = ["Yahoo Finance", "PR Newswire", "GlobeNewswire", "Business Wire"]

type Watchlist = {
  id: string
  name: string
  provider?: string
  q?: string
}

const DEFAULT_WATCHLISTS: Watchlist[] = [
  { id: "all", name: "All News" },
  { id: "business_wire", name: "Business Wire CEF News", provider: "Business Wire" },
  { id: "sec", name: "SEC Filings", q: "SEC" },
  { id: "distributions", name: "Distributions", q: "distribution" },
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
  const [items, setItems] = useState<NewsItem[]>([])
  const [nextCursor, setNextCursor] = useState<string | null>(null)

  // Watchlists state
  const [activeWatchlistId, setActiveWatchlistId] = useState<string>("all")
  const activeWatchlist = DEFAULT_WATCHLISTS.find(w => w.id === activeWatchlistId)

  // Search/Filter state
  const [ticker, setTicker] = useState("")
  const [provider, setProvider] = useState(activeWatchlist?.provider || "")
  const [searchInput, setSearchInput] = useState("")
  const [searchQuery, setSearchQuery] = useState(activeWatchlist?.q || "")

  // Read/Unread state
  const [readIds, setReadIds] = useState<Set<number>>(new Set())
  const [starredIds, setStarredIds] = useState<Set<number>>(new Set())

  const [loading, setLoading] = useState(false)
  const [loadingMore, setLoadingMore] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Load state from local storage on mount
  useEffect(() => {
    try {
      const storedRead = localStorage.getItem("readNewsIds")
      if (storedRead) setReadIds(new Set(JSON.parse(storedRead)))
      
      const storedStarred = localStorage.getItem("starredNewsIds")
      if (storedStarred) setStarredIds(new Set(JSON.parse(storedStarred)))
    } catch (e) {
      console.error("Failed to parse local storage", e)
    }
  }, [])

  // Save state to local storage when changed
  useEffect(() => {
    localStorage.setItem("readNewsIds", JSON.stringify(Array.from(readIds)))
  }, [readIds])

  useEffect(() => {
    localStorage.setItem("starredNewsIds", JSON.stringify(Array.from(starredIds)))
  }, [starredIds])

  // Fetch tickers once
  useEffect(() => {
    const controller = new AbortController()
    fetchTickers(controller.signal)
      .then((data) => setTickers(data.items))
      .catch(() => setTickers([]))
    return () => controller.abort()
  }, [])

  // Sync Watchlist -> Filters
  useEffect(() => {
    setProvider(activeWatchlist?.provider || "")
    setSearchQuery(activeWatchlist?.q || "")
    setSearchInput(activeWatchlist?.q || "")
  }, [activeWatchlist])

  // Fetch news when filters change
  useEffect(() => {
    const controller = new AbortController()
    setLoading(true)
    setError(null)

    fetchNews({
      ticker: ticker || undefined,
      provider: provider || undefined,
      includeUnmappedFromProvider: "Business Wire",
      q: searchQuery || undefined,
      limit: 40,
      signal: controller.signal,
    })
      .then((data) => {
        setItems(data.items)
        setNextCursor(data.next_cursor)
      })
      .catch((err: unknown) => {
        if (err instanceof DOMException && err.name === "AbortError") return
        const message = err instanceof Error ? err.message : "Failed to load feed"
        setError(message)
      })
      .finally(() => setLoading(false))

    return () => controller.abort()
  }, [ticker, provider, searchQuery])

  const onSearchSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setSearchQuery(searchInput.trim())
  }

  const loadMore = async () => {
    if (!nextCursor || loadingMore) return

    setLoadingMore(true)
    setError(null)
    try {
      const data = await fetchNews({
        ticker: ticker || undefined,
        provider: provider || undefined,
        includeUnmappedFromProvider: "Business Wire",
        q: searchQuery || undefined,
        limit: 40,
        cursor: nextCursor,
      })
      setItems((prev) => [...prev, ...data.items])
      setNextCursor(data.next_cursor)
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : "Failed to load more"
      setError(message)
    } finally {
      setLoadingMore(false)
    }
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

  const markAsReadAndOpen = (item: NewsItem) => {
    setReadIds(prev => {
      const next = new Set(prev)
      next.add(item.id)
      return next
    })
    window.open(item.url, "_blank", "noreferrer")
  }

  const unreadCount = useMemo(() => {
    return items.filter(i => !readIds.has(i.id)).length
  }, [items, readIds])

  return (
    <div className="deck-root">
      {/* Sidebar */}
      <aside className="sidebar">
        <div className="brand-header">
          <h1>CEF News</h1>
          <p>MARKET DATA</p>
        </div>

        <h2>Watchlists</h2>
        <div>
          {DEFAULT_WATCHLISTS.map(wl => (
            <div 
              key={wl.id} 
              className={`watchlist-item ${activeWatchlistId === wl.id ? "active" : ""}`}
              onClick={() => setActiveWatchlistId(wl.id)}
            >
              <span>{wl.name}</span>
            </div>
          ))}
        </div>
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

        <section className="status-strip">
          <span>{loading ? "Refreshing..." : `${unreadCount} Unread / ${items.length} Total`}</span>
          {error ? <span style={{ color: "#F23645" }}>Error: {error}</span> : <span>Live Data</span>}
        </section>

        <section className="feed-container">
          {items.map((item, index) => {
            const isRead = readIds.has(item.id)
            const isStarred = starredIds.has(item.id)
            
            return (
              <article 
                key={`${item.id}-${index}`} 
                className={`feed-row ${isRead ? "read" : "unread"}`}
                onClick={() => markAsReadAndOpen(item)}
              >
                <div className="stamp">{timeAgo(item.published_at)}</div>
                <div className="source" title={item.provider}>{item.provider}</div>
                
                <div className="headline">
                  {item.title}
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
              </article>
            )
          })}

          <div className="load-more-container">
            <button className="primary" disabled={!nextCursor || loadingMore} onClick={loadMore}>
              {loadingMore ? "Loading..." : nextCursor ? "Load More" : "End of results"}
            </button>
          </div>
        </section>
      </main>
    </div>
  )
}
