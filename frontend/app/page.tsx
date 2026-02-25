"use client"

import { FormEvent, useEffect, useMemo, useState } from "react"

import { fetchNews, fetchTickers } from "@/lib/api"
import { NewsItem, TickerItem } from "@/lib/types"

const STATIC_PROVIDERS = ["Yahoo Finance", "PR Newswire", "GlobeNewswire", "Business Wire"]

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

export default function Page() {
  const [tickers, setTickers] = useState<TickerItem[]>([])
  const [items, setItems] = useState<NewsItem[]>([])
  const [nextCursor, setNextCursor] = useState<string | null>(null)

  const [ticker, setTicker] = useState("")
  const [provider, setProvider] = useState("")
  const [searchInput, setSearchInput] = useState("")
  const [searchQuery, setSearchQuery] = useState("")

  const [loading, setLoading] = useState(false)
  const [loadingMore, setLoadingMore] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const controller = new AbortController()
    fetchTickers(controller.signal)
      .then((data) => setTickers(data.items))
      .catch(() => setTickers([]))
    return () => controller.abort()
  }, [])

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
    if (!nextCursor || loadingMore) {
      return
    }

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

  return (
    <main className="deck-root">
      <section className="hero">
        <p className="eyebrow">Closed-End Fund Intelligence Stream</p>
        <h1>CEF WIRE DECK</h1>
        <p className="subtitle">
          Fast tape for distributions, filings, fund actions, and cross-source headlines.
        </p>
      </section>

      <section className="filter-rack">
        <form onSubmit={onSearchSubmit} className="search-panel">
          <label htmlFor="search">Headline Scan</label>
          <div className="search-row">
            <input
              id="search"
              value={searchInput}
              onChange={(event) => setSearchInput(event.target.value)}
              placeholder="fund, distribution, rights offering..."
            />
            <button type="submit">Apply</button>
          </div>
        </form>

        <div className="select-grid">
          <label>
            Ticker
            <select value={ticker} onChange={(event) => setTicker(event.target.value)}>
              <option value="">All tickers</option>
              {tickers.map((item) => (
                <option key={item.symbol} value={item.symbol}>
                  {item.symbol}
                </option>
              ))}
            </select>
          </label>

          <label>
            Provider
            <select value={provider} onChange={(event) => setProvider(event.target.value)}>
              <option value="">All providers</option>
              {STATIC_PROVIDERS.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>
        </div>
      </section>

      <section className="status-strip">
        <span>{loading ? "Loading feed..." : `${items.length} stories`}</span>
        {error ? <span className="error">{error}</span> : <span>Live local mode</span>}
      </section>

      <section className="feed-grid">
        {items.map((item, index) => (
          <article key={`${item.id}-${index}`} className="feed-card">
            <div className="feed-meta">
              <span className="stamp">{timeAgo(item.published_at)}</span>
              <span className="source">{item.provider}</span>
            </div>

            <a className="headline" href={item.url} target="_blank" rel="noreferrer">
              {item.title}
            </a>

            {item.summary ? <p className="summary">{item.summary}</p> : null}

            <div className="chips">
              {item.tickers.length > 0 ? (
                item.tickers.map((symbol) => (
                  <span key={`${item.id}-${symbol}`} className="chip">
                    {symbol}
                  </span>
                ))
              ) : (
                <span className="chip chip-muted">GENERAL</span>
              )}
            </div>
          </article>
        ))}
      </section>

      <section className="actions">
        <button disabled={!nextCursor || loadingMore} onClick={loadMore}>
          {loadingMore ? "Loading more..." : nextCursor ? "Load More" : "No More Items"}
        </button>
      </section>
    </main>
  )
}
