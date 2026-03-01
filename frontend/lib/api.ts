import { NewsCountResponse, NewsIdsResponse, NewsResponse, TickerResponse } from "./types"

const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8000"

export async function fetchTickers(signal?: AbortSignal): Promise<TickerResponse> {
  const response = await fetch(`${API_BASE}/api/v1/tickers`, { signal, cache: "no-store" })
  if (!response.ok) {
    throw new Error(`Ticker request failed: ${response.status}`)
  }
  return response.json()
}

export async function fetchNews(params: {
  tickers?: string[]
  provider?: string
  includeUnmapped?: boolean
  includeUnmappedFromProvider?: string
  q?: string
  cursor?: string | null
  limit?: number
  from?: string
  to?: string
  signal?: AbortSignal
}): Promise<NewsResponse> {
  const query = new URLSearchParams()
  if (params.tickers && params.tickers.length > 0) {
    query.set("ticker", params.tickers.join(","))
  }
  if (params.provider) query.set("provider", params.provider)
  if (params.includeUnmapped !== undefined) {
    query.set("include_unmapped", String(params.includeUnmapped))
  }
  if (params.includeUnmappedFromProvider) {
    query.set("include_unmapped_from_provider", params.includeUnmappedFromProvider)
  }
  if (params.q) query.set("q", params.q)
  if (params.cursor) query.set("cursor", params.cursor)
  if (params.limit) query.set("limit", String(params.limit))
  if (params.from) query.set("from", params.from)
  if (params.to) query.set("to", params.to)

  const response = await fetch(`${API_BASE}/api/v1/news?${query.toString()}`, {
    signal: params.signal,
    cache: "no-store",
  })

  if (!response.ok) {
    throw new Error(`News request failed: ${response.status}`)
  }

  return response.json()
}

export async function fetchNewsCount(params: {
  tickers?: string[]
  provider?: string
  includeUnmappedFromProvider?: string
  q?: string
  signal?: AbortSignal
}): Promise<NewsCountResponse> {
  const query = new URLSearchParams()
  if (params.tickers && params.tickers.length > 0) {
    query.set("ticker", params.tickers.join(","))
  }
  if (params.provider) query.set("provider", params.provider)
  if (params.includeUnmappedFromProvider) {
    query.set("include_unmapped_from_provider", params.includeUnmappedFromProvider)
  }
  if (params.q) query.set("q", params.q)

  const response = await fetch(`${API_BASE}/api/v1/news/count?${query.toString()}`, {
    signal: params.signal,
    cache: "no-store",
  })

  if (!response.ok) {
    throw new Error(`News count request failed: ${response.status}`)
  }

  return response.json()
}

export async function fetchNewsIds(params: {
  tickers?: string[]
  provider?: string
  includeUnmappedFromProvider?: string
  q?: string
  signal?: AbortSignal
}): Promise<NewsIdsResponse> {
  const query = new URLSearchParams()
  if (params.tickers && params.tickers.length > 0) {
    query.set("ticker", params.tickers.join(","))
  }
  if (params.provider) query.set("provider", params.provider)
  if (params.includeUnmappedFromProvider) {
    query.set("include_unmapped_from_provider", params.includeUnmappedFromProvider)
  }
  if (params.q) query.set("q", params.q)

  const response = await fetch(`${API_BASE}/api/v1/news/ids?${query.toString()}`, {
    signal: params.signal,
    cache: "no-store",
  })

  if (!response.ok) {
    throw new Error(`News IDs request failed: ${response.status}`)
  }

  return response.json()
}
