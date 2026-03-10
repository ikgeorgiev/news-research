import { NewsIdsResponse, NewsResponse, TickerResponse } from "./types"
import { buildApiUrl } from "./api-base"

type NewsQueryParams = {
  tickers?: string[]
  provider?: string
  includeUnmappedFromProvider?: string
  q?: string
  cursor?: string | null
  limit?: number
}

function buildNewsQueryString(params: NewsQueryParams & { includeGlobalSummary?: boolean }): string {
  const query = new URLSearchParams()
  if (params.tickers && params.tickers.length > 0) {
    query.set("ticker", params.tickers.join(","))
  }
  if (params.provider) query.set("provider", params.provider)
  if (params.includeUnmappedFromProvider) {
    query.set("include_unmapped_from_provider", params.includeUnmappedFromProvider)
  }
  if (params.includeGlobalSummary) {
    query.set("include_global_summary", "true")
  }
  if (params.q) query.set("q", params.q)
  if (params.cursor) query.set("cursor", params.cursor)
  if (params.limit) query.set("limit", String(params.limit))
  return query.toString()
}

export async function fetchTickers(signal?: AbortSignal): Promise<TickerResponse> {
  const response = await fetch(buildApiUrl("/api/v1/tickers"), { signal, cache: "no-store" })
  if (!response.ok) {
    throw new Error(`Ticker request failed: ${response.status}`)
  }
  return response.json()
}

export async function fetchNews(params: {
  tickers?: string[]
  provider?: string
  includeUnmappedFromProvider?: string
  includeGlobalSummary?: boolean
  q?: string
  cursor?: string | null
  limit?: number
  signal?: AbortSignal
}): Promise<NewsResponse> {
  const response = await fetch(buildApiUrl(`/api/v1/news?${buildNewsQueryString(params)}`), {
    signal: params.signal,
    cache: "no-store",
  })

  if (!response.ok) {
    throw new Error(`News request failed: ${response.status}`)
  }

  return response.json()
}

export async function fetchNewsIds(params: {
  tickers?: string[]
  provider?: string
  includeUnmappedFromProvider?: string
  q?: string
  cursor?: string
  limit?: number
  signal?: AbortSignal
}): Promise<NewsIdsResponse> {
  const response = await fetch(buildApiUrl(`/api/v1/news/ids?${buildNewsQueryString(params)}`), {
    signal: params.signal,
    cache: "no-store",
  })

  if (!response.ok) {
    throw new Error(`News IDs request failed: ${response.status}`)
  }

  return response.json()
}

