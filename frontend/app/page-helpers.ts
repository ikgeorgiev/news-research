import type { NewsFilterParams } from "@/lib/api"
import { NewsItem } from "@/lib/types"

export const GENERAL_NEWS_PROVIDER = "Business Wire"

export const STATIC_PROVIDER_OPTIONS = [
  "Yahoo Finance",
  "PR Newswire",
  "GlobeNewswire",
  "FinancialContent",
  GENERAL_NEWS_PROVIDER,
]

export function toSafeExternalUrl(url: string | null | undefined): string | null {
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

export function formatDetailedDate(iso: string | null | undefined): string {
  if (!iso) return "Unknown"
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) return "Unknown"

  const dateStr = date.toDateString()
  const timeStr = date.toTimeString().split(" ")[0]
  const now = Date.now()
  const then = date.getTime()
  const secondsAgo = Math.max(1, Math.floor((now - then) / 1000))
  let relative = ""

  if (secondsAgo < 60) {
    relative = `${secondsAgo} secs`
  } else {
    const minutesAgo = Math.floor(secondsAgo / 60)
    if (minutesAgo < 60) {
      relative = `${minutesAgo} mins`
    } else {
      const hoursAgo = Math.floor(minutesAgo / 60)
      if (hoursAgo < 24) {
        relative = `${hoursAgo} hour${hoursAgo !== 1 ? "s" : ""}`
      } else {
        const daysAgo = Math.floor(hoursAgo / 24)
        relative = `${daysAgo} day${daysAgo !== 1 ? "s" : ""}`
      }
    }
  }

  return `${dateStr} ${timeStr} (${relative})`
}

export function mergeUniqueNewsItems(...groups: NewsItem[][]): NewsItem[] {
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

export function mergeUniqueIds<T>(...groups: T[][]): T[] {
  const merged: T[] = []
  const seen = new Set<T>()
  for (const group of groups) {
    for (const id of group) {
      if (seen.has(id)) continue
      seen.add(id)
      merged.push(id)
    }
  }
  return merged
}

export function buildNewsFilters(
  watchlistId: string,
  { tickers, provider, q }: NewsFilterParams = {},
  savedProvider?: string
): NewsFilterParams {
  const symbols = tickers?.length ? tickers : undefined
  const source = provider?.trim() || undefined
  const allowGeneral = watchlistId === "all" || [source, savedProvider].some(
    name => name?.trim().toLowerCase() === GENERAL_NEWS_PROVIDER.toLowerCase()
  )
  return {
    tickers: symbols,
    provider: source,
    q: q?.trim() || undefined,
    includeUnmappedFromProvider: !symbols && allowGeneral ? GENERAL_NEWS_PROVIDER : undefined,
  }
}

// Mirror the news and news/ids endpoints for local counts and offline read actions.
export function matchesNewsFilters(item: NewsItem, filters: NewsFilterParams): boolean {
  const provider = item.provider.trim().toLowerCase()
  if (filters.provider && provider !== filters.provider.toLowerCase()) return false
  if (filters.tickers?.length) {
    if (!filters.tickers.some(ticker => item.tickers.includes(ticker))) return false
  } else if (!item.tickers.length && provider !== filters.includeUnmappedFromProvider?.toLowerCase()) {
    return false
  }
  const query = filters.q?.toLowerCase()
  return !query || item.title.toLowerCase().includes(query)
    || item.tickers.some(ticker => ticker.toLowerCase().includes(query))
}

export function dedupeById<T extends { id: number }>(existing: T[], incoming: T[]): T[] {
  const ids = new Set(existing.map((item) => item.id))
  return incoming.filter((item) => !ids.has(item.id))
}

export function createWatchlistId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return `cwl_${crypto.randomUUID()}`
  }
  return `cwl_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`
}
