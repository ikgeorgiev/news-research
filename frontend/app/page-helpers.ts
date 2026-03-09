import { NewsItem, Watchlist } from "@/lib/types"

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

export function mergeUniqueIds(...groups: number[][]): number[] {
  const merged: number[] = []
  const seen = new Set<number>()
  for (const group of groups) {
    for (const id of group) {
      if (seen.has(id)) continue
      seen.add(id)
      merged.push(id)
    }
  }
  return merged
}

export function buildFetchParams(
  activeWatchlist: Watchlist | undefined,
  activeWatchlistId: string,
  ticker: string
): { fetchTickers: string[] | undefined; includeGeneralFromProvider: string | undefined } {
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
  return { fetchTickers, includeGeneralFromProvider }
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
