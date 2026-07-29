import type { PushWatchlistScope, Watchlist } from "@/lib/types"

// These are transport limits for the push DTO, not product limits for saved
// watchlists. Keep them aligned with backend/app/schemas.py.
export const PUSH_SCOPE_LIMITS = {
  watchlists: 50,
  tickersPerWatchlist: 500,
  watchlistId: 128,
  watchlistName: 120,
  provider: 120,
  query: 256,
  ticker: 16,
} as const

function truncateCodePoints(value: string, maxLength: number): string {
  return Array.from(value).slice(0, maxLength).join("")
}

function codePointLength(value: string): number {
  return Array.from(value).length
}

function normalizeTickers(values: Iterable<string>): {
  error: string | null
  tickers: string[]
} {
  const tickers: string[] = []
  const seen = new Set<string>()

  for (const value of values) {
    const symbol = value.trim().toUpperCase()
    if (!symbol || seen.has(symbol)) continue
    if (codePointLength(symbol) > PUSH_SCOPE_LIMITS.ticker) {
      return {
        error: `a ticker exceeds ${PUSH_SCOPE_LIMITS.ticker} characters`,
        tickers: [],
      }
    }

    seen.add(symbol)
    tickers.push(symbol)
    if (tickers.length > PUSH_SCOPE_LIMITS.tickersPerWatchlist) {
      return {
        error: `it contains more than ${PUSH_SCOPE_LIMITS.tickersPerWatchlist} distinct tickers`,
        tickers: [],
      }
    }
  }

  return { error: null, tickers }
}

export type PreparedPushWatchlistScope =
  | { error: null; scope: PushWatchlistScope }
  | { error: string; scope: null }

export function preparePushWatchlistScope(
  watchlist: Watchlist
): PreparedPushWatchlistScope {
  const id = watchlist.id.trim()
  if (!id) {
    return { error: "its ID is empty", scope: null }
  }
  if (codePointLength(id) > PUSH_SCOPE_LIMITS.watchlistId) {
    return {
      error: `its ID exceeds ${PUSH_SCOPE_LIMITS.watchlistId} characters`,
      scope: null,
    }
  }

  const provider = watchlist.provider?.trim() || undefined
  if (provider && codePointLength(provider) > PUSH_SCOPE_LIMITS.provider) {
    return {
      error: `its provider exceeds ${PUSH_SCOPE_LIMITS.provider} characters`,
      scope: null,
    }
  }

  const q = watchlist.q?.trim() || undefined
  if (q && codePointLength(q) > PUSH_SCOPE_LIMITS.query) {
    return {
      error: `its query exceeds ${PUSH_SCOPE_LIMITS.query} characters`,
      scope: null,
    }
  }

  const normalizedTickers = normalizeTickers(watchlist.tickers)
  if (normalizedTickers.error) {
    return { error: normalizedTickers.error, scope: null }
  }
  if (normalizedTickers.tickers.length === 0 && !provider && !q) {
    return { error: "it has no filters", scope: null }
  }

  // Name is display-only metadata, so shortening it cannot change matching
  // semantics. Semantic fields above are rejected instead of truncated.
  const name = truncateCodePoints(
    watchlist.name.trim(),
    PUSH_SCOPE_LIMITS.watchlistName
  ) || undefined

  return {
    error: null,
    scope: {
      id,
      name,
      tickers:
        normalizedTickers.tickers.length > 0
          ? normalizedTickers.tickers
          : undefined,
      provider,
      q,
    },
  }
}
