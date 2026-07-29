import { describe, expect, it } from "vitest"

import type { Watchlist } from "@/lib/types"

import {
  preparePushWatchlistScope,
  PUSH_SCOPE_LIMITS,
} from "./watchlist-limits"

function makeWatchlist(overrides: Partial<Watchlist> = {}): Watchlist {
  return {
    id: "watchlist-1",
    name: "Income funds",
    tickers: ["GOF"],
    ...overrides,
  }
}

describe("preparePushWatchlistScope", () => {
  it("normalizes only transport-safe fields without mutating the watchlist", () => {
    const name = "N".repeat(PUSH_SCOPE_LIMITS.watchlistName + 1)
    const watchlist = makeWatchlist({
      name,
      provider: "  Business Wire  ",
      q: "  rights offering  ",
      tickers: [" gof ", "GOF", " utf "],
    })

    const result = preparePushWatchlistScope(watchlist)

    expect(result.error).toBeNull()
    expect(result.scope).toEqual({
      id: "watchlist-1",
      name: "N".repeat(PUSH_SCOPE_LIMITS.watchlistName),
      provider: "Business Wire",
      q: "rights offering",
      tickers: ["GOF", "UTF"],
    })
    expect(watchlist).toEqual({
      id: "watchlist-1",
      name,
      provider: "  Business Wire  ",
      q: "  rights offering  ",
      tickers: [" gof ", "GOF", " utf "],
    })
  })

  it.each([
    [
      "ID",
      makeWatchlist({ id: "i".repeat(PUSH_SCOPE_LIMITS.watchlistId + 1) }),
      "ID exceeds",
    ],
    [
      "provider",
      makeWatchlist({ provider: "p".repeat(PUSH_SCOPE_LIMITS.provider + 1) }),
      "provider exceeds",
    ],
    [
      "query",
      makeWatchlist({ q: "q".repeat(PUSH_SCOPE_LIMITS.query + 1) }),
      "query exceeds",
    ],
    [
      "ticker",
      makeWatchlist({ tickers: ["T".repeat(PUSH_SCOPE_LIMITS.ticker + 1)] }),
      "ticker exceeds",
    ],
    [
      "ticker count",
      makeWatchlist({
        tickers: Array.from(
          { length: PUSH_SCOPE_LIMITS.tickersPerWatchlist + 1 },
          (_, index) => `T${index}`
        ),
      }),
      "more than 500 distinct tickers",
    ],
  ])("rejects an unrepresentable semantic %s field", (_field, watchlist, message) => {
    const result = preparePushWatchlistScope(watchlist)

    expect(result.scope).toBeNull()
    expect(result.error).toContain(message)
  })
})
