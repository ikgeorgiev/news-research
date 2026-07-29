import { act, renderHook, waitFor } from "@testing-library/react"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"

import { useEventSource } from "@/hooks/useEventSource"
import * as api from "@/lib/api"

import { useNewsFeed } from "./use-news-feed"

vi.mock("@/lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api")>()
  return {
    ...actual,
    fetchNews: vi.fn(),
    fetchTickers: vi.fn(),
  }
})

vi.mock("@/hooks/useEventSource", () => ({
  useEventSource: vi.fn(),
}))

const mockedApi = vi.mocked(api)
const mockedUseEventSource = vi.mocked(useEventSource)
const originalServiceWorkerDescriptor = Object.getOwnPropertyDescriptor(navigator, "serviceWorker")

describe("useNewsFeed auto refresh", () => {
  let autoRefreshTimer: (() => void) | null
  let now: number
  let serviceWorkerMessageListener: ((event: MessageEvent) => void) | null
  let sseEventListener: (() => void) | null

  beforeEach(() => {
    vi.clearAllMocks()
    autoRefreshTimer = null
    now = 10_000
    serviceWorkerMessageListener = null
    sseEventListener = null
    vi.spyOn(Date, "now").mockImplementation(() => now)
    const originalSetInterval = globalThis.setInterval
    vi.spyOn(globalThis, "setInterval").mockImplementation((handler, timeout, ...args) => {
      if (
        typeof handler === "function"
        && (timeout === 30_000 || timeout === 120_000)
      ) {
        autoRefreshTimer = handler
        return -1 as unknown as ReturnType<typeof setInterval>
      }
      return originalSetInterval(handler, timeout, ...args)
    })

    mockedApi.fetchTickers.mockResolvedValue({
      items: [],
      total: 0,
    })
    mockedApi.fetchNews.mockResolvedValue({
      items: Array.from({ length: 1 }, (_, index) => ({
        id: index + 1,
        read_key: `key-${index + 1}`,
        title: `Article ${index + 1}`,
        url: `https://example.com/${index + 1}`,
        source: "Example",
        provider: "Business Wire",
        summary: null,
        published_at: "2026-07-29T12:00:00Z",
        first_seen_at: "2026-07-29T12:01:00Z",
        alert_sent_at: null,
        tickers: [],
        dedupe_group: `group-${index + 1}`,
      })),
      next_cursor: null,
      meta: {
        count: 1,
        limit: 40,
        sort: "latest",
      },
      global_summary: {
        total: 1,
        tracked_ids: [],
        tracked_read_keys: [],
        tracked_limit: 100,
      },
    })

    mockedUseEventSource.mockImplementation((_url, onEvent, enabled) => {
      if (enabled) sseEventListener = onEvent
      return false
    })

    Object.defineProperty(navigator, "serviceWorker", {
      configurable: true,
      value: {
        addEventListener: vi.fn((type: string, listener: (event: MessageEvent) => void) => {
          if (type === "message") serviceWorkerMessageListener = listener
        }),
        removeEventListener: vi.fn(),
      },
    })
  })

  afterEach(() => {
    vi.restoreAllMocks()
    if (originalServiceWorkerDescriptor) {
      Object.defineProperty(navigator, "serviceWorker", originalServiceWorkerDescriptor)
    } else {
      Reflect.deleteProperty(navigator, "serviceWorker")
    }
  })

  it("dedupes bursts across SSE, timer, focus, visibility, and service-worker events", async () => {
    const { result } = renderHook(() =>
      useNewsFeed({
        activeWatchlistId: "all",
        provider: "",
        searchQuery: "",
        ticker: "",
      })
    )

    await waitFor(() => expect(mockedApi.fetchNews).toHaveBeenCalledTimes(1))
    await waitFor(() => expect(result.current.items).toHaveLength(1))
    await waitFor(() => expect(sseEventListener).not.toBeNull())
    await waitFor(() => expect(serviceWorkerMessageListener).not.toBeNull())

    act(() => {
      sseEventListener?.()
    })
    await waitFor(() => expect(mockedApi.fetchNews).toHaveBeenCalledTimes(2))

    now += 500
    act(() => {
      autoRefreshTimer?.()
      window.dispatchEvent(new Event("focus"))
      document.dispatchEvent(new Event("visibilitychange"))
      serviceWorkerMessageListener?.(
        new MessageEvent("message", {
          data: { type: "push-notification" },
        })
      )
    })

    expect(mockedApi.fetchNews).toHaveBeenCalledTimes(2)

    act(() => {
      result.current.triggerRefresh("manual")
    })
    await waitFor(() => expect(mockedApi.fetchNews).toHaveBeenCalledTimes(3))

    now += 500
    act(() => {
      sseEventListener?.()
    })
    expect(mockedApi.fetchNews).toHaveBeenCalledTimes(3)
  })
})
