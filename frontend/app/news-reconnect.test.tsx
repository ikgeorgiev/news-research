import { act, cleanup, renderHook, waitFor } from "@testing-library/react"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"

import * as api from "@/lib/api"
import { NewsItem } from "@/lib/types"
import { useNewsFeed } from "./use-news-feed"

vi.mock("@/lib/api", async (importOriginal) => ({
  ...await importOriginal<typeof import("@/lib/api")>(),
  fetchNews: vi.fn(),
  fetchTickers: vi.fn(),
}))

class Stream extends EventTarget {
  static instances: Stream[] = []
  closed = false
  constructor(public url: string) { super(); Stream.instances.push(this) }
  close() { this.closed = true }
  emit(type: string) { this.dispatchEvent(new Event(type)) }
}

const first: NewsItem = {
  id: 1, read_key: "first", title: "First release", url: "https://example.com/first",
  source: "Example", provider: "Business Wire", summary: null,
  published_at: "2026-11-16T07:00:00Z", first_seen_at: "2026-11-16T07:00:01Z",
  alert_sent_at: null, tickers: ["PDI"], dedupe_group: "first",
}

describe("news feed with a real subscription hook", () => {
  let stored: NewsItem[]
  let now: number

  beforeEach(() => {
    vi.clearAllMocks()
    Stream.instances = []
    vi.stubGlobal("EventSource", Stream)
    Object.defineProperty(document, "visibilityState", { configurable: true, value: "visible" })
    now = 10_000
    vi.spyOn(Date, "now").mockImplementation(() => now)
    stored = [first]
    vi.mocked(api.fetchTickers).mockResolvedValue({ items: [], total: 0 })
    vi.mocked(api.fetchNews).mockImplementation(async () => ({
      items: [...stored], next_cursor: null,
      meta: { count: stored.length, limit: 40, sort: "latest" },
    }))
  })

  afterEach(() => {
    cleanup()
    vi.useRealTimers()
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  it.each(["unsupported", "blocked"])("polls for new releases when EventSource is %s", async (mode) => {
    vi.stubGlobal("EventSource", mode === "unsupported" ? undefined : class {
      constructor() { throw new DOMException("Connection blocked", "SecurityError") }
    })
    vi.useFakeTimers()
    const { result } = renderHook(() => useNewsFeed({
      activeWatchlistId: "all", ticker: "", provider: "", searchQuery: "",
    }))
    await act(async () => { await vi.advanceTimersByTimeAsync(1) })
    expect(result.current.items).toEqual([first])
    const release = { ...first, id: 2, title: "Polled release" }
    stored.unshift(release)
    await act(async () => { await vi.advanceTimersByTimeAsync(30_000) })
    expect(result.current.items).toEqual([release, first])
  })

  it.each(["error", "unavailable"])("shows releases missed before subscription and during %s", async (outage) => {
    const { result } = renderHook(() => useNewsFeed({
      activeWatchlistId: "all", ticker: "", provider: "", searchQuery: "",
    }))
    await waitFor(() => expect(result.current.items).toEqual([first]))
    await waitFor(() => expect(Stream.instances).toHaveLength(1))
    const stream = Stream.instances[0]
    const beforeSubscription = { ...first, id: 2, title: "Stored before subscription" }
    stored.unshift(beforeSubscription)
    await act(async () => { stream.emit("ready") })
    expect(result.current.items[0]).toEqual(beforeSubscription)
    expect(api.fetchNews).toHaveBeenCalledTimes(2)

    // Freeze timers: no thirty-second or two-minute fallback can help here.
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval"] })
    await act(async () => { stream.emit(outage) })
    const duringOutage = { ...first, id: 3, title: "Stored during outage" }
    stored.unshift(duringOutage)
    now += 2_100
    await act(async () => { stream.emit("ready") })
    expect(result.current.items[0]).toEqual(duringOutage)
    expect(api.fetchNews).toHaveBeenCalledTimes(3)
    expect(Stream.instances).toHaveLength(1)
  })
})
