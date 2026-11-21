import { act, cleanup, renderHook, waitFor } from "@testing-library/react"
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
    cleanup()
    vi.useRealTimers()
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

  it("shows a release announced during the refresh window without waiting for the fallback poll", async () => {
    mockedUseEventSource.mockImplementation((_url, onEvent, enabled) => {
      if (enabled) sseEventListener = onEvent
      return enabled
    })
    const { result } = renderHook(() => useNewsFeed({
      activeWatchlistId: "all", provider: "", searchQuery: "", ticker: "",
    }))
    await waitFor(() => expect(result.current.items).toHaveLength(1))
    await waitFor(() => expect(sseEventListener).not.toBeNull())
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] })

    // First notification reads a snapshot before the next release is committed.
    await act(async () => { sseEventListener?.() })
    expect(mockedApi.fetchNews).toHaveBeenCalledTimes(2)
    const previous = await mockedApi.fetchNews.mock.results[1].value
    mockedApi.fetchNews.mockResolvedValue({
      ...previous,
      items: [{ ...previous.items[0], id: 2, title: "Newly committed release" }, ...previous.items],
    })

    now += 500
    await act(async () => { sseEventListener?.() })
    now += 500
    await act(async () => { sseEventListener?.() })
    expect(mockedApi.fetchNews).toHaveBeenCalledTimes(2)

    // No more notifications or fallback polls occur: the pending update must run.
    now += 1_000
    await act(async () => { await vi.advanceTimersByTimeAsync(1_500) })
    expect(mockedApi.fetchNews).toHaveBeenCalledTimes(3)
    expect(result.current.items[0].title).toBe("Newly committed release")
  })

  it.each(["manual", "unmount"])("cancels a queued refresh on %s", async (action) => {
    const { result, unmount } = renderHook(() => useNewsFeed({
      activeWatchlistId: "all", provider: "", searchQuery: "", ticker: "",
    }))
    await waitFor(() => expect(result.current.items).toHaveLength(1))
    await waitFor(() => expect(sseEventListener).not.toBeNull())
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] })
    await act(async () => { sseEventListener?.() })
    now += 500
    await act(async () => { sseEventListener?.() })
    expect(mockedApi.fetchNews).toHaveBeenCalledTimes(2)
    if (action === "manual") {
      await act(async () => { result.current.triggerRefresh("manual") })
    } else {
      unmount()
    }
    now += 2_000
    await act(async () => { await vi.advanceTimersByTimeAsync(2_000) })
    expect(mockedApi.fetchNews).toHaveBeenCalledTimes(action === "manual" ? 3 : 2)
  })

  it.each(["foreground", "background"])(
    "finishes a slow %s request during continuous events, then catches up once",
    async (mode) => {
      if (mode === "background") {
        const defaultResponse = mockedApi.fetchNews.getMockImplementation()!
        mockedApi.fetchNews.mockImplementationOnce(async (params) => {
          const data = await defaultResponse(params)
          return {
            ...data,
            items: Array.from({ length: 50 }, (_, index) => ({ ...data.items[0], id: index + 1 })),
          }
        })
      }
      const { result } = renderHook(() => useNewsFeed({
        activeWatchlistId: "all", provider: "", searchQuery: "", ticker: "",
      }))
      await waitFor(() => expect(result.current.items).toHaveLength(mode === "background" ? 50 : 1))
      await waitFor(() => expect(sseEventListener).not.toBeNull())
      const baseline = await mockedApi.fetchNews.mock.results[0].value
      if (mode === "background") {
        result.current.feedContainerRef.current = { scrollTop: 200 } as HTMLElement
      }

      type Response = Awaited<ReturnType<typeof api.fetchNews>>
      const requests: { resolve: (data: Response) => void; signal?: AbortSignal }[] = []
      mockedApi.fetchNews.mockImplementation(({ signal }) => new Promise((resolve, reject) => {
        requests.push({ resolve, signal })
        signal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), { once: true })
      }))
      await act(async () => { sseEventListener?.() })
      for (let event = 0; event < 3; event += 1) {
        now += 2_100
        await act(async () => { sseEventListener?.() })
      }
      expect(requests[0].signal?.aborted).toBe(false)
      expect(requests).toHaveLength(1)

      const first = { ...baseline.items[0], id: 1001, title: "First slow response" }
      const second = { ...baseline.items[0], id: 1002, title: "Latest committed release" }
      await act(async () => { requests[0].resolve({ ...baseline, items: [first, ...baseline.items] }) })
      const visible = () => mode === "background" ? result.current.pendingNewItems : result.current.items
      expect(visible()[0].id).toBe(1001)
      expect(requests).toHaveLength(2)
      await act(async () => { requests[1].resolve({ ...baseline, items: [second, first, ...baseline.items] }) })
      expect(visible()[0].id).toBe(1002)
      expect(visible().filter((item) => item.id === 1001)).toHaveLength(1)
      expect(mockedApi.fetchNews).toHaveBeenCalledTimes(3)
    }
  )

  it.each(["manual", "filter", "unmount"])(
    "cancels an active request and clears its queued event on %s",
    async (action) => {
      const { result, rerender, unmount } = renderHook(({ ticker }) => useNewsFeed({
        activeWatchlistId: "all", provider: "", searchQuery: "", ticker,
      }), { initialProps: { ticker: "" } })
      await waitFor(() => expect(result.current.items).toHaveLength(1))
      await waitFor(() => expect(sseEventListener).not.toBeNull())
      const baseline = await mockedApi.fetchNews.mock.results[0].value
      type Response = Awaited<ReturnType<typeof api.fetchNews>>
      const requests: { resolve: (data: Response) => void; signal?: AbortSignal }[] = []
      mockedApi.fetchNews.mockImplementation(({ signal }) => new Promise((resolve, reject) => {
        requests.push({ resolve, signal })
        signal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), { once: true })
      }))
      await act(async () => { sseEventListener?.() })
      now += 2_100
      await act(async () => { sseEventListener?.() })
      await act(async () => {
        if (action === "manual") result.current.triggerRefresh("manual")
        else if (action === "filter") rerender({ ticker: "UTF" })
        else unmount()
      })
      expect(requests[0].signal?.aborted).toBe(true)
      if (action === "unmount") {
        expect(mockedApi.fetchNews).toHaveBeenCalledTimes(2)
        return
      }
      expect(requests).toHaveLength(2)
      now += 2_100
      await act(async () => { requests[1].resolve(baseline) })
      expect(mockedApi.fetchNews).toHaveBeenCalledTimes(3)
    }
  )

  it.each([
    ["foreground", "page-3"], ["background", "page-3"],
    ["foreground", null], ["background", null],
  ])(
    "refreshes article details in a paginated %s feed with cursor %s without dropping older rows",
    async (mode, nextCursor) => {
      const defaultResponse = mockedApi.fetchNews.getMockImplementation()!
      const baseline = await defaultResponse({})
      const firstPage = Array.from({ length: 40 }, (_, index) => ({ ...baseline.items[0], id: 100 - index }))
      const secondPage = Array.from({ length: 40 }, (_, index) => ({ ...baseline.items[0], id: 60 - index }))
      mockedApi.fetchNews.mockResolvedValueOnce({ ...baseline, items: firstPage, next_cursor: "page-2" })
      const { result } = renderHook(() => useNewsFeed({
        activeWatchlistId: "all", provider: "", searchQuery: "", ticker: "",
      }))
      await waitFor(() => expect(result.current.items).toHaveLength(40))
      await waitFor(() => expect(sseEventListener).not.toBeNull())
      mockedApi.fetchNews.mockResolvedValueOnce({ ...baseline, items: secondPage, next_cursor: nextCursor })
      await act(async () => { await result.current.loadMore() })
      expect(result.current.items).toHaveLength(80)
      const scrollTo = vi.fn()
      result.current.feedContainerRef.current = { scrollTop: mode === "background" ? 200 : 0, scrollTo } as unknown as HTMLElement
      const refreshed = { ...firstPage[0], summary: "Verified publisher body", tickers: ["MFM", "MMT"] }
      const newRelease = { ...firstPage[0], id: 101, title: "New release" }
      mockedApi.fetchNews.mockResolvedValue({
        ...baseline, items: [newRelease, refreshed, ...firstPage.slice(1, 39)], next_cursor: "new-head-cursor",
      })
      await act(async () => { sseEventListener?.() })
      expect(result.current.items.find((item) => item.id === 100)).toEqual(refreshed)
      expect(result.current.items.slice(-40)).toEqual(secondPage)
      expect(result.current.nextCursor).toBe(nextCursor)
      expect(scrollTo).not.toHaveBeenCalled()
      if (mode === "background") {
        expect(result.current.items.map((item) => item.id)).toEqual([...firstPage, ...secondPage].map((item) => item.id))
        expect(result.current.pendingNewItems).toEqual([newRelease])
        const enrichedNewRelease = { ...newRelease, summary: "New release full summary", tickers: ["PDI"] }
        mockedApi.fetchNews.mockResolvedValue({ ...baseline, items: [enrichedNewRelease, refreshed, ...firstPage.slice(1, 39)] })
        now += 2_100
        await act(async () => { sseEventListener?.() })
        expect(result.current.pendingNewItems).toEqual([enrichedNewRelease])
        act(() => { result.current.loadPendingArticles() })
        expect(result.current.items[0]).toEqual(enrichedNewRelease)
      } else {
        expect(result.current.items[0]).toEqual(newRelease)
        expect(result.current.pendingNewItems).toHaveLength(0)
      }
      expect(result.current.items).toHaveLength(81)
      expect(new Set(result.current.items.map((item) => item.id)).size).toBe(81)
    }
  )

  it("keeps queued releases that roll off the first page before a reader scrolls to the top", async () => {
    const baseline = await mockedApi.fetchNews.getMockImplementation()!({})
    const page = (latest: number) => Array.from({ length: 40 }, (_, index) => ({
      ...baseline.items[0], id: latest - index,
    }))
    mockedApi.fetchNews.mockResolvedValueOnce({ ...baseline, items: page(80), next_cursor: "older" })
    const { result } = renderHook(() => useNewsFeed({ activeWatchlistId: "all", provider: "", searchQuery: "", ticker: "" }))
    await waitFor(() => expect(result.current.nextCursor).toBe("older"))
    mockedApi.fetchNews.mockResolvedValueOnce({ ...baseline, items: page(40) })
    await act(async () => { await result.current.loadMore() })
    const container = document.createElement("section")
    result.current.feedContainerRef.current = container
    container.scrollTop = 200
    for (const latest of [100, 120]) {
      mockedApi.fetchNews.mockResolvedValueOnce({ ...baseline, items: page(latest) })
      now += 2_100
      await act(async () => { result.current.triggerRefresh("auto") })
    }
    expect(result.current.pendingNewItems).toHaveLength(40)
    container.scrollTop = 0
    mockedApi.fetchNews.mockResolvedValueOnce({ ...baseline, items: page(140) })
    now += 2_100
    await act(async () => { result.current.triggerRefresh("auto") })
    expect(result.current.pendingNewItems).toHaveLength(0)
    expect(result.current.items.map(item => item.id)).toEqual(Array.from({ length: 140 }, (_, index) => 140 - index))
    expect(result.current.nextCursor).toBeNull()
  })

  it.each(["head first", "older page first"])("keeps both pages when a live refresh overlaps Load More (%s)", async (order) => {
    const baseline = await mockedApi.fetchNews.getMockImplementation()!({})
    const article = (id: number) => ({ ...baseline.items[0], id })
    mockedApi.fetchNews.mockResolvedValueOnce({ ...baseline, items: [article(3), article(2)], next_cursor: "older" })
    const { result } = renderHook(() => useNewsFeed({ activeWatchlistId: "all", provider: "", searchQuery: "", ticker: "" }))
    await waitFor(() => expect(result.current.nextCursor).toBe("older"))
    let finishOlder!: () => void
    let finishHead!: () => void
    mockedApi.fetchNews.mockImplementationOnce(() => new Promise(resolve => {
      finishOlder = () => resolve({ ...baseline, items: [article(1)], next_cursor: null })
    })).mockImplementationOnce(() => new Promise(resolve => {
      finishHead = () => resolve({ ...baseline, items: [article(4), article(3)], next_cursor: "new-head-cursor" })
    }))
    act(() => { void result.current.loadMore() })
    act(() => { result.current.triggerRefresh("auto") })
    const completions = order === "head first" ? [finishHead, finishOlder] : [finishOlder, finishHead]
    for (const complete of completions) await act(async () => { complete() })
    expect(result.current.items.map(item => item.id)).toEqual([4, 3, 2, 1])
    expect(result.current.nextCursor).toBeNull()
  })

  it("allows the new filter to paginate while a canceled older page is settling", async () => {
    const baseline = await mockedApi.fetchNews.getMockImplementation()!({})
    mockedApi.fetchNews.mockResolvedValueOnce({ ...baseline, next_cursor: "original-page" })
    const { result, rerender } = renderHook(({ ticker }) => useNewsFeed({
      activeWatchlistId: "all", provider: "", searchQuery: "", ticker,
    }), { initialProps: { ticker: "" } })
    await waitFor(() => expect(result.current.nextCursor).toBe("original-page"))
    let finishOldPage!: () => void
    mockedApi.fetchNews.mockImplementationOnce(() => new Promise(resolve => {
      finishOldPage = () => resolve(baseline)
    })).mockResolvedValueOnce({ ...baseline, next_cursor: "filtered-page" })
    act(() => { void result.current.loadMore() })
    const oldSignal = mockedApi.fetchNews.mock.lastCall![0].signal
    rerender({ ticker: "UTF" })
    await waitFor(() => expect(result.current.nextCursor).toBe("filtered-page"))
    expect(oldSignal?.aborted).toBe(true)
    expect(result.current.loadingMore).toBe(false)
    let finishNewPage!: () => void
    mockedApi.fetchNews.mockImplementationOnce(() => new Promise(resolve => {
      finishNewPage = () => resolve({ ...baseline, items: [{ ...baseline.items[0], id: 2 }] })
    }))
    act(() => { void result.current.loadMore() })
    await act(async () => { finishOldPage() })
    expect(result.current.loadingMore).toBe(true)
    expect(result.current.nextCursor).toBe("filtered-page")
    await act(async () => { finishNewPage() })
    expect(result.current.items.map(item => item.id)).toEqual([1, 2])
    expect(result.current.loadingMore).toBe(false)
  })

  it("times out a stuck request so its queued update can complete", async () => {
    const { result } = renderHook(() => useNewsFeed({
      activeWatchlistId: "all", provider: "", searchQuery: "", ticker: "",
    }))
    await waitFor(() => expect(result.current.items).toHaveLength(1))
    await waitFor(() => expect(sseEventListener).not.toBeNull())
    const baseline = await mockedApi.fetchNews.mock.results[0].value
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] })
    let stuckSignal: AbortSignal | undefined
    mockedApi.fetchNews.mockImplementationOnce(({ signal }) => new Promise((_resolve, reject) => {
      stuckSignal = signal
      signal?.addEventListener("abort", () => reject(signal.reason), { once: true })
    }))
    mockedApi.fetchNews.mockResolvedValue({
      ...baseline, items: [{ ...baseline.items[0], id: 2, title: "Recovered update" }],
    })
    await act(async () => { sseEventListener?.() })
    now += 500
    await act(async () => { sseEventListener?.() })
    expect(mockedApi.fetchNews).toHaveBeenCalledTimes(2)
    now += 14_500
    await act(async () => { await vi.advanceTimersByTimeAsync(15_000) })
    expect(stuckSignal?.aborted).toBe(true)
    expect(stuckSignal?.reason.name).toBe("TimeoutError")
    expect(mockedApi.fetchNews).toHaveBeenCalledTimes(3)
    expect(result.current.items[0].title).toBe("Recovered update")
    expect(result.current.error).toBeNull()
  })
})
