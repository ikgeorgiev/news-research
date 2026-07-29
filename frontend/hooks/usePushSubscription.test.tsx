import { act, cleanup, renderHook, waitFor } from "@testing-library/react"
import { beforeEach, describe, expect, it, vi } from "vitest"

import { usePushSubscription } from "./usePushSubscription"
import * as pushApi from "@/lib/push"
import type { Watchlist } from "@/lib/types"
import { PUSH_SCOPE_LIMITS } from "@/lib/watchlist-limits"

vi.mock("@/lib/push", () => ({
  disablePushNotifications: vi.fn(),
  enablePushNotifications: vi.fn(),
  getPushStatus: vi.fn(),
  isPushSupported: vi.fn(),
  syncPushScopes: vi.fn(),
}))

const mockedPushApi = vi.mocked(pushApi)

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise
    reject = rejectPromise
  })
  return { promise, reject, resolve }
}

describe("usePushSubscription", () => {
  beforeEach(() => {
    cleanup()
    localStorage.clear()
    vi.resetAllMocks()
    mockedPushApi.syncPushScopes.mockResolvedValue(true)
  })

  it("hydrates a subscribed push session", async () => {
    mockedPushApi.isPushSupported.mockReturnValue(true)
    mockedPushApi.getPushStatus.mockResolvedValue({
      supported: true,
      subscribed: true,
    })
    mockedPushApi.syncPushScopes.mockResolvedValue(true)

    const { result } = renderHook(() =>
      usePushSubscription({
        customWatchlists: [],
        mounted: true,
      })
    )

    await waitFor(() => expect(result.current.pushSubscribed).toBe(true))
  })

  it("stays unsubscribed when push is unavailable", async () => {
    mockedPushApi.isPushSupported.mockReturnValue(false)

    const { result } = renderHook(() =>
      usePushSubscription({
        customWatchlists: [],
        mounted: true,
      })
    )

    await waitFor(() => expect(result.current.pushSubscribed).toBe(false))
    expect(result.current.pushSubscribed).toBe(false)
    expect(result.current.pushError).toBeNull()
  })

  it("surfaces subscribe failures without flipping the subscription on", async () => {
    mockedPushApi.isPushSupported.mockReturnValue(true)
    mockedPushApi.getPushStatus.mockResolvedValue({
      supported: true,
      subscribed: false,
    })
    mockedPushApi.enablePushNotifications.mockResolvedValue({
      permissionGranted: false,
      pushActive: false,
    })

    const { result } = renderHook(() =>
      usePushSubscription({
        customWatchlists: [],
        mounted: true,
      })
    )

    await waitFor(() => expect(result.current.pushSubscribed).toBe(false))

    await act(async () => {
      await result.current.togglePushSubscription()
    })

    expect(result.current.pushSubscribed).toBe(false)
    expect(result.current.pushError).toBe("Browser notification permission denied")
  })

  it("waits for initial status cleanup before enabling push", async () => {
    const statusReconciliation = deferred<{
      supported: boolean
      subscribed: boolean
    }>()
    mockedPushApi.isPushSupported.mockReturnValue(true)
    mockedPushApi.getPushStatus.mockImplementationOnce(
      () => statusReconciliation.promise
    )
    mockedPushApi.enablePushNotifications.mockResolvedValue({
      permissionGranted: true,
      pushActive: true,
    })

    const { result } = renderHook(() =>
      usePushSubscription({
        customWatchlists: [],
        mounted: true,
      })
    )

    await waitFor(() => expect(mockedPushApi.getPushStatus).toHaveBeenCalledTimes(1))

    let enablePromise: Promise<void> | undefined
    act(() => {
      enablePromise = result.current.togglePushSubscription()
    })
    expect(mockedPushApi.enablePushNotifications).not.toHaveBeenCalled()

    await act(async () => {
      statusReconciliation.resolve({
        supported: true,
        subscribed: false,
      })
      await enablePromise
    })

    expect(mockedPushApi.enablePushNotifications).toHaveBeenCalledTimes(1)
    expect(result.current.pushSubscribed).toBe(true)
  })

  it("keeps local push state when backend delete fails", async () => {
    mockedPushApi.isPushSupported.mockReturnValue(true)
    mockedPushApi.getPushStatus.mockResolvedValue({
      supported: true,
      subscribed: true,
    })
    mockedPushApi.disablePushNotifications.mockRejectedValue(new Error("unsubscribe failed"))

    const { result } = renderHook(() =>
      usePushSubscription({
        customWatchlists: [],
        mounted: true,
      })
    )

    await waitFor(() => expect(result.current.pushSubscribed).toBe(true))

    await act(async () => {
      await result.current.togglePushSubscription()
    })

    expect(mockedPushApi.disablePushNotifications).toHaveBeenCalledTimes(1)
    expect(result.current.pushSubscribed).toBe(true)
    expect(result.current.pushError).toBe("Failed to disable push notifications")
  })

  it("preserves legacy provider/query scope filters when watchlists hydrate", async () => {
    type HookProps = {
      customWatchlists: Watchlist[]
      mounted: boolean
    }

    localStorage.setItem("alertWatchlistIds", JSON.stringify(["wl-1"]))
    localStorage.setItem("alertIncludeAllNews", "false")

    mockedPushApi.isPushSupported.mockReturnValue(true)
    mockedPushApi.getPushStatus.mockResolvedValue({
      supported: true,
      subscribed: true,
    })
    mockedPushApi.syncPushScopes.mockResolvedValue(true)

    const initialProps: HookProps = {
      customWatchlists: [],
      mounted: false,
    }

    const { result, rerender } = renderHook(
      ({ customWatchlists, mounted }: HookProps) =>
        usePushSubscription({
          customWatchlists,
          mounted,
        }),
      { initialProps }
    )

    await waitFor(() => expect(result.current.pushSubscribed).toBe(true))
    expect(result.current.isPushScopeEnabled("wl-1")).toBe(true)
    expect(localStorage.getItem("alertWatchlistIds")).toBe(JSON.stringify(["wl-1"]))
    expect(mockedPushApi.syncPushScopes).not.toHaveBeenCalled()

    rerender({
      customWatchlists: [
        {
          id: "wl-1",
          name: "Legacy Provider",
          provider: "Business Wire",
          q: "rights offering",
          tickers: [],
        },
      ],
      mounted: true,
    })

    await waitFor(() =>
        expect(mockedPushApi.syncPushScopes).toHaveBeenCalledWith({
          include_all_news: false,
          watchlists: [
            {
              id: "wl-1",
              name: "Legacy Provider",
              provider: "Business Wire",
              q: "rights offering",
            },
          ],
        })
    )
    expect(result.current.isPushScopeEnabled("wl-1")).toBe(true)
    expect(localStorage.getItem("alertWatchlistIds")).toBe(JSON.stringify(["wl-1"]))
  })

  it("serializes scope writes and coalesces rapid changes to the latest scopes", async () => {
    const firstSync = deferred<boolean>()
    const secondSync = deferred<boolean>()
    const customWatchlists: Watchlist[] = [
      { id: "wl-1", name: "First", tickers: ["GOF"] },
      { id: "wl-2", name: "Second", tickers: ["UTF"] },
    ]

    mockedPushApi.isPushSupported.mockReturnValue(true)
    mockedPushApi.getPushStatus.mockResolvedValue({
      supported: true,
      subscribed: true,
    })
    mockedPushApi.syncPushScopes
      .mockImplementationOnce(() => firstSync.promise)
      .mockImplementationOnce(() => secondSync.promise)

    const { result } = renderHook(() =>
      usePushSubscription({
        customWatchlists,
        mounted: true,
      })
    )

    await waitFor(() => expect(mockedPushApi.syncPushScopes).toHaveBeenCalledTimes(1))

    act(() => {
      result.current.togglePushScope("wl-1")
    })
    await waitFor(() => expect(result.current.isPushScopeEnabled("wl-1")).toBe(true))

    act(() => {
      result.current.togglePushScope("wl-2")
    })
    await waitFor(() => expect(result.current.isPushScopeEnabled("wl-2")).toBe(true))
    expect(mockedPushApi.syncPushScopes).toHaveBeenCalledTimes(1)

    await act(async () => {
      firstSync.resolve(true)
      await firstSync.promise
    })

    await waitFor(() => expect(mockedPushApi.syncPushScopes).toHaveBeenCalledTimes(2))
    expect(mockedPushApi.syncPushScopes).toHaveBeenLastCalledWith({
      include_all_news: true,
      watchlists: [
        {
          id: "wl-1",
          name: "First",
          tickers: ["GOF"],
          provider: undefined,
          q: undefined,
        },
        {
          id: "wl-2",
          name: "Second",
          tickers: ["UTF"],
          provider: undefined,
          q: undefined,
        },
      ],
    })

    await act(async () => {
      secondSync.resolve(true)
      await secondSync.promise
    })
  })

  it("retries a transient scope sync failure once", async () => {
    mockedPushApi.isPushSupported.mockReturnValue(true)
    mockedPushApi.getPushStatus.mockResolvedValue({
      supported: true,
      subscribed: true,
    })
    mockedPushApi.syncPushScopes
      .mockRejectedValueOnce(new Error("temporary failure"))
      .mockResolvedValueOnce(true)

    renderHook(() =>
      usePushSubscription({
        customWatchlists: [],
        mounted: true,
      })
    )

    await waitFor(() => expect(mockedPushApi.syncPushScopes).toHaveBeenCalledTimes(2))
    expect(mockedPushApi.syncPushScopes.mock.calls[0][0])
      .toEqual(mockedPushApi.syncPushScopes.mock.calls[1][0])
  })

  it("surfaces a scope sync error after the retry also fails", async () => {
    const consoleWarnSpy = vi.spyOn(console, "warn").mockImplementation(() => {})
    const customWatchlists: Watchlist[] = []
    mockedPushApi.isPushSupported.mockReturnValue(true)
    mockedPushApi.getPushStatus.mockResolvedValue({
      supported: true,
      subscribed: true,
    })
    mockedPushApi.syncPushScopes.mockRejectedValue(
      new Error("Push subscription upsert failed: 422")
    )

    try {
      const { result, rerender } = renderHook(
        ({ mounted }: { mounted: boolean }) =>
          usePushSubscription({
            customWatchlists,
            mounted,
          }),
        { initialProps: { mounted: false } }
      )

      await waitFor(() => expect(result.current.pushSubscribed).toBe(true))
      rerender({ mounted: true })
      await waitFor(() =>
        expect(result.current.pushScopeSyncError).toBe(
          "Failed to sync push notification scopes: Push subscription upsert failed: 422"
        )
      )
      expect(result.current.pushSubscribed).toBe(true)
      expect(mockedPushApi.syncPushScopes).toHaveBeenCalledTimes(2)
    } finally {
      consoleWarnSpy.mockRestore()
    }
  })

  it("turns off an unrepresentable legacy scope without changing its watchlist", async () => {
    const oversizedQuery = "q".repeat(PUSH_SCOPE_LIMITS.query + 1)
    const customWatchlists: Watchlist[] = [
      {
        id: "legacy-invalid",
        name: "Legacy invalid",
        q: oversizedQuery,
        tickers: [],
      },
    ]
    localStorage.setItem("alertWatchlistIds", JSON.stringify(["legacy-invalid"]))
    localStorage.setItem("alertIncludeAllNews", "false")
    mockedPushApi.isPushSupported.mockReturnValue(true)
    mockedPushApi.getPushStatus.mockResolvedValue({
      supported: true,
      subscribed: true,
    })

    const { result, rerender } = renderHook(
      ({ mounted }: { mounted: boolean }) =>
        usePushSubscription({
          customWatchlists,
          mounted,
        }),
      { initialProps: { mounted: false } }
    )

    await waitFor(() =>
      expect(result.current.isPushScopeEnabled("legacy-invalid")).toBe(true)
    )
    rerender({ mounted: true })
    await waitFor(() =>
      expect(result.current.pushScopeSelectionError).toContain(
        `query exceeds ${PUSH_SCOPE_LIMITS.query} characters`
      )
    )
    await waitFor(() =>
      expect(result.current.isPushScopeEnabled("legacy-invalid")).toBe(false)
    )
    expect(customWatchlists[0].q).toBe(oversizedQuery)
    expect(mockedPushApi.syncPushScopes.mock.calls.every(
      ([scopes]) => scopes.watchlists.length === 0
    )).toBe(true)
    await waitFor(() =>
      expect(localStorage.getItem("alertWatchlistIds")).toBe("[]")
    )
  })

  it("keeps at most 50 legacy push selections while preserving all watchlists", async () => {
    const customWatchlists: Watchlist[] = Array.from(
      { length: PUSH_SCOPE_LIMITS.watchlists + 1 },
      (_, index) => ({
        id: `wl-${index}`,
        name: `Watchlist ${index}`,
        tickers: [`T${index}`],
      })
    )
    localStorage.setItem(
      "alertWatchlistIds",
      JSON.stringify(customWatchlists.map((watchlist) => watchlist.id))
    )
    mockedPushApi.isPushSupported.mockReturnValue(true)
    mockedPushApi.getPushStatus.mockResolvedValue({
      supported: true,
      subscribed: true,
    })

    const { result, rerender } = renderHook(
      ({ mounted }: { mounted: boolean }) =>
        usePushSubscription({
          customWatchlists,
          mounted,
        }),
      { initialProps: { mounted: false } }
    )

    await waitFor(() => expect(result.current.isPushScopeEnabled("wl-50")).toBe(true))
    rerender({ mounted: true })
    await waitFor(() =>
      expect(result.current.pushScopeSelectionError).toBe(
        `Only ${PUSH_SCOPE_LIMITS.watchlists} custom watchlists can use push at once.`
      )
    )
    await waitFor(() => expect(result.current.isPushScopeEnabled("wl-50")).toBe(false))
    expect(result.current.isPushScopeEnabled("wl-0")).toBe(true)
    expect(customWatchlists).toHaveLength(PUSH_SCOPE_LIMITS.watchlists + 1)

    await waitFor(() =>
      expect(mockedPushApi.syncPushScopes.mock.calls.some(
        ([scopes]) => scopes.watchlists.length === PUSH_SCOPE_LIMITS.watchlists
      )).toBe(true)
    )
    expect(mockedPushApi.syncPushScopes.mock.calls.every(
      ([scopes]) => scopes.watchlists.length <= PUSH_SCOPE_LIMITS.watchlists
    )).toBe(true)
  })

  it("drops queued scope changes after unsubscribing", async () => {
    const inFlightSync = deferred<boolean>()
    const customWatchlists: Watchlist[] = [
      { id: "wl-1", name: "First", tickers: ["GOF"] },
    ]

    mockedPushApi.isPushSupported.mockReturnValue(true)
    mockedPushApi.getPushStatus.mockResolvedValue({
      supported: true,
      subscribed: true,
    })
    mockedPushApi.syncPushScopes.mockImplementationOnce(() => inFlightSync.promise)
    mockedPushApi.disablePushNotifications.mockResolvedValue()

    const { result } = renderHook(() =>
      usePushSubscription({
        customWatchlists,
        mounted: true,
      })
    )

    await waitFor(() => expect(mockedPushApi.syncPushScopes).toHaveBeenCalledTimes(1))

    act(() => {
      result.current.togglePushScope("wl-1")
    })
    await waitFor(() => expect(result.current.isPushScopeEnabled("wl-1")).toBe(true))

    let disablePromise: Promise<void> | undefined
    act(() => {
      disablePromise = result.current.togglePushSubscription()
    })
    expect(mockedPushApi.disablePushNotifications).not.toHaveBeenCalled()

    await act(async () => {
      inFlightSync.resolve(true)
      await inFlightSync.promise
      await disablePromise
    })
    expect(result.current.pushSubscribed).toBe(false)
    expect(mockedPushApi.disablePushNotifications).toHaveBeenCalledTimes(1)
    expect(mockedPushApi.syncPushScopes).toHaveBeenCalledTimes(1)
  })

  it("waits for an in-flight PUT before starting DELETE and does not retry it", async () => {
    const inFlightPut = deferred<boolean>()
    const inFlightDelete = deferred<void>()
    mockedPushApi.isPushSupported.mockReturnValue(true)
    mockedPushApi.getPushStatus.mockResolvedValue({
      supported: true,
      subscribed: true,
    })
    mockedPushApi.syncPushScopes.mockImplementationOnce(() => inFlightPut.promise)
    mockedPushApi.disablePushNotifications.mockImplementationOnce(
      () => inFlightDelete.promise
    )

    const { result } = renderHook(() =>
      usePushSubscription({
        customWatchlists: [],
        mounted: true,
      })
    )

    await waitFor(() => expect(mockedPushApi.syncPushScopes).toHaveBeenCalledTimes(1))

    let disablePromise: Promise<void> | undefined
    act(() => {
      disablePromise = result.current.togglePushSubscription()
    })
    expect(mockedPushApi.disablePushNotifications).not.toHaveBeenCalled()

    await act(async () => {
      inFlightPut.reject(new Error("PUT failed before DELETE"))
      await inFlightPut.promise.catch(() => undefined)
    })
    await waitFor(() =>
      expect(mockedPushApi.disablePushNotifications).toHaveBeenCalledTimes(1)
    )

    await act(async () => {
      inFlightDelete.resolve()
      await disablePromise
    })
    expect(result.current.pushSubscribed).toBe(false)
    expect(mockedPushApi.syncPushScopes).toHaveBeenCalledTimes(1)
  })

  it("allows only one concurrent disable transition when outcomes would conflict", async () => {
    const inFlightDelete = deferred<void>()
    const customWatchlists: Watchlist[] = []
    mockedPushApi.isPushSupported.mockReturnValue(true)
    mockedPushApi.getPushStatus.mockResolvedValue({
      supported: true,
      subscribed: true,
    })
    mockedPushApi.disablePushNotifications
      .mockImplementationOnce(() => inFlightDelete.promise)
      .mockRejectedValueOnce(new Error("conflicting DELETE failed"))

    const { result } = renderHook(() =>
      usePushSubscription({
        customWatchlists,
        mounted: true,
      })
    )

    await waitFor(() => expect(mockedPushApi.syncPushScopes).toHaveBeenCalledTimes(1))

    let firstTransition: Promise<void> | undefined
    let secondTransition: Promise<void> | undefined
    act(() => {
      firstTransition = result.current.togglePushSubscription()
      secondTransition = result.current.togglePushSubscription()
    })

    await waitFor(() =>
      expect(mockedPushApi.disablePushNotifications).toHaveBeenCalledTimes(1)
    )
    expect(result.current.pushSubscribed).toBe(true)

    await act(async () => {
      inFlightDelete.resolve()
      await Promise.all([firstTransition, secondTransition])
    })

    expect(result.current.pushSubscribed).toBe(false)
    expect(result.current.pushError).toBeNull()
    expect(mockedPushApi.disablePushNotifications).toHaveBeenCalledTimes(1)
    expect(mockedPushApi.syncPushScopes).toHaveBeenCalledTimes(1)
  })

  it("restores the latest scopes when disabling push fails", async () => {
    const inFlightDelete = deferred<void>()
    const customWatchlists: Watchlist[] = [
      { id: "wl-1", name: "First", tickers: ["GOF"] },
    ]
    mockedPushApi.isPushSupported.mockReturnValue(true)
    mockedPushApi.getPushStatus.mockResolvedValue({
      supported: true,
      subscribed: true,
    })
    mockedPushApi.disablePushNotifications.mockImplementationOnce(
      () => inFlightDelete.promise
    )

    const { result } = renderHook(() =>
      usePushSubscription({
        customWatchlists,
        mounted: true,
      })
    )

    await waitFor(() => expect(mockedPushApi.syncPushScopes).toHaveBeenCalledTimes(1))
    let disablePromise: Promise<void> | undefined
    act(() => {
      disablePromise = result.current.togglePushSubscription()
    })
    await waitFor(() =>
      expect(mockedPushApi.disablePushNotifications).toHaveBeenCalledTimes(1)
    )

    // Scope changes during DELETE are not queued while sync is inactive, but
    // the latest value must be used if DELETE fails and synchronization resumes.
    act(() => {
      result.current.togglePushScope("wl-1")
    })
    await waitFor(() => expect(result.current.isPushScopeEnabled("wl-1")).toBe(true))

    await act(async () => {
      inFlightDelete.reject(new Error("DELETE failed"))
      await disablePromise
    })

    expect(result.current.pushSubscribed).toBe(true)
    expect(result.current.pushError).toBe("Failed to disable push notifications")
    await waitFor(() =>
      expect(mockedPushApi.syncPushScopes).toHaveBeenCalledWith({
        include_all_news: true,
        watchlists: [
          {
            id: "wl-1",
            name: "First",
            tickers: ["GOF"],
            provider: undefined,
            q: undefined,
          },
        ],
      })
    )
  })
})
