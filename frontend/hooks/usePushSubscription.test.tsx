import { act, renderHook, waitFor } from "@testing-library/react"
import { beforeEach, describe, expect, it, vi } from "vitest"

import { usePushSubscription } from "./usePushSubscription"
import * as pushApi from "@/lib/push"

vi.mock("@/lib/push", () => ({
  disablePushNotifications: vi.fn(),
  enablePushNotifications: vi.fn(),
  getPushStatus: vi.fn(),
  isPushSupported: vi.fn(),
  syncPushScopes: vi.fn(),
}))

const mockedPushApi = vi.mocked(pushApi)

describe("usePushSubscription", () => {
  beforeEach(() => {
    localStorage.clear()
    vi.clearAllMocks()
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
    expect(localStorage.getItem("pushSubscribed")).toBe("true")
  })

  it("stays unsubscribed when push is unavailable", async () => {
    mockedPushApi.isPushSupported.mockReturnValue(false)

    const { result } = renderHook(() =>
      usePushSubscription({
        customWatchlists: [],
        mounted: true,
      })
    )

    await waitFor(() => expect(localStorage.getItem("pushSubscribed")).toBe("false"))
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

  it("preserves stored custom scope ids until watchlists hydrate", async () => {
    localStorage.setItem("alertWatchlistIds", JSON.stringify(["wl-1"]))
    localStorage.setItem("alertIncludeAllNews", "false")

    mockedPushApi.isPushSupported.mockReturnValue(true)
    mockedPushApi.getPushStatus.mockResolvedValue({
      supported: true,
      subscribed: true,
    })
    mockedPushApi.syncPushScopes.mockResolvedValue(true)

    const { result, rerender } = renderHook(
      ({ customWatchlists, mounted }: { customWatchlists: Array<{ id: string; name: string; tickers?: string[] }>; mounted: boolean }) =>
        usePushSubscription({
          customWatchlists,
          mounted,
        }),
      {
        initialProps: {
          customWatchlists: [],
          mounted: false,
        },
      }
    )

    await waitFor(() => expect(result.current.pushSubscribed).toBe(true))
    expect(result.current.isPushScopeEnabled("wl-1")).toBe(true)
    expect(localStorage.getItem("alertWatchlistIds")).toBe(JSON.stringify(["wl-1"]))
    expect(mockedPushApi.syncPushScopes).not.toHaveBeenCalled()

    rerender({
      customWatchlists: [
        {
          id: "wl-1",
          name: "Watchlist 1",
          tickers: ["GOF"],
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
            name: "Watchlist 1",
            tickers: ["GOF"],
            provider: undefined,
            q: undefined,
          },
        ],
      })
    )
    expect(result.current.isPushScopeEnabled("wl-1")).toBe(true)
    expect(localStorage.getItem("alertWatchlistIds")).toBe(JSON.stringify(["wl-1"]))
  })
})
