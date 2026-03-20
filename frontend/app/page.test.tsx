import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react"
import { beforeEach, describe, expect, it, vi } from "vitest"

import Page from "./page"
import * as api from "@/lib/api"

vi.mock("@/lib/api", () => ({
  fetchNews: vi.fn(),
  fetchNewsIds: vi.fn(),
  fetchTickers: vi.fn(),
}))

vi.mock("@/hooks/usePushSubscription", () => ({
  usePushSubscription: vi.fn(() => ({
    alertIncludeAllNews: true,
    activePushScopeCount: 0,
    activePushScopeNames: [],
    isPushScopeEnabled: vi.fn(() => false),
    pushError: null,
    pushSubscribed: false,
    togglePushScope: vi.fn(),
    togglePushSubscription: vi.fn(),
  })),
}))

vi.mock("@/lib/local-storage", () => ({
  persistJson: vi.fn(),
  persistValue: vi.fn(),
}))

const mockedApi = vi.mocked(api)

describe("Page refresh requests", () => {
  beforeEach(() => {
    cleanup()
    localStorage.clear()
    vi.clearAllMocks()

    mockedApi.fetchTickers.mockResolvedValue({
      items: [],
      total: 0,
    })
    mockedApi.fetchNews.mockResolvedValue({
      items: [],
      next_cursor: null,
      meta: {
        count: 0,
        limit: 40,
        sort: "latest",
      },
      global_summary: {
        total: 2,
        tracked_ids: [101, 102],
        tracked_limit: 100,
      },
    })
    mockedApi.fetchNewsIds.mockResolvedValue({
      ids: [],
      next_cursor: null,
    })

    class MockEventSource {
      addEventListener = vi.fn()
      close = vi.fn()
    }

    vi.stubGlobal("EventSource", MockEventSource)
  })

  it("uses only the main news request for initial load and manual refresh", async () => {
    render(<Page />)

    await waitFor(() => expect(mockedApi.fetchNews).toHaveBeenCalledTimes(1))
    expect(mockedApi.fetchNews).toHaveBeenCalledWith(
      expect.objectContaining({
        includeGlobalSummary: true,
        limit: 40,
      })
    )
    await waitFor(() => expect(screen.getByTitle("Refresh Data")).toBeInTheDocument())
    fireEvent.click(screen.getByTitle("Refresh Data"))

    await waitFor(() => expect(mockedApi.fetchNews).toHaveBeenCalledTimes(2))
  })

  it("hydrates legacy provider/query watchlists without dropping their filters", async () => {
    localStorage.setItem(
      "customWatchlists",
      JSON.stringify([
        {
          id: "legacy-provider-watchlist",
          name: "Legacy Provider",
          provider: "Business Wire",
          q: "rights offering",
          tickers: [],
        },
      ])
    )

    render(<Page />)

    await waitFor(() => expect(screen.getByText("Legacy Provider")).toBeInTheDocument())
    fireEvent.click(screen.getByText("Legacy Provider"))

    await waitFor(() =>
      expect(mockedApi.fetchNews).toHaveBeenLastCalledWith(
        expect.objectContaining({
          provider: "Business Wire",
          q: "rights offering",
          includeGlobalSummary: true,
          limit: 40,
        })
      )
    )
  })

  it("lets the main provider control clear a saved watchlist provider", async () => {
    localStorage.setItem(
      "customWatchlists",
      JSON.stringify([
        {
          id: "legacy-provider-watchlist",
          name: "Legacy Provider",
          provider: "Business Wire",
          q: "rights offering",
          tickers: [],
        },
      ])
    )

    render(<Page />)

    await waitFor(() => expect(screen.getByText("Legacy Provider")).toBeInTheDocument())
    fireEvent.click(screen.getByText("Legacy Provider"))

    await waitFor(() =>
      expect(mockedApi.fetchNews).toHaveBeenLastCalledWith(
        expect.objectContaining({
          provider: "Business Wire",
          q: "rights offering",
          includeGlobalSummary: true,
          limit: 40,
        })
      )
    )

    fireEvent.change(screen.getByDisplayValue("Business Wire"), {
      target: { value: "" },
    })

    await waitFor(() =>
      expect(mockedApi.fetchNews).toHaveBeenLastCalledWith(
        expect.objectContaining({
          q: "rights offering",
          includeGlobalSummary: true,
          limit: 40,
        })
      )
    )
    expect(mockedApi.fetchNews.mock.lastCall?.[0].provider).toBeUndefined()
  })

  it("lets the main search control clear a saved watchlist query", async () => {
    localStorage.setItem(
      "customWatchlists",
      JSON.stringify([
        {
          id: "legacy-provider-watchlist",
          name: "Legacy Provider",
          provider: "Business Wire",
          q: "rights offering",
          tickers: [],
        },
      ])
    )

    render(<Page />)

    await waitFor(() => expect(screen.getByText("Legacy Provider")).toBeInTheDocument())
    fireEvent.click(screen.getByText("Legacy Provider"))

    await waitFor(() =>
      expect(mockedApi.fetchNews).toHaveBeenLastCalledWith(
        expect.objectContaining({
          provider: "Business Wire",
          q: "rights offering",
          includeGlobalSummary: true,
          limit: 40,
        })
      )
    )

    fireEvent.change(screen.getByDisplayValue("rights offering"), {
      target: { value: "" },
    })
    fireEvent.click(screen.getByRole("button", { name: "Search" }))

    await waitFor(() =>
      expect(mockedApi.fetchNews).toHaveBeenLastCalledWith(
        expect.objectContaining({
          provider: "Business Wire",
          includeGlobalSummary: true,
          limit: 40,
        })
      )
    )
    expect(mockedApi.fetchNews.mock.lastCall?.[0].q).toBeUndefined()
  })
})
