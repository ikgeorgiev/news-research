import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react"
import { beforeEach, describe, expect, it, vi } from "vitest"

import Page from "./page"
import * as api from "@/lib/api"

vi.mock("@/lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api")>()
  return {
    ...actual,
    fetchNews: vi.fn(),
    fetchNewsIds: vi.fn(),
    fetchTickers: vi.fn(),
  }
})

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

vi.mock("@/lib/local-storage", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/local-storage")>()
  return {
    ...actual,
    persistJson: vi.fn(),
    persistValue: vi.fn(),
  }
})

const mockedApi = vi.mocked(api)

describe("Page refresh requests", () => {
  beforeEach(() => {
    cleanup()
    localStorage.clear()
    vi.clearAllMocks()

    mockedApi.fetchTickers.mockResolvedValue({
      items: [
        { symbol: "UTF", fund_name: null, sponsor: null, active: true },
        { symbol: "BME", fund_name: null, sponsor: null, active: true },
        { symbol: "GOF", fund_name: null, sponsor: null, active: true },
      ],
      total: 3,
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
          includeUnmappedFromProvider: "Business Wire",
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
          includeUnmappedFromProvider: "Business Wire",
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
          includeUnmappedFromProvider: "Business Wire",
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
          includeUnmappedFromProvider: "Business Wire",
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
          includeUnmappedFromProvider: "Business Wire",
          includeGlobalSummary: true,
          limit: 40,
        })
      )
    )
    expect(mockedApi.fetchNews.mock.lastCall?.[0].q).toBeUndefined()
  })

  it("lets an explicit ticker narrow a watchlist instead of broadening it", async () => {
    localStorage.setItem(
      "customWatchlists",
      JSON.stringify([
        {
          id: "ticker-watchlist",
          name: "Ticker Watchlist",
          provider: "Business Wire",
          q: "rights offering",
          tickers: ["UTF", "BME"],
        },
      ])
    )

    render(<Page />)

    await waitFor(() => expect(screen.getByText("Ticker Watchlist")).toBeInTheDocument())
    fireEvent.click(screen.getByText("Ticker Watchlist"))

    await waitFor(() =>
      expect(mockedApi.fetchNews).toHaveBeenLastCalledWith(
        expect.objectContaining({
          tickers: ["UTF", "BME"],
          provider: "Business Wire",
          q: "rights offering",
          includeUnmappedFromProvider: undefined,
          includeGlobalSummary: true,
          limit: 40,
        })
      )
    )

    const tickerSelect = screen.getAllByRole("combobox")[0]
    fireEvent.change(tickerSelect, { target: { value: "GOF" } })

    await waitFor(() =>
      expect(mockedApi.fetchNews).toHaveBeenLastCalledWith(
        expect.objectContaining({
          tickers: ["GOF"],
          provider: "Business Wire",
          q: "rights offering",
          includeUnmappedFromProvider: undefined,
          includeGlobalSummary: true,
          limit: 40,
        })
      )
    )
  })

  it("can create a provider/query-only watchlist from the sidebar form", async () => {
    render(<Page />)

    await waitFor(() => expect(screen.getByTitle("New Watchlist")).toBeInTheDocument())
    fireEvent.click(screen.getByTitle("New Watchlist"))

    fireEvent.change(screen.getByPlaceholderText("Watchlist Name"), {
      target: { value: "BW Specials" },
    })
    fireEvent.change(screen.getByLabelText("Watchlist Provider"), {
      target: { value: "Business Wire" },
    })
    fireEvent.change(screen.getByLabelText("Watchlist Query"), {
      target: { value: "rights offering" },
    })
    fireEvent.click(screen.getByRole("button", { name: "Save" }))

    await waitFor(() => expect(screen.getByText("BW Specials")).toBeInTheDocument())
    await waitFor(() =>
      expect(mockedApi.fetchNews).toHaveBeenLastCalledWith(
        expect.objectContaining({
          provider: "Business Wire",
          q: "rights offering",
          includeUnmappedFromProvider: "Business Wire",
          includeGlobalSummary: true,
          limit: 40,
        })
      )
    )
  })
})
