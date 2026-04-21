import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react"
import { beforeEach, describe, expect, it, vi } from "vitest"

import Page from "./page"
import * as api from "@/lib/api"
import * as localStorageApi from "@/lib/local-storage"

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
const mockedLocalStorage = vi.mocked(localStorageApi)

function makeNewsItem({
  id,
  provider,
  readKey,
  source,
  tickers,
  title,
  summary = null,
}: {
  id: number
  provider: string
  readKey: string
  source: string
  tickers: string[]
  title: string
  summary?: string | null
}) {
  return {
    id,
    read_key: readKey,
    title,
    url: `https://example.com/${id}`,
    source,
    provider,
    summary,
    published_at: "2026-04-21T06:00:00Z",
    first_seen_at: "2026-04-21T06:01:00Z",
    alert_sent_at: null,
    tickers,
    dedupe_group: `group-${id}`,
  }
}

function matchNewsIdsForReadQuery(params: {
  tickers?: string[]
  provider?: string
  includeUnmappedFromProvider?: string
  q?: string
}) {
  const items = [
    makeNewsItem({
      id: 301,
      readKey: "key-gof",
      title: "GOF merger update",
      source: "Example",
      provider: "Yahoo Finance",
      tickers: ["GOF"],
    }),
    makeNewsItem({
      id: 302,
      readKey: "key-utf",
      title: "UTF routine update",
      source: "Example",
      provider: "Business Wire",
      tickers: ["UTF"],
    }),
    makeNewsItem({
      id: 303,
      readKey: "key-bme",
      title: "BME distribution notice",
      source: "Example",
      provider: "PR Newswire",
      tickers: ["BME"],
    }),
  ]

  return items.filter((item) => {
    if (params.tickers && params.tickers.length > 0 && !item.tickers.some((ticker) => params.tickers?.includes(ticker))) {
      return false
    }

    if (params.provider && item.provider !== params.provider) {
      return false
    }

    if (
      params.includeUnmappedFromProvider &&
      item.tickers.length === 0 &&
      item.provider !== params.includeUnmappedFromProvider
    ) {
      return false
    }

    if (params.q) {
      const haystack = [item.title, item.summary, item.source, item.provider, ...item.tickers]
        .join(" ")
        .toLowerCase()
      if (!haystack.includes(params.q.toLowerCase())) {
        return false
      }
    }

    return true
  })
}

type ReadQueryMockParams = Parameters<typeof matchNewsIdsForReadQuery>[0]

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
        tracked_read_keys: ["key-101", "key-102"],
        tracked_limit: 100,
      },
    })
    mockedApi.fetchNewsIds.mockResolvedValue({
      ids: [],
      read_keys: [],
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

  it("does not mark a row read when only expanding its summary", async () => {
    mockedApi.fetchNews.mockResolvedValue({
      items: [
        {
          id: 201,
          read_key: "fresh-article-key",
          title: "Fresh article",
          url: "https://example.com/fresh",
          source: "Example",
          provider: "Business Wire",
          summary: "Inline summary",
          published_at: "2026-04-21T06:00:00Z",
          first_seen_at: "2026-04-21T06:01:00Z",
          alert_sent_at: null,
          tickers: [],
          dedupe_group: "fresh-group",
        },
      ],
      next_cursor: null,
      meta: {
        count: 1,
        limit: 40,
        sort: "latest",
      },
      global_summary: {
        total: 1,
        tracked_ids: [201],
        tracked_read_keys: ["fresh-article-key"],
        tracked_limit: 100,
      },
    })

    render(<Page />)

    const link = await screen.findByRole("link", { name: "Fresh article" })
    const row = link.closest(".feed-row")
    const article = link.closest("article")

    expect(row).not.toBeNull()
    expect(article).not.toBeNull()
    expect(article).toHaveClass("unread")

    fireEvent.click(row as HTMLElement)

    expect(await screen.findByText("Inline summary")).toBeInTheDocument()
    expect(article).toHaveClass("unread")

    fireEvent.click(link)

    await waitFor(() => expect(article).toHaveClass("read"))
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

  it("keeps seeded read preferences from being overwritten on mount", async () => {
    localStorage.setItem("readNewsKeys", JSON.stringify(["seed-read-key"]))
    localStorage.setItem("newsViewMode", "full")
    localStorage.setItem(
      "customWatchlists",
      JSON.stringify([
        {
          id: "seed-watchlist",
          name: "Seed Watchlist",
          provider: "Business Wire",
          q: "rights offering",
          tickers: [],
        },
      ])
    )

    render(<Page />)

    await waitFor(() => expect(screen.getByTitle("Refresh Data")).toBeInTheDocument())

    expect(mockedLocalStorage.persistJson).not.toHaveBeenCalledWith("readNewsKeys", [])
    expect(mockedLocalStorage.persistValue).not.toHaveBeenCalledWith("newsViewMode", "list")
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

  it("keeps seeded custom watchlists from being overwritten on mount", async () => {
    localStorage.setItem("readNewsKeys", JSON.stringify(["seed-read-key"]))
    localStorage.setItem("newsViewMode", "full")
    localStorage.setItem(
      "customWatchlists",
      JSON.stringify([
        {
          id: "seed-watchlist",
          name: "Seed Watchlist",
          provider: "Business Wire",
          q: "rights offering",
          tickers: [],
        },
      ])
    )

    render(<Page />)

    await waitFor(() => expect(screen.getByText("Seed Watchlist")).toBeInTheDocument())

    expect(mockedLocalStorage.persistJson).not.toHaveBeenCalledWith("customWatchlists", [])
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

  it("marks only the active watchlist ticker when the view is narrowed by ticker", async () => {
    localStorage.setItem(
      "customWatchlists",
      JSON.stringify([
        {
          id: "ticker-watchlist",
          name: "Ticker Watchlist",
          tickers: ["UTF", "BME"],
        },
      ])
    )

    mockedApi.fetchNews.mockResolvedValue({
      items: [
        makeNewsItem({
          id: 301,
          readKey: "key-gof",
          title: "GOF merger update",
          source: "Example",
          provider: "Yahoo Finance",
          tickers: ["GOF"],
        }),
        makeNewsItem({
          id: 302,
          readKey: "key-utf",
          title: "UTF routine update",
          source: "Example",
          provider: "Business Wire",
          tickers: ["UTF"],
        }),
        makeNewsItem({
          id: 303,
          readKey: "key-bme",
          title: "BME distribution notice",
          source: "Example",
          provider: "PR Newswire",
          tickers: ["BME"],
        }),
      ],
      next_cursor: null,
      meta: {
        count: 3,
        limit: 40,
        sort: "latest",
      },
      global_summary: {
        total: 3,
        tracked_ids: [301, 302, 303],
        tracked_read_keys: ["key-gof", "key-utf", "key-bme"],
        tracked_limit: 100,
      },
    })
    mockedApi.fetchNewsIds.mockImplementation(async (params: ReadQueryMockParams) => ({
      ids: matchNewsIdsForReadQuery(params).map((item) => item.id),
      read_keys: matchNewsIdsForReadQuery(params).map((item) => item.read_key),
      next_cursor: null,
    }))

    render(<Page />)

    await waitFor(() => expect(screen.getByText("Ticker Watchlist")).toBeInTheDocument())
    fireEvent.click(screen.getByText("Ticker Watchlist"))

    await waitFor(() =>
      expect(mockedApi.fetchNews).toHaveBeenLastCalledWith(
        expect.objectContaining({
          tickers: ["UTF", "BME"],
          provider: undefined,
          q: undefined,
          includeUnmappedFromProvider: undefined,
          includeGlobalSummary: true,
          limit: 40,
        })
      )
    )

    fireEvent.change(screen.getAllByRole("combobox")[0], {
      target: { value: "GOF" },
    })

    await waitFor(() =>
      expect(mockedApi.fetchNews).toHaveBeenLastCalledWith(
        expect.objectContaining({
          tickers: ["GOF"],
          provider: undefined,
          q: undefined,
          includeUnmappedFromProvider: undefined,
          includeGlobalSummary: true,
          limit: 40,
        })
      )
    )

    fireEvent.contextMenu(screen.getByText("Ticker Watchlist"))
    await waitFor(() => expect(screen.getByText("Mark All Items as Read")).toBeInTheDocument())
    fireEvent.click(screen.getByText("Mark All Items as Read"))

    await waitFor(() =>
      expect(mockedApi.fetchNewsIds).toHaveBeenLastCalledWith(
        expect.objectContaining({
          tickers: ["GOF"],
          limit: 1000,
        })
      )
    )

    expect(screen.getByRole("link", { name: "GOF merger update" }).closest("article")).toHaveClass("read")
    expect(screen.getByRole("link", { name: "UTF routine update" }).closest("article")).toHaveClass("unread")
    expect(screen.getByRole("link", { name: "BME distribution notice" }).closest("article")).toHaveClass("unread")
  })

  it("respects active all-news ticker, provider, and search filters when marking all read", async () => {
    mockedApi.fetchNews.mockResolvedValue({
      items: [
        makeNewsItem({
          id: 401,
          readKey: "key-gof",
          title: "GOF merger update",
          source: "Example",
          provider: "Yahoo Finance",
          tickers: ["GOF"],
        }),
        makeNewsItem({
          id: 402,
          readKey: "key-utf",
          title: "UTF routine update",
          source: "Example",
          provider: "Business Wire",
          tickers: ["UTF"],
        }),
        makeNewsItem({
          id: 403,
          readKey: "key-bme",
          title: "BME merger update",
          source: "Example",
          provider: "Yahoo Finance",
          tickers: ["BME"],
        }),
      ],
      next_cursor: null,
      meta: {
        count: 3,
        limit: 40,
        sort: "latest",
      },
      global_summary: {
        total: 3,
        tracked_ids: [401, 402, 403],
        tracked_read_keys: ["key-gof", "key-utf", "key-bme"],
        tracked_limit: 100,
      },
    })
    mockedApi.fetchNewsIds.mockImplementation(async (params: ReadQueryMockParams) => ({
      ids: matchNewsIdsForReadQuery(params).map((item) => item.id),
      read_keys: matchNewsIdsForReadQuery(params).map((item) => item.read_key),
      next_cursor: null,
    }))

    render(<Page />)

    await waitFor(() => expect(screen.getByText("All News")).toBeInTheDocument())
    fireEvent.change(screen.getAllByRole("combobox")[0], {
      target: { value: "GOF" },
    })
    fireEvent.change(screen.getAllByRole("combobox")[1], {
      target: { value: "Yahoo Finance" },
    })
    fireEvent.change(screen.getByPlaceholderText("Search news..."), {
      target: { value: "merger" },
    })
    fireEvent.click(screen.getByRole("button", { name: "Search" }))

    await waitFor(() =>
      expect(mockedApi.fetchNews).toHaveBeenLastCalledWith(
        expect.objectContaining({
          tickers: ["GOF"],
          provider: "Yahoo Finance",
          q: "merger",
          includeUnmappedFromProvider: undefined,
          includeGlobalSummary: true,
          limit: 40,
        })
      )
    )

    fireEvent.contextMenu(screen.getByText("All News"))
    await waitFor(() => expect(screen.getByText("Mark All Items as Read")).toBeInTheDocument())
    fireEvent.click(screen.getByText("Mark All Items as Read"))

    await waitFor(() =>
      expect(mockedApi.fetchNewsIds).toHaveBeenLastCalledWith(
        expect.objectContaining({
          tickers: ["GOF"],
          provider: "Yahoo Finance",
          q: "merger",
          limit: 1000,
        })
      )
    )

    expect(screen.getByRole("link", { name: "GOF merger update" }).closest("article")).toHaveClass("read")
    expect(screen.getByRole("link", { name: "UTF routine update" }).closest("article")).toHaveClass("unread")
    expect(screen.getByRole("link", { name: "BME merger update" }).closest("article")).toHaveClass("unread")
  })

  it("keeps the Business Wire unmapped include when marking provider/query-only all-news views", async () => {
    mockedApi.fetchNews.mockResolvedValue({
      items: [
        makeNewsItem({
          id: 451,
          readKey: "key-bw",
          title: "Business Wire rights offering",
          source: "Example",
          provider: "Business Wire",
          tickers: [],
        }),
        makeNewsItem({
          id: 452,
          readKey: "key-gof",
          title: "GOF rights offering",
          source: "Example",
          provider: "Business Wire",
          tickers: ["GOF"],
        }),
      ],
      next_cursor: null,
      meta: {
        count: 2,
        limit: 40,
        sort: "latest",
      },
      global_summary: {
        total: 2,
        tracked_ids: [451, 452],
        tracked_read_keys: ["key-bw", "key-gof"],
        tracked_limit: 100,
      },
    })
    mockedApi.fetchNewsIds.mockImplementation(async (params: ReadQueryMockParams) => ({
      ids: matchNewsIdsForReadQuery(params).map((item) => item.id),
      read_keys: matchNewsIdsForReadQuery(params).map((item) => item.read_key),
      next_cursor: null,
    }))

    render(<Page />)

    await waitFor(() => expect(screen.getByText("All News")).toBeInTheDocument())
    fireEvent.change(screen.getAllByRole("combobox")[1], {
      target: { value: "Business Wire" },
    })
    fireEvent.change(screen.getByPlaceholderText("Search news..."), {
      target: { value: "rights" },
    })
    fireEvent.click(screen.getByRole("button", { name: "Search" }))

    await waitFor(() =>
      expect(mockedApi.fetchNews).toHaveBeenLastCalledWith(
        expect.objectContaining({
          provider: "Business Wire",
          q: "rights",
          includeUnmappedFromProvider: "Business Wire",
          includeGlobalSummary: true,
          limit: 40,
        })
      )
    )

    fireEvent.contextMenu(screen.getByText("All News"))
    await waitFor(() => expect(screen.getByText("Mark All Items as Read")).toBeInTheDocument())
    fireEvent.click(screen.getByText("Mark All Items as Read"))

    await waitFor(() =>
      expect(mockedApi.fetchNewsIds).toHaveBeenLastCalledWith(
        expect.objectContaining({
          provider: "Business Wire",
          q: "rights",
          includeUnmappedFromProvider: "Business Wire",
          limit: 1000,
        })
      )
    )
  })

  it("fallback marks mapped non-Business-Wire items in the default all-news view", async () => {
    mockedApi.fetchNews.mockResolvedValue({
      items: [
        makeNewsItem({
          id: 501,
          readKey: "key-gof",
          title: "GOF mapped Yahoo update",
          source: "Example",
          provider: "Yahoo Finance",
          tickers: ["GOF"],
        }),
        makeNewsItem({
          id: 502,
          readKey: "key-bw",
          title: "Business Wire general update",
          source: "Example",
          provider: "Business Wire",
          tickers: [],
        }),
        makeNewsItem({
          id: 503,
          readKey: "key-prn",
          title: "PRN general update",
          source: "Example",
          provider: "PR Newswire",
          tickers: [],
        }),
      ],
      next_cursor: null,
      meta: {
        count: 3,
        limit: 40,
        sort: "latest",
      },
      global_summary: {
        total: 3,
        tracked_ids: [501, 502, 503],
        tracked_read_keys: ["key-gof", "key-bw", "key-prn"],
        tracked_limit: 100,
      },
    })
    mockedApi.fetchNewsIds.mockRejectedValue(new Error("ids unavailable"))

    render(<Page />)

    await waitFor(() => expect(screen.getByText("All News")).toBeInTheDocument())
    fireEvent.contextMenu(screen.getByText("All News"))
    await waitFor(() => expect(screen.getByText("Mark All Items as Read")).toBeInTheDocument())
    fireEvent.click(screen.getByText("Mark All Items as Read"))

    await waitFor(() =>
      expect(mockedApi.fetchNewsIds).toHaveBeenLastCalledWith(
        expect.objectContaining({
          includeUnmappedFromProvider: "Business Wire",
          limit: 1000,
        })
      )
    )

    expect(screen.getByRole("link", { name: "GOF mapped Yahoo update" }).closest("article")).toHaveClass("read")
    expect(screen.getByRole("link", { name: "Business Wire general update" }).closest("article")).toHaveClass("read")
    expect(screen.getByRole("link", { name: "PRN general update" }).closest("article")).toHaveClass("unread")
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
