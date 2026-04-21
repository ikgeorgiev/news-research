import { render, screen, waitFor } from "@testing-library/react"
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

const mockedApi = vi.mocked(api)

describe("ticker selector errors", () => {
  beforeEach(() => {
    localStorage.clear()
    vi.clearAllMocks()

    mockedApi.fetchNews.mockResolvedValue({
      items: [],
      next_cursor: null,
      meta: {
        count: 0,
        limit: 40,
        sort: "latest",
      },
      global_summary: {
        total: 0,
        tracked_ids: [],
        tracked_read_keys: [],
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

  it("keeps the feed usable and shows a ticker hint when ticker loading fails", async () => {
    mockedApi.fetchTickers.mockRejectedValue(new Error("ticker API unavailable"))

    render(<Page />)

    await waitFor(() => expect(mockedApi.fetchNews).toHaveBeenCalled())
    expect(await screen.findByText("Ticker list unavailable")).toBeInTheDocument()
    expect(screen.getAllByRole("combobox")[0]).toHaveValue("")
  })
})
