import { createRef } from "react"
import { cleanup, fireEvent, render, screen } from "@testing-library/react"
import { afterEach, expect, it, vi } from "vitest"
import { NewsFeedSection } from "./news-feed-section"
import type { NewsItem } from "@/lib/types"

afterEach(() => { cleanup(); vi.unstubAllGlobals() })

it("uses the original publisher for headlines and opens the reader directly while preserving provider attribution", async () => {
  const original = "https://www.businesswire.com/news/home/20261106529638/en/"
  const item: NewsItem = { id: 1714, title: "ClearBridge statement", url: "https://markets.chroniclejournal.com/thepilotnews/article/bizwire-2026-11-6-clearbridge",
    source: "Business Wire", provider: "FinancialContent", summary: "View source version on businesswire.com: " + original,
    published_at: "2026-11-06T13:00:00Z", first_seen_at: "2026-11-06T13:00:39Z", alert_sent_at: null, read_key: "r", dedupe_group: "g", tickers: ["EMO"] }
  const open = vi.fn((_item, event) => { event.preventDefault(); event.stopPropagation() })
  const toggle = vi.fn()
  const markRead = vi.fn()
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: false }))
  render(<NewsFeedSection items={[item]} expandedIds={new Set()} feedContainerRef={createRef()} loadMore={vi.fn()}
    loadPendingArticles={vi.fn()} loading={false} loadingMore={false} markAsRead={markRead} markAsReadAndOpen={open} nextCursor={null}
    pendingNewItems={[]} readKeys={new Set()} toggleRead={vi.fn()} toggleSummary={toggle} viewMode="full" />)
  const headline = screen.getByRole("link", { name: item.title })
  expect(headline).toHaveAttribute("href", original)
  expect(screen.getByText("FinancialContent")).toBeInTheDocument()
  fireEvent.click(headline)
  expect(open).toHaveBeenCalledWith(expect.objectContaining({ url: original, id: 1714 }), expect.anything())
  fireEvent.click(screen.getByRole("button", { name: "Read in app: " + item.title }))
  expect(await screen.findByRole("dialog")).toBeInTheDocument()
  expect(markRead).toHaveBeenCalledWith(item)
  fireEvent.click(screen.getByRole("button", { name: "Close article" }))
  fireEvent.click(screen.getByRole("button", { name: "Read in app" }))
  expect(await screen.findByRole("dialog")).toBeInTheDocument()
  expect(markRead).toHaveBeenCalledTimes(2)
  expect(open).toHaveBeenCalledTimes(1)
  expect(toggle).not.toHaveBeenCalled()
  expect(item.url).toContain("chroniclejournal")
})
