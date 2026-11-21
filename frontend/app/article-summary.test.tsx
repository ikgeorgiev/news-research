import { cleanup, fireEvent, render, screen } from "@testing-library/react"
import { afterEach, describe, expect, it, vi } from "vitest"

import { NewsItem } from "@/lib/types"
import { ArticleSummary } from "./article-summary"

afterEach(cleanup)

const base: NewsItem = {
  id: 1634,
  read_key: "blackrock-1634",
  title: "Certain BlackRock Closed-End Funds Announce Estimated Sources of Distributions",
  url: "https://markets.chroniclejournal.com/thepilotnews/article/bizwire-2026-10-30-certain-blackrock-closed-end-funds-announce-estimated-sources-of-distributions",
  source: "Business Wire",
  provider: "FinancialContent",
  summary: null,
  published_at: "2026-10-30T20:30:00Z",
  first_seen_at: "2026-10-30T20:30:11Z",
  alert_sent_at: "2026-10-30T20:30:12Z",
  tickers: ["BCX", "HYT"],
  dedupe_group: "bw:20261030641475:en",
}

describe("article summary previews", () => {
  it("bounds a full FinancialContent body without changing evidence or ticker data", () => {
    const fullBody = "BlackRock funds announced distributions. ".repeat(440) + "HYT final table row"
    const item = { ...base, summary: fullBody }
    const before = JSON.stringify(item)
    const openArticle = vi.fn()
    const { container } = render(<ArticleSummary item={item} openArticle={openArticle} openReader={vi.fn()} />)

    const preview = container.querySelector(".summary-text")!
    expect(preview.textContent!.length).toBeLessThanOrEqual(601)
    expect(preview.textContent).toMatch(/…$/)
    expect(fullBody.startsWith(preview.textContent!.slice(0, -1))).toBe(true)
    expect(fullBody[preview.textContent!.length - 1]).toMatch(/\s/)
    expect(screen.queryByText(/HYT final table row/)).not.toBeInTheDocument()
    const link = screen.getByRole("link", { name: `Open FinancialContent: ${base.title}` })
    expect(link).toHaveAttribute("href", base.url)
    expect(link).toHaveAttribute("target", "_blank")
    expect(openArticle).not.toHaveBeenCalled()
    fireEvent.click(link)
    expect(openArticle).toHaveBeenCalledWith(item, expect.anything())
    expect(JSON.stringify(item)).toBe(before)
  })

  it("keeps an ordinary RSS preview intact with an explicit source link", () => {
    const summary = "NEW YORK--(BUSINESS WIRE)--The Fund announced its distribution."
    render(<ArticleSummary item={{ ...base, provider: "Business Wire", summary }} openArticle={vi.fn()} openReader={vi.fn()} />)
    expect(screen.getByText(summary)).toBeInTheDocument()
    expect(screen.getByRole("link", { name: `Open Business Wire: ${base.title}` })).toHaveAttribute("href", base.url)
  })

  it("renders untrusted text safely and does not expose unsafe article URLs", () => {
    const { container } = render(<ArticleSummary
      item={{ ...base, url: "javascript:alert(1)", summary: '<img src=x onerror="alert(1)"> '.repeat(40) }}
      openArticle={vi.fn()} openReader={vi.fn()}
    />)
    expect(container.querySelector("img")).toBeNull()
    expect(screen.queryByRole("link")).not.toBeInTheDocument()
    expect(container.textContent).toContain("<img")
  })

  it("can open an archived body even when the feed summary is missing", () => {
    const { container } = render(<ArticleSummary item={base} openArticle={vi.fn()} openReader={vi.fn()} />)
    expect(container.querySelector(".summary-text")).toBeNull()
    expect(screen.getByRole("button", { name: "Read in app" })).toBeInTheDocument()
  })
})
