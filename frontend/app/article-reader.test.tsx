import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react"
import { afterEach, describe, expect, it, vi } from "vitest"
import { ArticleBody, parseArticleBody } from "@/lib/article-body"
import { NewsItem } from "@/lib/types"
import { createRef, type MouseEvent } from "react"
import { NewsFeedSection } from "./news-feed-section"

const item: NewsItem = { id: 1634, read_key: "r", title: "BlackRock distributions", url: "https://example.com/article",
  source: "Business Wire", provider: "FinancialContent", summary: "Original body. ".repeat(100),
  published_at: "2026-10-30T20:30:00Z", first_seen_at: "2026-10-30T20:30:11Z", alert_sent_at: "2026-10-30T20:30:12Z", tickers: ["BCX"], dedupe_group: "g" }
const document: ArticleBody = { version: 1, article_id: 1634, wire_key: "bw:20261030641475:en", title: item.title,
  source_url: item.url, original_url: "https://www.businesswire.com/news/home/20261030641475/en/", published_at: item.published_at,
  blocks: [{ type: "paragraph", text: "A separate opening paragraph." }, { type: "table", caption: "Distributions", rows: [
    [{ text: "Fund", header: true, colspan: 1, rowspan: 1 }, { text: "Per share", header: true, colspan: 1, rowspan: 1 }],
    [{ text: "BCX", header: false, colspan: 1, rowspan: 1 }, { text: "$0.069700", header: false, colspan: 1, rowspan: 1 }],
  ] }, { type: "paragraph", text: '<img src=x onerror="alert(1)">' }] }

afterEach(() => { cleanup(); vi.unstubAllGlobals(); vi.useRealTimers(); localStorage.clear() })

function renderReader(article: NewsItem, openArticle = vi.fn<(item: NewsItem, event: MouseEvent) => void>()) {
  return render(<NewsFeedSection items={[article]} expandedIds={new Set()} feedContainerRef={createRef()}
    loadMore={vi.fn()} loadPendingArticles={vi.fn()} loading={false} loadingMore={false} markAsRead={vi.fn()}
    markAsReadAndOpen={openArticle} nextCursor={null} pendingNewItems={[]} readKeys={new Set()}
    toggleRead={vi.fn()} toggleSummary={vi.fn()} viewMode="full" />)
}

describe("in-app article reader", () => {
  it("fetches only on explicit open, renders separate paragraphs and real cells, and restores focus", async () => {
    const fetch = vi.fn().mockResolvedValue({ ok: true, json: async () => document })
    vi.stubGlobal("fetch", fetch)
    const before = JSON.stringify(item)
    const openArticle = vi.fn()
    renderReader(item, openArticle)
    expect(fetch).not.toHaveBeenCalled()
    const trigger = screen.getByRole("button", { name: "Read in app" })
    trigger.focus(); fireEvent.click(trigger)
    expect(await screen.findByRole("cell", { name: "$0.069700" })).toBeInTheDocument()
    expect(screen.getByText("A separate opening paragraph.").tagName).toBe("P")
    expect(screen.getAllByRole("columnheader")).toHaveLength(2)
    expect(window.document.querySelector("dialog img")).toBeNull()
    expect(screen.getByRole("dialog")).toHaveTextContent('<img src=x onerror="alert(1)">')
    expect(fetch).toHaveBeenCalledWith("/article-body/1634", expect.objectContaining({ signal: expect.any(AbortSignal) }))
    expect(openArticle).not.toHaveBeenCalled()
    expect(JSON.stringify(item)).toBe(before)
    fireEvent.click(screen.getByRole("button", { name: "Close article" }))
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument()
    expect(window.document.activeElement).toBe(trigger)
    expect(window.document.body.style.overflow).not.toBe("hidden")
  })

  it.each(["missing", "wrong identity", "network failure"])("uses the complete saved text when formatted content is %s", async mode => {
    vi.stubGlobal("fetch", mode === "network failure" ? vi.fn().mockRejectedValue(new Error("offline"))
      : vi.fn().mockResolvedValue(mode === "missing" ? { ok: false }
        : { ok: true, json: async () => ({ ...document, article_id: 1623 }) }))
    renderReader(item)
    fireEvent.click(screen.getByRole("button", { name: "Read in app" }))
    await screen.findByText(/Reading the saved text/)
    const paragraphs = [...window.document.querySelectorAll(".reader-content > p")].map(p => p.textContent).join(" ")
    expect(paragraphs).toBe(item.summary!.trim())
    expect(window.document.querySelectorAll(".reader-content > p").length).toBeGreaterThan(1)
    expect(screen.queryByRole("table")).not.toBeInTheDocument()
    fireEvent(screen.getByRole("dialog"), new Event("cancel"))
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument()
  })

  it("keeps late responses from reopening a dismissed reader", async () => {
    let resolve: (value: unknown) => void = () => {}
    vi.stubGlobal("fetch", vi.fn().mockReturnValue(new Promise(r => { resolve = r })))
    renderReader(item)
    fireEvent.click(screen.getByRole("button", { name: "Read in app" }))
    fireEvent.click(screen.getByRole("button", { name: "Close article" }))
    resolve({ ok: true, json: async () => document })
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument())
  })

  it.each(["publisher URL", "publisher citation"])("uses saved text when a same-headline archive conflicts with the %s", async (source) => {
    const original = "https://www.businesswire.com/news/home/20261106529638/en/"
    const release = source === "publisher URL" ? { ...item, url: original }
      : { ...item, summary: item.summary + " View source version on businesswire.com: " + original }
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: true, json: async () => document }))
    renderReader(release)
    fireEvent.click(screen.getByRole("button", { name: "Read in app" }))
    await screen.findByText(/Reading the saved text/)
    expect(screen.queryByText("A separate opening paragraph.")).not.toBeInTheDocument()
    expect(screen.getByRole("link", { name: "Open Business Wire" })).toHaveAttribute("href", original)
  })

  it("uses the verified original publisher, preserves attribution, and remembers reading preferences", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: true, json: async () => document }))
    const openArticle = vi.fn()
    renderReader(item, openArticle)
    fireEvent.click(screen.getByRole("button", { name: "Read in app" }))
    const link = await screen.findByRole("link", { name: "Open Business Wire" })
    expect(link).toHaveAttribute("href", document.original_url)
    expect(screen.getByText("Received via FinancialContent")).toBeInTheDocument()
    fireEvent.click(link)
    expect(openArticle).toHaveBeenCalledWith(expect.objectContaining({ id: item.id, url: document.original_url }), expect.anything())
    fireEvent.click(screen.getByRole("button", { name: "Increase text size" }))
    fireEvent.click(screen.getByRole("button", { name: "Use dark reading theme" }))
    fireEvent.click(screen.getByRole("button", { name: "Close article" }))
    fireEvent.click(screen.getByRole("button", { name: "Read in app" }))
    expect(screen.getByRole("dialog")).toHaveClass("reader-theme-dark")
    expect(window.document.querySelector(".reader-shell")).toHaveStyle({ "--reader-font-size": "20px" })
    expect(screen.getByRole("button", { name: "Use light reading theme" })).toBeInTheDocument()
  })

  it("labels a short RSS item as an excerpt instead of claiming a full article", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: false }))
    renderReader({ ...item, summary: "The fund announced a distribution." })
    fireEvent.click(screen.getByRole("button", { name: "Read in app" }))
    expect(await screen.findByText(/Only a news excerpt/)).toBeInTheDocument()
    expect(screen.getByText("News excerpt")).toBeInTheDocument()
  })

  it("jumps to the chosen table and focuses its scroll area for keyboard readers", async () => {
    const table = document.blocks[1]
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: true, json: async () => ({ ...document,
      blocks: [document.blocks[0], table, { ...table, caption: "Distribution changes" }] }) }))
    renderReader(item)
    fireEvent.click(screen.getByRole("button", { name: "Read in app" }))
    const chooser = await screen.findByRole("combobox", { name: "Jump to article table" })
    fireEvent.change(chooser, { target: { value: "2" } })
    expect(window.document.activeElement).toBe(screen.getByRole("region", { name: "Distribution changes" }))
    expect(screen.getAllByRole("table")).toHaveLength(2)
    expect(screen.getByRole("option", { name: "Table 1: Distributions" })).toBeInTheDocument()
    expect(screen.getByRole("option", { name: "Table 2: Distribution changes" })).toBeInTheDocument()
  })

  it("shows the publication time with the browser's explicit time zone", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: false }))
    renderReader(item)
    fireEvent.click(screen.getByRole("button", { name: "Read in app" }))
    await screen.findByText(/Reading the saved text/)
    const zone = new Intl.DateTimeFormat(undefined, { timeZoneName: "short" })
      .formatToParts(new Date(item.published_at)).find(part => part.type === "timeZoneName")!.value
    expect(window.document.querySelector("dialog time")).toHaveTextContent(zone)
    expect(window.document.querySelector("dialog time")).toHaveAttribute("datetime", item.published_at)
  })

  it("falls back after five seconds when the archive request hangs", async () => {
    vi.useFakeTimers()
    const fetch = vi.fn().mockReturnValue(new Promise(() => {}))
    vi.stubGlobal("fetch", fetch)
    renderReader(item)
    fireEvent.click(screen.getByRole("button", { name: "Read in app" }))
    expect(screen.getByRole("status")).toHaveTextContent("Opening article")
    await act(async () => { await vi.advanceTimersByTimeAsync(5000) })
    expect(screen.getByText(/Reading the saved text/)).toBeInTheDocument()
    expect(fetch.mock.calls[0][1].signal.aborted).toBe(true)
  })
})

describe("reader document validation", () => {
  it("accepts the expected article only", () => {
    expect(parseArticleBody(document, 1634, item.title)).not.toBeNull()
    expect(parseArticleBody(document, 1623, item.title)).toBeNull()
    expect(parseArticleBody(document, 1634, "Different release")).toBeNull()
  })
  it.each(["script", "iframe", "html"])("rejects executable block type %s", type => {
    expect(parseArticleBody({ ...document, blocks: [{ type, text: "unsafe" }] }, 1634)).toBeNull()
  })
  it.each([0, 41, Infinity, "2"])("rejects invalid spans %s", colspan => {
    const blocks = [{ type: "table", caption: "", rows: [[{ text: "x", header: false, rowspan: 1, colspan }]] }]
    expect(parseArticleBody({ ...document, blocks }, 1634)).toBeNull()
  })
  it("rejects a nonnumeric archive heading level", () => {
    expect(parseArticleBody({ ...document, blocks: [{ type: "heading", level: "02", text: "Distributions" }] }, 1634)).toBeNull()
  })
})
