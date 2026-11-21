import { describe, expect, it } from "vitest"
import { articleLink, originalWireLink } from "./article-link"
import type { NewsItem } from "./types"
import type { ArticleBody } from "./article-body"

const original = "https://www.businesswire.com/news/home/20261106529638/en/"
const item = { id: 1714, title: "ClearBridge balance sheet", source: "Business Wire", provider: "FinancialContent",
  url: "https://markets.chroniclejournal.com/thepilotnews/article/bizwire-2026-11-6-clearbridge",
  summary: "Financial text. View source version on businesswire.com: " + original + " Contacts Investor relations" } as NewsItem

describe("original publisher links", () => {
  it("uses the explicit BW citation in the saved body without altering the item", () => {
    const before = JSON.stringify(item)
    expect(articleLink(item)).toMatchObject({ url: original, publisher: "Business Wire", original: true })
    expect(articleLink({ ...item, source: "Yahoo Finance", provider: "Yahoo Finance" })?.url).toBe(original)
    expect(JSON.stringify(item)).toBe(before)
  })
  it("uses a matching archived identity when a summary is absent", () => {
    const body = { article_id: item.id, title: item.title, original_url: original, wire_key: "bw:20261106529638:en" } as ArticleBody
    expect(articleLink({ ...item, summary: null }, body)?.url).toBe(original)
    for (const bad of [{ ...body, article_id: 1 }, { ...body, title: "Different release" }, { ...body, wire_key: "bw:20261106529639:en" }]) {
      expect(articleLink({ ...item, summary: null }, bad)?.url).toBe(item.url)
    }
  })
  it("keeps native GNW links but never guesses an original from a Yahoo ID", () => {
    const url = "https://www.globenewswire.com/news-release/2026/11/04/3395763/0/en/oxford-lane.html"
    expect(articleLink({ ...item, url })?.publisher).toBe("GlobeNewswire")
    const yahoo = { ...item, source: "Yahoo Finance", provider: "Yahoo Finance", summary: "OXLC (GLOBE NEWSWIRE)", url: "https://finance.yahoo.com/news/oxford-130000884.html" }
    expect(articleLink(yahoo)).toEqual({ url: yahoo.url, publisher: "Yahoo Finance", original: false })
  })
  it("requires one unambiguous identity and a publisher attribution", () => {
    expect(articleLink({ ...item, summary: "Related link " + original })?.url).toBe(item.url)
    expect(articleLink({ ...item, summary: item.summary + " View source version on businesswire.com: https://www.businesswire.com/news/home/20261106529639/en/" })?.url).toBe(item.url)
  })
  it.each(["javascript:alert(1)", "https://www.businesswire.com.evil.test/news/home/20261106529638/en/", "https://www.businesswire.com@evil.test/news/home/20261106529638/en/", "https://user:pass@www.businesswire.com/news/home/20261106529638/en/", "https://www.businesswire.com:444/news/home/20261106529638/en/", "https://www.businesswire.com/"])("rejects an unsafe or non-article original: %s", url => {
    expect(originalWireLink(url)).toBeNull()
  })
})
