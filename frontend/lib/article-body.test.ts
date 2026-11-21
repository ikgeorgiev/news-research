import { describe, expect, it } from "vitest"
import { parseArticleBody } from "./article-body"
import { articleLink } from "./article-link"
import type { NewsItem } from "./types"

const cases = [
  ["bw:20261106529638:en", "https://www.businesswire.com/news/home/20261106529638/en/", "Business Wire"],
  ["gnw:3393802:en", "https://www.globenewswire.com/news-release/2026/11/02/3393802/16530/en/guggenheim.html", "GlobeNewswire"],
  ["prn:302926958", "https://www.prnewswire.com/news-releases/john-hancock-302926958.html", "PR Newswire"],
]
const body = (key: string, url: string) => ({ version: 1, article_id: 42, title: "Fund distributions",
  wire_key: key, source_url: url, original_url: url, published_at: "2026-11-02T21:15:00Z",
  blocks: [{ type: "paragraph", text: "Distribution details." }] })

describe("publisher reader identities", () => {
  it.each(cases)("accepts the matching original identity %s", (key, url, publisher) => {
    const document = parseArticleBody(body(key, url), 42, "Fund distributions")
    expect(document).not.toBeNull()
    const item = { id: 42, title: "Fund distributions", url: "https://example.com/copy", provider: "Other" } as NewsItem
    expect(articleLink(item, document)).toMatchObject({ url, publisher, original: true })
  })
  it.each(cases)("rejects a different release URL for %s", (key, url) => {
    expect(parseArticleBody({ ...body(key, url), original_url: cases.find(c => c[0] !== key)![1] }, 42)).toBeNull()
  })
  it.each(["2026-11-02", "2026-11-02T21:15:00", "not-a-date"])("rejects an imprecise date %s", published_at => {
    expect(parseArticleBody({ ...body(cases[0][0], cases[0][1]), published_at }, 42)).toBeNull()
  })
  it("rejects a disguised publisher host", () => {
    expect(parseArticleBody(body("prn:302926958", "https://prnewswire.com.evil.test/news-releases/a-302926958.html"), 42)).toBeNull()
  })
})
