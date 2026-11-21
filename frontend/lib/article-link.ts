import type { ArticleBody } from "./article-body"
import type { NewsItem } from "./types"

type ArticleLink = { url: string; publisher: string; original: boolean }

function safeUrl(value: string): URL | null {
  try {
    const url = new URL(value)
    return /^https?:$/.test(url.protocol) && !url.username && !url.password ? url : null
  } catch { return null }
}

export function originalWireLink(value: string): (ArticleLink & { wireKey: string }) | null {
  const url = safeUrl(value)
  if (!url || url.port) return null
  const bw = /^(?:www\.)?businesswire\.com$/.test(url.hostname)
    && url.pathname.match(/^\/news\/home\/(\d{12,20})\/([a-z]{2})(?:\/|$)/i)
  if (bw) return { url: url.href, publisher: "Business Wire", original: true, wireKey: `bw:${bw[1]}:${bw[2].toLowerCase()}` }
  const gnw = /^(?:www\.)?globenewswire\.com$/.test(url.hostname)
    && url.pathname.match(/^\/news-release\/\d{4}\/\d{2}\/\d{2}\/(\d+)\/\d+\/([a-z]{2})\//i)
  if (gnw) return { url: url.href, publisher: "GlobeNewswire", original: true, wireKey: `gnw:${gnw[1]}:${gnw[2].toLowerCase()}` }
  const prn = /^(?:www\.)?prnewswire\.com$/.test(url.hostname)
    && url.pathname.match(/^\/news-releases\/.+-(\d+)\.html$/i)
  if (prn) return { url: url.href, publisher: "PR Newswire", original: true, wireKey: `prn:${prn[1]}` }
  return null
}

/** Presentation only: never changes stored source, deduplication identity or arrival times. */
export function articleLink(item: NewsItem, body?: ArticleBody | null): ArticleLink | null {
  const direct = originalWireLink(item.url)
  if (direct) return direct
  if (body?.article_id === item.id && body.title.trim() === item.title.trim()) {
    const original = originalWireLink(body.original_url)
    if (original?.wireKey === body.wire_key) return original
  }
  // The validated BW body retains this explicit publisher attribution. Do not
  // infer wire IDs from republisher slugs, Yahoo IDs, titles or related tickers.
  if (["FinancialContent", "Yahoo Finance"].includes(item.provider) || item.source === "Business Wire") {
    const citations = [...(item.summary ?? "").matchAll(/View source version on businesswire\.com:\s*(https?:\/\/[^\s<>"']+)/gi)]
      .map(match => originalWireLink(match[1])).filter(link => link?.publisher === "Business Wire")
    const identities = new Set(citations.map(link => link!.wireKey))
    if (identities.size === 1) return citations[0]!
  }
  const fallback = safeUrl(item.url)
  return fallback ? { url: fallback.href, publisher: item.provider || item.source || fallback.hostname, original: false } : null
}
