import { MouseEvent } from "react"

import { NewsItem } from "@/lib/types"

import { articleLink } from "@/lib/article-link"

const PREVIEW_LENGTH = 600

type ArticleSummaryProps = {
  item: NewsItem
  openArticle: (item: NewsItem, event: MouseEvent) => void
  openReader: (item: NewsItem) => void
}

export function ArticleSummary({ item, openArticle, openReader }: ArticleSummaryProps) {
  // This is a display preview only. The stored evidence body remains complete.
  const text = (item.summary ?? "").trim()
  const shortened = text.length > PREVIEW_LENGTH
  const prefix = text.slice(0, PREVIEW_LENGTH)
  const boundary = prefix.search(/\s+\S*$/u)
  const preview = shortened
    ? `${prefix.slice(0, boundary > 0 ? boundary : PREVIEW_LENGTH).trimEnd()}…`
    : text
  const link = articleLink(item)

  return (
    <div className="article-summary">
      {preview && <div className="summary-text">{preview}</div>}
      <button className="summary-read-more reader-open"
        onClick={() => openReader(item)}>Read in app</button>
      {link && (
        <a
          className="summary-read-more"
          href={link.url}
          target="_blank"
          rel="noreferrer"
          aria-label={`Open ${link.publisher}: ${item.title}`}
          onClick={(event) => openArticle({ ...item, url: link.url }, event)}
        >
          Open {link.publisher} <span aria-hidden="true">↗</span>
        </a>
      )}
    </div>
  )
}
