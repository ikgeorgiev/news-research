"use client"

import { MouseEvent, RefObject } from "react"

import { NewsItem } from "@/lib/types"

import { CheckIcon } from "./page-icons"
import { formatDetailedDate, toSafeExternalUrl } from "./page-helpers"

type NewsFeedSectionProps = {
  expandedIds: Set<number>
  feedContainerRef: RefObject<HTMLElement | null>
  items: NewsItem[]
  loadMore: () => void
  loadPendingArticles: () => void
  loading: boolean
  loadingMore: boolean
  markAsReadAndOpen: (item: NewsItem, event: MouseEvent) => void
  nextCursor: string | null
  pendingNewItems: NewsItem[]
  readKeys: Set<string>
  toggleRead: (item: NewsItem, event?: MouseEvent) => void
  toggleSummary: (item: NewsItem) => void
  viewMode: "list" | "full"
}

export function NewsFeedSection({
  expandedIds,
  feedContainerRef,
  items,
  loadMore,
  loadPendingArticles,
  loading,
  loadingMore,
  markAsReadAndOpen,
  nextCursor,
  pendingNewItems,
  readKeys,
  toggleRead,
  toggleSummary,
  viewMode,
}: NewsFeedSectionProps) {
  return (
    <section className="feed-container" ref={feedContainerRef}>
      {pendingNewItems.length > 0 && (
        <div className="new-articles-banner" onClick={loadPendingArticles}>
          {pendingNewItems.length} new article{pendingNewItems.length !== 1 ? "s" : ""} available
          {" "}click to load
        </div>
      )}
      {!loading && items.length === 0 && (
        <div style={{ padding: "3rem 1.5rem", textAlign: "center", color: "var(--text-secondary)" }}>
          <p style={{ fontSize: "1.1rem", marginBottom: "0.5rem" }}>No news found</p>
          <p style={{ fontSize: "0.85rem", color: "var(--text-secondary)" }}>
            Try adjusting your filters or switching watchlists.
          </p>
        </div>
      )}
      {items.map((item) => {
        const isRead = readKeys.has(item.read_key)
        const isExpanded = viewMode === "full" || expandedIds.has(item.id)
        const safeItemUrl = toSafeExternalUrl(item.url)

        return (
          <article key={item.id} className={`feed-row-wrapper ${isRead ? "read" : "unread"}`}>
            <div className="feed-row" onClick={() => toggleSummary(item)}>
              <div className="main-col">
                <div className="headline-content">
                  <div className="headline">
                    <a
                      href={safeItemUrl ?? "#"}
                      target="_blank"
                      rel="noreferrer"
                      aria-disabled={!safeItemUrl}
                      onClick={(event) => markAsReadAndOpen(item, event)}
                    >
                      {item.title}
                    </a>
                  </div>
                </div>
                <div className="metadata-row">
                  <div className="source-and-tickers">
                    <span className="source" title={item.provider}>
                      {item.provider}
                    </span>
                    <div className="ticker-rack">
                      {item.tickers.length > 0 ? (
                        item.tickers.map((ticker) => (
                          <span key={ticker} className="ticker-pill">
                            {ticker}
                          </span>
                        ))
                      ) : (
                        <span className="ticker-pill">GENERAL</span>
                      )}
                    </div>
                  </div>
                  <span className="stamp">{formatDetailedDate(item.published_at)}</span>
                </div>
              </div>

              <div className="actions">
                <button
                  className="icon-button"
                  title={isRead ? "Mark as unread" : "Mark as read"}
                  onClick={(event) => toggleRead(item, event)}
                >
                  <CheckIcon />
                </button>
              </div>
            </div>

            {isExpanded && (
              <div className="feed-row-details">
                {item.summary && <div className="summary-text">{item.summary}</div>}
                <p>
                  <strong>Seen by system:</strong> {formatDetailedDate(item.first_seen_at)}
                </p>
                <p>
                  <strong>Alert sent:</strong>{" "}
                  {item.alert_sent_at ? formatDetailedDate(item.alert_sent_at) : "Pending / not sent"}
                </p>
              </div>
            )}
          </article>
        )
      })}

      <div className="load-more-container">
        <button className="primary" disabled={loading || !nextCursor || loadingMore} onClick={loadMore}>
          {loadingMore ? "Loading..." : nextCursor ? "Load More" : "End of results"}
        </button>
      </div>
    </section>
  )
}
