"use client"

import { MouseEvent, RefObject, useState } from "react"

import { NewsItem } from "@/lib/types"

import { CheckIcon } from "./page-icons"
import { ArticleSummary } from "./article-summary"
import { ArticleReader } from "./article-reader"
import { articleLink } from "@/lib/article-link"
import { formatDetailedDate } from "./page-helpers"
import { useDeliveryViewport } from "@/hooks/use-delivery-viewport"

type NewsFeedSectionProps = {
  expandedIds: Set<number>
  feedContainerRef: RefObject<HTMLElement | null>
  items: NewsItem[]
  loadMore: () => void
  loadPendingArticles: () => void
  loading: boolean
  loadingMore: boolean
  markAsRead: (item: NewsItem) => void
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
  markAsRead,
  markAsReadAndOpen,
  nextCursor,
  pendingNewItems,
  readKeys,
  toggleRead,
  toggleSummary,
  viewMode,
}: NewsFeedSectionProps) {
  const [readingItem, setReadingItem] = useState<NewsItem | null>(null)
  const openReader = (item: NewsItem) => {
    markAsRead(item)
    setReadingItem(item)
  }
  useDeliveryViewport(feedContainerRef, items)
  return (
    <section className="feed-container" ref={feedContainerRef}>
      {pendingNewItems.length > 0 && (
        <button type="button" className="new-articles-banner" onClick={loadPendingArticles}>
          {pendingNewItems.length} new article{pendingNewItems.length !== 1 ? "s" : ""} available
          {" "}click to load
        </button>
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
        const link = articleLink(item)
        const safeItemUrl = link?.url

        return (
          <article key={item.id} data-news-id={item.id} className={`feed-row-wrapper ${isRead ? "read" : "unread"}`}>
            <div className="feed-row" onClick={() => toggleSummary(item)}>
              <div className="main-col">
                <div className="headline-content">
                  <div className="headline">
                    <a
                      href={safeItemUrl ?? "#"}
                      target="_blank"
                      rel="noreferrer"
                      aria-disabled={!safeItemUrl}
                      title={link ? `Open ${link.publisher}` : undefined}
                      onClick={(event) => markAsReadAndOpen({ ...item, url: safeItemUrl ?? "" }, event)}
                    >
                      {item.title}
                    </a>
                  </div>
                </div>
                <div className="metadata-row">
                  <div className="source-and-tickers">
                    <span className="source" title={link?.original && item.provider !== link.publisher ? `${link.publisher} · received via ${item.provider}` : item.provider}>
                      {item.provider}
                      {link?.original && item.provider !== link.publisher && <span className="source-origin"> · {link.publisher}</span>}
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
                <button className="feed-reader-button" aria-label={`Read in app: ${item.title}`} onClick={event => {
                  event.stopPropagation()
                  openReader(item)
                }}>Read</button>
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
                <ArticleSummary item={item} openArticle={markAsReadAndOpen} openReader={openReader} />
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
      {readingItem && <ArticleReader key={readingItem.id} item={readingItem} openArticle={markAsReadAndOpen} onClose={() => setReadingItem(null)} />}
    </section>
  )
}
