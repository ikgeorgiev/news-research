"use client"

import { usePageController } from "./use-page-controller"
import { NewsFeedSection } from "./news-feed-section"
import { BellIcon } from "./page-icons"
import { WatchlistSidebar } from "./watchlist-sidebar"

export default function Page() {
  const controller = usePageController()
  const { filters, newsFeed, preferences, push, sidebar, triggerRefresh } = controller

  return (
    <div className="deck-root">
      <WatchlistSidebar sidebar={sidebar} />

      {/* Main Content */}
      <main className="main-content">
        <section className="filter-rack">
          <form onSubmit={filters.onSearchSubmit} style={{ display: "flex", gap: "0.5rem", flex: 1 }}>
            <input
              value={filters.searchInput}
              onChange={(event) => filters.setSearchInput(event.target.value)}
              placeholder="Search news..."
              style={{ width: "300px" }}
            />
            <button className="primary" type="submit">Search</button>
          </form>

          <div style={{ display: "flex", alignItems: "center", gap: "0.45rem" }}>
            <select value={filters.ticker} onChange={(event) => filters.setTicker(event.target.value)}>
              <option value="">All symbols</option>
              {newsFeed.tickers.map((item) => (
                <option key={item.symbol} value={item.symbol}>{item.symbol}</option>
              ))}
            </select>
            {newsFeed.tickerError && (
              <span style={{ color: "#F23645", fontSize: "0.75rem", whiteSpace: "nowrap" }} title={newsFeed.tickerError}>
                Ticker list unavailable
              </span>
            )}
          </div>

          <select value={filters.provider} onChange={(event) => filters.setProvider(event.target.value)}>
            <option value="">All sources</option>
            {filters.providerOptions.map((item) => (
              <option key={item} value={item}>{item}</option>
            ))}
          </select>
        </section>

        <section className="status-strip" style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "1rem" }}>
            <span>
              {newsFeed.loading
                ? "Refreshing..."
                : `${preferences.unreadCount} Unread / ${newsFeed.totalCount} Total`}
              {newsFeed.items.length !== newsFeed.totalCount ? ` • ${newsFeed.items.length} shown` : ""}
            </span>
            {newsFeed.error && <span style={{ color: "#F23645" }}>Error: {newsFeed.error}</span>}
          </div>
          
          {preferences.mounted && (
            <div style={{ display: "flex", gap: "1rem", alignItems: "center" }}>
              <button 
                className="icon-button" 
                onClick={() => triggerRefresh("manual")}
                title="Refresh Data"
                style={{ cursor: "pointer", padding: "4px 12px", fontSize: "0.85rem", borderRadius: "4px", border: "1px solid var(--border-color)", background: "transparent", color: "var(--text-secondary)", display: "flex", alignItems: "center", gap: "0.4rem" }}
              >
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.59-9.21l5.67-1.16" />
                </svg>
                Refresh
              </button>
              <button
                className="icon-button"
                onClick={push.togglePushSubscription}
                title={
                  push.pushSubscribed
                    ? "Disable push notifications"
                    : "Enable push notifications"
                }
                style={{ cursor: "pointer", padding: "4px 12px", fontSize: "0.85rem", borderRadius: "4px", border: "1px solid var(--border-color)", background: push.pushSubscribed ? "var(--accent-blue)" : push.pushError ? "rgba(242, 54, 69, 0.15)" : "transparent", color: push.pushSubscribed ? "var(--text-solid)" : push.pushError ? "#F23645" : "var(--text-secondary)", display: "flex", alignItems: "center", gap: "0.4rem" }}
              >
                <BellIcon active={push.pushSubscribed} />
                {push.pushSubscribed ? "Push On" : push.pushError ? "Push Failed" : "Push"}
              </button>
              {push.pushError && !push.pushSubscribed && (
                <span style={{ fontSize: "0.75rem", color: "#F23645", maxWidth: "200px" }} title={push.pushError}>
                  {push.pushError}
                </span>
              )}
              <span
                title={
                  push.activePushScopeCount > 0
                    ? `Enabled push scopes: ${push.activePushScopeNames.join(", ")}`
                    : "No push scopes enabled"
                }
                style={{
                  padding: "4px 10px",
                  fontSize: "0.8rem",
                  borderRadius: "999px",
                  border: "1px solid var(--border-color)",
                  background: push.activePushScopeCount > 0 ? "rgba(41, 98, 255, 0.15)" : "transparent",
                  color: push.activePushScopeCount > 0 ? "var(--accent-blue)" : "var(--text-secondary)",
                  whiteSpace: "nowrap",
                }}
              >
                Push scopes: {push.activePushScopeCount}
              </span>
              <div className="view-mode-toggle" style={{ display: "flex", gap: "0.25rem", zIndex: 10 }}>
              <button 
                className={`icon-button ${preferences.viewMode === "list" ? "active" : ""}`} 
                onClick={() => preferences.setViewMode("list")}
                title="List View"
                style={{ cursor: "pointer", padding: "4px 12px", fontSize: "0.75rem", borderRadius: "4px", border: "1px solid var(--border-color)", background: preferences.viewMode === "list" ? "var(--accent-blue)" : "transparent", color: preferences.viewMode === "list" ? "var(--text-solid)" : "var(--text-secondary)" }}
              >
                List
              </button>
              <button 
                className={`icon-button ${preferences.viewMode === "full" ? "active" : ""}`} 
                onClick={() => preferences.setViewMode("full")}
                title="Full View"
                style={{ cursor: "pointer", padding: "4px 12px", fontSize: "0.75rem", borderRadius: "4px", border: "1px solid var(--border-color)", background: preferences.viewMode === "full" ? "var(--accent-blue)" : "transparent", color: preferences.viewMode === "full" ? "var(--text-solid)" : "var(--text-secondary)" }}
              >
                Full
              </button>
            </div>
          </div>
          )}
        </section>

        <NewsFeedSection
          expandedIds={preferences.expandedIds}
          feedContainerRef={newsFeed.feedContainerRef}
          items={newsFeed.items}
          loadMore={newsFeed.loadMore}
          loadPendingArticles={newsFeed.loadPendingArticles}
          loading={newsFeed.loading}
          loadingMore={newsFeed.loadingMore}
          markAsReadAndOpen={preferences.markAsReadAndOpen}
          nextCursor={newsFeed.nextCursor}
          pendingNewItems={newsFeed.pendingNewItems}
          readKeys={preferences.readKeys}
          toggleRead={preferences.toggleRead}
          toggleSummary={preferences.toggleSummary}
          viewMode={preferences.viewMode}
        />
      </main>
    </div>
  )
}
