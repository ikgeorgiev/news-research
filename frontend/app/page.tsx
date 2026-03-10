"use client"

import { FormEvent, MouseEvent, useState } from "react"
import { usePushSubscription } from "@/hooks/usePushSubscription"
import { Watchlist } from "@/lib/types"
import { NewsFeedSection } from "./news-feed-section"
import { BellIcon } from "./page-icons"
import { useFeedPreferences } from "./use-feed-preferences"
import { useNewsFeed } from "./use-news-feed"
import { useWatchlists } from "./use-watchlists"
import { watchlistMatchesItem } from "./page-helpers"
import { WatchlistSidebar } from "./watchlist-sidebar"

const STATIC_PROVIDERS = ["Yahoo Finance", "PR Newswire", "GlobeNewswire", "Business Wire"]

export default function Page() {
  // Search/Filter state
  const [ticker, setTicker] = useState("")
  const [provider, setProvider] = useState("")
  const [searchInput, setSearchInput] = useState("")
  const [searchQuery, setSearchQuery] = useState("")

  const syncFiltersToWatchlist = (watchlist?: Watchlist) => {
    const nextProvider = watchlist?.provider || ""
    const nextQuery = watchlist?.q || ""
    setTicker("")
    setProvider(nextProvider)
    setSearchQuery(nextQuery)
    setSearchInput(nextQuery)
  }

  const {
    activeWatchlist,
    activeWatchlistId,
    closeContextMenu,
    contextMenu,
    customWatchlists,
    handleContextDelete,
    handleCreateWatchlist,
    handleFinishRename,
    handleStartRename,
    handleWatchlistContextMenu,
    isCreatingWatchlist,
    newWatchlistName,
    renameValue,
    renamingWatchlistId,
    selectWatchlist,
    selectedTickers,
    setIsCreatingWatchlist,
    setNewWatchlistName,
    setRenameValue,
    setRenamingWatchlistId,
    toggleTickerSelection,
  } = useWatchlists({
    onSelectWatchlist: syncFiltersToWatchlist,
  })

  const {
    error,
    feedContainerRef,
    globalTrackedIds,
    items,
    loadMore,
    loadPendingArticles,
    loading,
    loadingMore,
    nextCursor,
    pendingNewItems,
    tickers,
    totalCount,
    triggerRefresh,
  } = useNewsFeed({
    activeWatchlist,
    activeWatchlistId,
    provider,
    searchQuery,
    ticker,
  })

  const {
    addReadIds,
    expandedIds,
    markAsReadAndOpen,
    markReadByQuery,
    mounted,
    readIds,
    setViewMode,
    toggleRead,
    toggleSummary,
    trackedUnreadItems,
    unreadCount,
    viewMode,
  } = useFeedPreferences({
    globalTrackedIds,
    items,
    pendingNewItems,
    totalCount,
  })

  const {
    alertIncludeAllNews,
    activePushScopeCount,
    activePushScopeNames,
    isPushScopeEnabled,
    pushError,
    pushSubscribed,
    togglePushScope,
    togglePushSubscription,
  } = usePushSubscription({
    customWatchlists,
    mounted,
  })

  const handleTogglePushScope = (watchlistId: string, e?: MouseEvent) => {
    e?.preventDefault()
    e?.stopPropagation()
    togglePushScope(watchlistId)
  }

  const onSearchSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setSearchQuery(searchInput.trim())
  }

  const handleMarkAllRead = (wlId: string) => {
    closeContextMenu()

    if (wlId === "all") {
      // Fetch ALL article IDs from the backend so we mark everything read,
      // including articles beyond the current Load More window.
      markReadByQuery({ includeUnmappedFromProvider: "Business Wire" })
        .catch(() => {
          // Fallback: mark only tracked items if the fetch fails.
          addReadIds(trackedUnreadItems.map((item) => item.id))
        })
      return
    }

    const wl = customWatchlists.find(w => w.id === wlId)
    if (!wl) return
    const params = {
      tickers: wl.tickers.length > 0 ? wl.tickers : undefined,
      provider: wl.provider,
      q: wl.q,
    }
    if (!params.tickers && !params.provider && !params.q) return

    markReadByQuery(params)
      .catch(() => {
        // Fallback: mark only tracked items matching this watchlist.
        const matchingIds = trackedUnreadItems
          .filter((item) => watchlistMatchesItem(item, wl))
          .map((item) => item.id)
        addReadIds(matchingIds)
      })
  }

  return (
    <div className="deck-root">
      <WatchlistSidebar
        activeWatchlistId={activeWatchlistId}
        alertIncludeAllNews={alertIncludeAllNews}
        closeContextMenu={closeContextMenu}
        contextMenu={contextMenu}
        customWatchlists={customWatchlists}
        handleContextDelete={handleContextDelete}
        handleCreateWatchlist={handleCreateWatchlist}
        handleFinishRename={handleFinishRename}
        handleMarkAllRead={handleMarkAllRead}
        handleStartRename={handleStartRename}
        handleTogglePushScope={handleTogglePushScope}
        handleWatchlistContextMenu={handleWatchlistContextMenu}
        isCreatingWatchlist={isCreatingWatchlist}
        isPushScopeEnabled={isPushScopeEnabled}
        newWatchlistName={newWatchlistName}
        readIds={readIds}
        renameValue={renameValue}
        renamingWatchlistId={renamingWatchlistId}
        selectWatchlist={selectWatchlist}
        selectedTickers={selectedTickers}
        setIsCreatingWatchlist={setIsCreatingWatchlist}
        setNewWatchlistName={setNewWatchlistName}
        setRenameValue={setRenameValue}
        setRenamingWatchlistId={setRenamingWatchlistId}
        tickers={tickers}
        togglePushScope={togglePushScope}
        toggleTickerSelection={toggleTickerSelection}
        trackedUnreadItems={trackedUnreadItems}
        unreadCount={unreadCount}
      />

      {/* Main Content */}
      <main className="main-content">
        <section className="filter-rack">
          <form onSubmit={onSearchSubmit} style={{ display: "flex", gap: "0.5rem", flex: 1 }}>
            <input
              value={searchInput}
              onChange={(event) => setSearchInput(event.target.value)}
              placeholder="Search news..."
              style={{ width: "300px" }}
            />
            <button className="primary" type="submit">Search</button>
          </form>

          <select value={ticker} onChange={(event) => setTicker(event.target.value)}>
            <option value="">All symbols</option>
            {tickers.map((item) => (
              <option key={item.symbol} value={item.symbol}>{item.symbol}</option>
            ))}
          </select>

          <select value={provider} onChange={(event) => setProvider(event.target.value)}>
            <option value="">All sources</option>
            {STATIC_PROVIDERS.map((item) => (
              <option key={item} value={item}>{item}</option>
            ))}
          </select>
        </section>

        <section className="status-strip" style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "1rem" }}>
            <span>
              {loading
                ? "Refreshing..."
                : `${unreadCount} Unread / ${totalCount} Total`}
              {items.length !== totalCount ? ` • ${items.length} shown` : ""}
            </span>
            {error && <span style={{ color: "#F23645" }}>Error: {error}</span>}
          </div>
          
          {mounted && (
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
                onClick={togglePushSubscription}
                title={
                  pushSubscribed
                    ? "Disable push notifications"
                    : "Enable push notifications"
                }
                style={{ cursor: "pointer", padding: "4px 12px", fontSize: "0.85rem", borderRadius: "4px", border: "1px solid var(--border-color)", background: pushSubscribed ? "var(--accent-blue)" : pushError ? "rgba(242, 54, 69, 0.15)" : "transparent", color: pushSubscribed ? "var(--text-solid)" : pushError ? "#F23645" : "var(--text-secondary)", display: "flex", alignItems: "center", gap: "0.4rem" }}
              >
                <BellIcon active={pushSubscribed} />
                {pushSubscribed ? "Push On" : pushError ? "Push Failed" : "Push"}
              </button>
              {pushError && !pushSubscribed && (
                <span style={{ fontSize: "0.75rem", color: "#F23645", maxWidth: "200px" }} title={pushError}>
                  {pushError}
                </span>
              )}
              <span
                title={
                  activePushScopeCount > 0
                    ? `Enabled push scopes: ${activePushScopeNames.join(", ")}`
                    : "No push scopes enabled"
                }
                style={{
                  padding: "4px 10px",
                  fontSize: "0.8rem",
                  borderRadius: "999px",
                  border: "1px solid var(--border-color)",
                  background: activePushScopeCount > 0 ? "rgba(41, 98, 255, 0.15)" : "transparent",
                  color: activePushScopeCount > 0 ? "var(--accent-blue)" : "var(--text-secondary)",
                  whiteSpace: "nowrap",
                }}
              >
                Push scopes: {activePushScopeCount}
              </span>
              <div className="view-mode-toggle" style={{ display: "flex", gap: "0.25rem", zIndex: 10 }}>
              <button 
                className={`icon-button ${viewMode === "list" ? "active" : ""}`} 
                onClick={() => setViewMode("list")}
                title="List View"
                style={{ cursor: "pointer", padding: "4px 12px", fontSize: "0.75rem", borderRadius: "4px", border: "1px solid var(--border-color)", background: viewMode === "list" ? "var(--accent-blue)" : "transparent", color: viewMode === "list" ? "var(--text-solid)" : "var(--text-secondary)" }}
              >
                List
              </button>
              <button 
                className={`icon-button ${viewMode === "full" ? "active" : ""}`} 
                onClick={() => setViewMode("full")}
                title="Full View"
                style={{ cursor: "pointer", padding: "4px 12px", fontSize: "0.75rem", borderRadius: "4px", border: "1px solid var(--border-color)", background: viewMode === "full" ? "var(--accent-blue)" : "transparent", color: viewMode === "full" ? "var(--text-solid)" : "var(--text-secondary)" }}
              >
                Full
              </button>
            </div>
          </div>
          )}
        </section>

        <NewsFeedSection
          expandedIds={expandedIds}
          feedContainerRef={feedContainerRef}
          items={items}
          loadMore={loadMore}
          loadPendingArticles={loadPendingArticles}
          loading={loading}
          loadingMore={loadingMore}
          markAsReadAndOpen={markAsReadAndOpen}
          nextCursor={nextCursor}
          pendingNewItems={pendingNewItems}
          readIds={readIds}
          toggleRead={toggleRead}
          toggleSummary={toggleSummary}
          viewMode={viewMode}
        />
      </main>
    </div>
  )
}
