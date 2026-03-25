"use client"

import { KeyboardEvent, useMemo } from "react"

import { BellIcon } from "./page-icons"
import { watchlistMatchesItem } from "./page-helpers"
import { WatchlistSidebarModel } from "./use-page-controller"

function renderUnreadBadge(active: boolean, count: number) {
  if (count <= 0) return null

  return (
    <span
      style={{
        background: active ? "var(--accent-blue)" : "rgba(255, 255, 255, 0.1)",
        color: active ? "white" : "var(--text-secondary)",
        fontSize: "0.75rem",
        padding: "2px 8px",
        borderRadius: "12px",
        fontWeight: "normal",
      }}
    >
      {count}
    </span>
  )
}

export function WatchlistSidebar({
  sidebar,
}: {
  sidebar: WatchlistSidebarModel
}) {
  const {
    activeWatchlistId,
    alertIncludeAllNews,
    closeContextMenu,
    contextMenu,
    customWatchlists,
    handleContextDelete,
    handleCreateWatchlist,
    handleFinishRename,
    handleMarkAllRead,
    handleStartRename,
    handleTogglePushScope,
    handleWatchlistContextMenu,
    isCreatingWatchlist,
    isPushScopeEnabled,
    newWatchlistName,
    newWatchlistProvider,
    newWatchlistQuery,
    providerOptions,
    readIds,
    renameValue,
    renamingWatchlistId,
    selectWatchlist,
    selectedTickers,
    setIsCreatingWatchlist,
    setNewWatchlistName,
    setNewWatchlistProvider,
    setNewWatchlistQuery,
    setRenameValue,
    setRenamingWatchlistId,
    tickers,
    togglePushScope,
    toggleTickerSelection,
    trackedUnreadItems,
    unreadCount,
  } = sidebar

  const handleRenameKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "Enter") handleFinishRename()
    if (event.key === "Escape") {
      setRenamingWatchlistId(null)
      setRenameValue("")
    }
  }

  const unreadCountByWatchlist = useMemo(() => {
    const counts = new Map<string, number>()
    for (const watchlist of customWatchlists) {
      counts.set(watchlist.id, 0)
    }

    for (const item of trackedUnreadItems) {
      if (readIds.has(item.id)) {
        continue
      }
      for (const watchlist of customWatchlists) {
        if (watchlistMatchesItem(item, watchlist)) {
          counts.set(watchlist.id, (counts.get(watchlist.id) || 0) + 1)
        }
      }
    }

    return counts
  }, [customWatchlists, readIds, trackedUnreadItems])

  return (
    <aside className="sidebar">
      <div className="brand-header">
        <h1>CEF News</h1>
        <p>MARKET DATA</p>
      </div>

      <div
        className={`watchlist-item all-news-item ${activeWatchlistId === "all" ? "active" : ""}`}
        onClick={() => selectWatchlist("all")}
        onContextMenu={(event) => handleWatchlistContextMenu(event, "all")}
        style={{
          fontWeight: "bold",
          fontSize: "1.1rem",
          marginBottom: "1rem",
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
        }}
      >
        <span>All News</span>
        <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
          <button
            className="icon-button"
            title={
              alertIncludeAllNews
                ? "Disable push notifications for All News"
                : "Enable push notifications for All News"
            }
            onClick={(event) => handleTogglePushScope("all", event)}
            style={{
              padding: "2px",
              color: alertIncludeAllNews ? "var(--accent-blue)" : "var(--text-secondary)",
            }}
          >
            <BellIcon active={alertIncludeAllNews} />
          </button>
          {renderUnreadBadge(activeWatchlistId === "all", unreadCount)}
        </div>
      </div>

      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <h2>Watchlists</h2>
        {!isCreatingWatchlist && (
          <button
            className="icon-button"
            title="New Watchlist"
            onClick={() => setIsCreatingWatchlist(true)}
            style={{ padding: "4px", fontSize: "1.2rem" }}
          >
            +
          </button>
        )}
      </div>

      <div>
        {customWatchlists.map((watchlist) => {
          const watchlistUnreadCount = unreadCountByWatchlist.get(watchlist.id) || 0

          return (
            <div
              key={watchlist.id}
              className={`watchlist-item ${activeWatchlistId === watchlist.id ? "active" : ""}`}
              onClick={() => selectWatchlist(watchlist.id, watchlist)}
              onContextMenu={(event) => handleWatchlistContextMenu(event, watchlist.id)}
              style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}
            >
              {renamingWatchlistId === watchlist.id ? (
                <input
                  autoFocus
                  className="rename-input"
                  value={renameValue}
                  onChange={(event) => setRenameValue(event.target.value)}
                  onFocus={(event) => event.target.select()}
                  onBlur={handleFinishRename}
                  onKeyDown={handleRenameKeyDown}
                  onClick={(event) => event.stopPropagation()}
                  style={{ flex: 1, fontSize: "0.9rem", padding: "2px 4px" }}
                />
              ) : (
                <span>{watchlist.name}</span>
              )}
              <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
                <button
                  className="icon-button"
                  title={
                    isPushScopeEnabled(watchlist.id)
                      ? `Disable push notifications for ${watchlist.name}`
                      : `Enable push notifications for ${watchlist.name}`
                  }
                  onClick={(event) => handleTogglePushScope(watchlist.id, event)}
                  style={{
                    padding: "2px",
                    color: isPushScopeEnabled(watchlist.id)
                      ? "var(--accent-blue)"
                      : "var(--text-secondary)",
                  }}
                >
                  <BellIcon active={isPushScopeEnabled(watchlist.id)} />
                </button>
                {renderUnreadBadge(activeWatchlistId === watchlist.id, watchlistUnreadCount)}
              </div>
            </div>
          )
        })}
        {customWatchlists.length === 0 && !isCreatingWatchlist && (
          <p
            style={{
              fontSize: "0.85rem",
              color: "var(--text-secondary)",
              padding: "0 0.5rem",
            }}
          >
            No custom watchlists yet.
          </p>
        )}
      </div>

      {isCreatingWatchlist && (
        <form
          className="create-watchlist-form"
          onSubmit={handleCreateWatchlist}
          style={{
            marginTop: "1rem",
            padding: "0.5rem",
            background: "var(--bg-hover)",
            borderRadius: "4px",
          }}
        >
          <input
            autoFocus
            placeholder="Watchlist Name"
            value={newWatchlistName}
            onChange={(event) => setNewWatchlistName(event.target.value)}
            style={{ width: "100%", marginBottom: "0.5rem" }}
          />
          <select
            aria-label="Watchlist Provider"
            value={newWatchlistProvider}
            onChange={(event) => setNewWatchlistProvider(event.target.value)}
            style={{ width: "100%", marginBottom: "0.5rem" }}
          >
            <option value="">Any provider</option>
            {providerOptions.map((provider) => (
              <option key={provider} value={provider}>
                {provider}
              </option>
            ))}
          </select>
          <input
            aria-label="Watchlist Query"
            placeholder="Optional search query"
            value={newWatchlistQuery}
            onChange={(event) => setNewWatchlistQuery(event.target.value)}
            style={{ width: "100%", marginBottom: "0.5rem" }}
          />
          <div
            className="ticker-selector"
            style={{
              maxHeight: "150px",
              overflowY: "auto",
              marginBottom: "0.5rem",
              border: "1px solid var(--border-color)",
              borderRadius: "4px",
              padding: "4px",
            }}
          >
            {tickers.map((tickerItem) => (
              <label
                key={tickerItem.symbol}
                style={{ display: "block", fontSize: "0.85rem", cursor: "pointer", padding: "2px 0" }}
              >
                <input
                  type="checkbox"
                  checked={selectedTickers.has(tickerItem.symbol)}
                  onChange={() => toggleTickerSelection(tickerItem.symbol)}
                  style={{ marginRight: "6px" }}
                />
                {tickerItem.symbol}
              </label>
            ))}
          </div>
          <div style={{ display: "flex", gap: "0.5rem" }}>
            <button
              type="submit"
              className="primary"
              style={{ flex: 1, padding: "4px" }}
              disabled={
                !newWatchlistName.trim()
                || (
                  selectedTickers.size === 0
                  && !newWatchlistProvider.trim()
                  && !newWatchlistQuery.trim()
                )
              }
            >
              Save
            </button>
            <button
              type="button"
              onClick={() => setIsCreatingWatchlist(false)}
              style={{ flex: 1, padding: "4px" }}
            >
              Cancel
            </button>
          </div>
        </form>
      )}

      {contextMenu && (
        <div
          className="context-menu"
          style={{ top: contextMenu.y, left: contextMenu.x }}
          onClick={(event) => event.stopPropagation()}
        >
          <div
            className="context-menu-item"
            onClick={() => {
              togglePushScope(contextMenu.watchlistId)
              closeContextMenu()
            }}
          >
            {isPushScopeEnabled(contextMenu.watchlistId) ? "Disable Push" : "Enable Push"}
          </div>
          <div className="context-menu-separator" />
          <div className="context-menu-item" onClick={() => handleMarkAllRead(contextMenu.watchlistId)}>
            Mark All Items as Read
          </div>
          {contextMenu.watchlistId !== "all" && (
            <>
              <div className="context-menu-item" onClick={() => handleStartRename(contextMenu.watchlistId)}>
                Rename
              </div>
              <div className="context-menu-separator" />
              <div
                className="context-menu-item delete"
                onClick={() => handleContextDelete(contextMenu.watchlistId)}
              >
                Delete
              </div>
            </>
          )}
        </div>
      )}
    </aside>
  )
}
