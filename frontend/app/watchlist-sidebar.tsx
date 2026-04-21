"use client"

import { KeyboardEvent, useMemo } from "react"

import { BellIcon } from "./page-icons"
import { watchlistMatchesItem } from "./page-helpers"
import { WatchlistSidebarModel } from "./use-page-controller"

function renderUnreadBadge(active: boolean, count: number) {
  if (count <= 0) return null

  return (
    <span className={`unread-badge ${active ? "active" : ""}`}>
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
    readKeys,
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
      if (readKeys.has(item.read_key)) {
        continue
      }
      for (const watchlist of customWatchlists) {
        if (watchlistMatchesItem(item, watchlist)) {
          counts.set(watchlist.id, (counts.get(watchlist.id) || 0) + 1)
        }
      }
    }

    return counts
  }, [customWatchlists, readKeys, trackedUnreadItems])

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
      >
        <span>All News</span>
        <div className="watchlist-item-actions">
          <button
            className={`icon-button icon-button-tight ${alertIncludeAllNews ? "icon-button-active" : ""}`}
            title={
              alertIncludeAllNews
                ? "Disable push notifications for All News"
                : "Enable push notifications for All News"
            }
            onClick={(event) => handleTogglePushScope("all", event)}
          >
            <BellIcon active={alertIncludeAllNews} />
          </button>
          {renderUnreadBadge(activeWatchlistId === "all", unreadCount)}
        </div>
      </div>

      <div className="sidebar-section-header">
        <h2>Watchlists</h2>
        {!isCreatingWatchlist && (
          <button
            className="icon-button icon-button-add"
            title="New Watchlist"
            onClick={() => setIsCreatingWatchlist(true)}
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
            >
              {renamingWatchlistId === watchlist.id ? (
                <input
                  autoFocus
                  className="rename-input rename-input-inline"
                  value={renameValue}
                  onChange={(event) => setRenameValue(event.target.value)}
                  onFocus={(event) => event.target.select()}
                  onBlur={handleFinishRename}
                  onKeyDown={handleRenameKeyDown}
                  onClick={(event) => event.stopPropagation()}
                />
              ) : (
                <span>{watchlist.name}</span>
              )}
              <div className="watchlist-item-actions">
                <button
                  className={`icon-button icon-button-tight ${isPushScopeEnabled(watchlist.id) ? "icon-button-active" : ""}`}
                  title={
                    isPushScopeEnabled(watchlist.id)
                      ? `Disable push notifications for ${watchlist.name}`
                      : `Enable push notifications for ${watchlist.name}`
                  }
                  onClick={(event) => handleTogglePushScope(watchlist.id, event)}
                >
                  <BellIcon active={isPushScopeEnabled(watchlist.id)} />
                </button>
                {renderUnreadBadge(activeWatchlistId === watchlist.id, watchlistUnreadCount)}
              </div>
            </div>
          )
        })}
        {customWatchlists.length === 0 && !isCreatingWatchlist && (
          <p className="watchlist-empty-state">
            No custom watchlists yet.
          </p>
        )}
      </div>

      {isCreatingWatchlist && (
        <form
          className="create-watchlist-form"
          onSubmit={handleCreateWatchlist}
        >
          <input
            autoFocus
            className="watchlist-form-field"
            placeholder="Watchlist Name"
            value={newWatchlistName}
            onChange={(event) => setNewWatchlistName(event.target.value)}
          />
          <select
            aria-label="Watchlist Provider"
            className="watchlist-form-field"
            value={newWatchlistProvider}
            onChange={(event) => setNewWatchlistProvider(event.target.value)}
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
            className="watchlist-form-field"
            placeholder="Optional search query"
            value={newWatchlistQuery}
            onChange={(event) => setNewWatchlistQuery(event.target.value)}
          />
          <div className="ticker-selector">
            {tickers.map((tickerItem) => (
              <label
                key={tickerItem.symbol}
                className="ticker-selector-option"
              >
                <input
                  type="checkbox"
                  checked={selectedTickers.has(tickerItem.symbol)}
                  onChange={() => toggleTickerSelection(tickerItem.symbol)}
                  className="ticker-selector-checkbox"
                />
                {tickerItem.symbol}
              </label>
            ))}
          </div>
          <div className="watchlist-form-actions">
            <button
              type="submit"
              className="primary watchlist-form-button"
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
              className="watchlist-form-button"
              onClick={() => setIsCreatingWatchlist(false)}
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
