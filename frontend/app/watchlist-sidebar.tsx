"use client"

import { FormEvent, KeyboardEvent, MouseEvent } from "react"

import { NewsItem, TickerItem, Watchlist } from "@/lib/types"

import { BellIcon } from "./page-icons"
import { watchlistMatchesItem } from "./page-helpers"
import { ContextMenuState } from "./use-watchlists"

type WatchlistSidebarProps = {
  activeWatchlistId: string
  alertIncludeAllNews: boolean
  closeContextMenu: () => void
  contextMenu: ContextMenuState
  customWatchlists: Watchlist[]
  handleContextDelete: (watchlistId: string) => void
  handleCreateWatchlist: (event: FormEvent) => void
  handleFinishRename: () => void
  handleMarkAllRead: (watchlistId: string) => void
  handleStartRename: (watchlistId: string) => void
  handleTogglePushScope: (watchlistId: string, event?: MouseEvent) => void
  handleWatchlistContextMenu: (event: MouseEvent, watchlistId: string) => void
  isCreatingWatchlist: boolean
  isPushScopeEnabled: (watchlistId: string) => boolean
  newWatchlistName: string
  readIds: Set<number>
  renameValue: string
  renamingWatchlistId: string | null
  selectWatchlist: (watchlistId: string, watchlist?: Watchlist) => void
  selectedTickers: Set<string>
  setIsCreatingWatchlist: (value: boolean) => void
  setNewWatchlistName: (value: string) => void
  setRenameValue: (value: string) => void
  setRenamingWatchlistId: (value: string | null) => void
  tickers: TickerItem[]
  togglePushScope: (watchlistId: string) => void
  toggleTickerSelection: (symbol: string) => void
  trackedUnreadItems: NewsItem[]
  unreadCount: number
}

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
  readIds,
  renameValue,
  renamingWatchlistId,
  selectWatchlist,
  selectedTickers,
  setIsCreatingWatchlist,
  setNewWatchlistName,
  setRenameValue,
  setRenamingWatchlistId,
  tickers,
  togglePushScope,
  toggleTickerSelection,
  trackedUnreadItems,
  unreadCount,
}: WatchlistSidebarProps) {
  const handleRenameKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "Enter") handleFinishRename()
    if (event.key === "Escape") {
      setRenamingWatchlistId(null)
      setRenameValue("")
    }
  }

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
          const watchlistUnreadCount = trackedUnreadItems.filter(
            (item) => !readIds.has(item.id) && watchlistMatchesItem(item, watchlist)
          ).length

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
              disabled={!newWatchlistName.trim() || selectedTickers.size === 0}
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
