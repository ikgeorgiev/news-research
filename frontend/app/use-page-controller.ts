"use client"

import { FormEvent, MouseEvent, useState } from "react"

import { usePushSubscription } from "@/hooks/usePushSubscription"
import { Watchlist } from "@/lib/types"

import {
  buildWatchlistQueryParams,
  GENERAL_NEWS_PROVIDER,
  STATIC_PROVIDER_OPTIONS,
  watchlistMatchesItem,
} from "./page-helpers"
import { useFeedPreferences } from "./use-feed-preferences"
import { useNewsFeed } from "./use-news-feed"
import { ContextMenuState, useWatchlists } from "./use-watchlists"

export type WatchlistSidebarModel = {
  activeWatchlistId: string
  alertIncludeAllNews: boolean
  closeContextMenu: () => void
  contextMenu: ContextMenuState
  customWatchlists: ReturnType<typeof useWatchlists>["customWatchlists"]
  handleContextDelete: (watchlistId: string) => void
  handleCreateWatchlist: ReturnType<typeof useWatchlists>["handleCreateWatchlist"]
  handleFinishRename: () => void
  handleMarkAllRead: (watchlistId: string) => void
  handleStartRename: (watchlistId: string) => void
  handleTogglePushScope: (watchlistId: string, event?: MouseEvent) => void
  handleWatchlistContextMenu: (event: MouseEvent, watchlistId: string) => void
  isCreatingWatchlist: boolean
  isPushScopeEnabled: (watchlistId: string) => boolean
  newWatchlistName: string
  newWatchlistProvider: string
  newWatchlistQuery: string
  providerOptions: string[]
  readIds: Set<number>
  renameValue: string
  renamingWatchlistId: string | null
  selectWatchlist: (watchlistId: string, watchlist?: Watchlist) => void
  selectedTickers: Set<string>
  setIsCreatingWatchlist: (value: boolean) => void
  setNewWatchlistName: (value: string) => void
  setNewWatchlistProvider: (value: string) => void
  setNewWatchlistQuery: (value: string) => void
  setRenameValue: (value: string) => void
  setRenamingWatchlistId: (value: string | null) => void
  tickers: ReturnType<typeof useNewsFeed>["tickers"]
  togglePushScope: (watchlistId: string) => void
  toggleTickerSelection: (symbol: string) => void
  trackedUnreadItems: ReturnType<typeof useFeedPreferences>["trackedUnreadItems"]
  unreadCount: number
}

export function usePageController() {
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

  const watchlists = useWatchlists({
    onSelectWatchlist: syncFiltersToWatchlist,
  })

  const newsFeed = useNewsFeed({
    activeWatchlist: watchlists.activeWatchlist,
    activeWatchlistId: watchlists.activeWatchlistId,
    provider,
    searchQuery,
    ticker,
  })

  const preferences = useFeedPreferences({
    globalTrackedIds: newsFeed.globalTrackedIds,
    items: newsFeed.items,
    pendingNewItems: newsFeed.pendingNewItems,
    totalCount: newsFeed.totalCount,
  })

  const push = usePushSubscription({
    customWatchlists: watchlists.customWatchlists,
    mounted: preferences.mounted,
  })

  const handleTogglePushScope = (watchlistId: string, event?: MouseEvent) => {
    event?.preventDefault()
    event?.stopPropagation()
    push.togglePushScope(watchlistId)
  }

  const onSearchSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setSearchQuery(searchInput.trim())
  }

  const handleMarkAllRead = (watchlistId: string) => {
    watchlists.closeContextMenu()

    if (watchlistId === "all") {
      preferences.markReadByQuery({ includeUnmappedFromProvider: GENERAL_NEWS_PROVIDER })
        .catch(() => {
          preferences.addReadIds(preferences.trackedUnreadItems.map((item) => item.id))
        })
      return
    }

    const watchlist = watchlists.customWatchlists.find((item) => item.id === watchlistId)
    if (!watchlist) return

    const params = buildWatchlistQueryParams(watchlist)
    if (!params.tickers && !params.provider && !params.q) return

    preferences.markReadByQuery(params)
      .catch(() => {
        const matchingIds = preferences.trackedUnreadItems
          .filter((item) => watchlistMatchesItem(item, watchlist))
          .map((item) => item.id)
        preferences.addReadIds(matchingIds)
      })
  }

  const sidebar: WatchlistSidebarModel = {
    activeWatchlistId: watchlists.activeWatchlistId,
    alertIncludeAllNews: push.alertIncludeAllNews,
    closeContextMenu: watchlists.closeContextMenu,
    contextMenu: watchlists.contextMenu,
    customWatchlists: watchlists.customWatchlists,
    handleContextDelete: watchlists.handleContextDelete,
    handleCreateWatchlist: watchlists.handleCreateWatchlist,
    handleFinishRename: watchlists.handleFinishRename,
    handleMarkAllRead,
    handleStartRename: watchlists.handleStartRename,
    handleTogglePushScope,
    handleWatchlistContextMenu: watchlists.handleWatchlistContextMenu,
    isCreatingWatchlist: watchlists.isCreatingWatchlist,
    isPushScopeEnabled: push.isPushScopeEnabled,
    newWatchlistName: watchlists.newWatchlistName,
    newWatchlistProvider: watchlists.newWatchlistProvider,
    newWatchlistQuery: watchlists.newWatchlistQuery,
    providerOptions: STATIC_PROVIDER_OPTIONS,
    readIds: preferences.readIds,
    renameValue: watchlists.renameValue,
    renamingWatchlistId: watchlists.renamingWatchlistId,
    selectWatchlist: watchlists.selectWatchlist,
    selectedTickers: watchlists.selectedTickers,
    setIsCreatingWatchlist: watchlists.setIsCreatingWatchlist,
    setNewWatchlistName: watchlists.setNewWatchlistName,
    setNewWatchlistProvider: watchlists.setNewWatchlistProvider,
    setNewWatchlistQuery: watchlists.setNewWatchlistQuery,
    setRenameValue: watchlists.setRenameValue,
    setRenamingWatchlistId: watchlists.setRenamingWatchlistId,
    tickers: newsFeed.tickers,
    togglePushScope: push.togglePushScope,
    toggleTickerSelection: watchlists.toggleTickerSelection,
    trackedUnreadItems: preferences.trackedUnreadItems,
    unreadCount: preferences.unreadCount,
  }

  return {
    filters: {
      onSearchSubmit,
      provider,
      providerOptions: STATIC_PROVIDER_OPTIONS,
      searchInput,
      setProvider,
      setSearchInput,
      setTicker,
      ticker,
    },
    newsFeed,
    preferences,
    push,
    sidebar,
    triggerRefresh: newsFeed.triggerRefresh,
  }
}
