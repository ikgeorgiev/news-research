"use client"

import { FormEvent, MouseEvent, useState } from "react"

import { usePushSubscription } from "@/hooks/usePushSubscription"
import { Watchlist } from "@/lib/types"

import {
  buildWatchlistQueryParams,
  GENERAL_NEWS_PROVIDER,
  STATIC_PROVIDER_OPTIONS,
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
  readKeys: Set<string>
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

type MarkAllReadQueryParams = {
  tickers?: string[]
  provider?: string
  q?: string
  includeUnmappedFromProvider?: string
}

function buildMarkAllReadQueryParams({
  activeWatchlist,
  activeWatchlistId,
  currentProvider,
  currentSearchQuery,
  currentTicker,
  targetWatchlistId,
  watchlist,
}: {
  activeWatchlist?: Watchlist
  activeWatchlistId: string
  currentProvider: string
  currentSearchQuery: string
  currentTicker: string
  targetWatchlistId: string
  watchlist?: Watchlist
}): MarkAllReadQueryParams | null {
  if (targetWatchlistId === "all" && activeWatchlistId !== "all") {
    return { includeUnmappedFromProvider: GENERAL_NEWS_PROVIDER }
  }

  if (targetWatchlistId !== activeWatchlistId && watchlist) {
    const savedParams = buildWatchlistQueryParams(watchlist)
    return savedParams
  }

  const provider = currentProvider.trim() || undefined
  const q = currentSearchQuery.trim() || undefined
  const tickers = currentTicker ? [currentTicker] : undefined

  if (activeWatchlistId === "all") {
    return {
      ...(tickers ? { tickers } : {}),
      ...(provider ? { provider } : {}),
      ...(q ? { q } : {}),
      includeUnmappedFromProvider: !tickers ? GENERAL_NEWS_PROVIDER : undefined,
    }
  }

  const activeParams = buildWatchlistQueryParams(activeWatchlist)
  return {
    tickers: tickers ?? activeParams.tickers,
    ...(provider ? { provider } : {}),
    ...(q ? { q } : {}),
    includeUnmappedFromProvider: !tickers ? activeParams.includeUnmappedFromProvider : undefined,
  }
}

function markAllReadMatchesItem(
  item: { provider: string; summary: string | null; source: string; tickers: string[]; title: string },
  params: MarkAllReadQueryParams
): boolean {
  // Best-effort fallback for API failures; keep this aligned with backend read-query filtering.
  if (params.tickers && params.tickers.length > 0) {
    const hasTickerMatch = item.tickers.some((ticker) => params.tickers?.includes(ticker))
    if (!hasTickerMatch) {
      return false
    }
  }

  if (params.provider) {
    const providerText = params.provider.trim().toLowerCase()
    if (item.provider.trim().toLowerCase() !== providerText) {
      return false
    }
  }

  if (params.includeUnmappedFromProvider) {
    const providerText = params.includeUnmappedFromProvider.trim().toLowerCase()
    const isMapped = item.tickers.length > 0
    if (!isMapped && item.provider.trim().toLowerCase() !== providerText) {
      return false
    }
  }

  if (params.q) {
    const queryText = params.q.trim().toLowerCase()
    const haystack = [item.title, item.summary, item.source, item.provider, ...(item.tickers || [])]
      .filter((v): v is string => typeof v === "string" && v.length > 0)
      .join(" ")
      .toLowerCase()
    if (!haystack.includes(queryText)) {
      return false
    }
  }

  return true
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
    globalTrackedReadKeys: newsFeed.globalTrackedReadKeys,
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

    const activeWatchlist = watchlists.activeWatchlist
    const watchlist = watchlists.customWatchlists.find((item) => item.id === watchlistId)
    const params = buildMarkAllReadQueryParams({
      activeWatchlist,
      activeWatchlistId: watchlists.activeWatchlistId,
      currentProvider: provider,
      currentSearchQuery: searchQuery,
      currentTicker: ticker,
      targetWatchlistId: watchlistId,
      watchlist,
    })
    if (!params) return

    preferences.markReadByQuery(params)
      .catch(() => {
        const matchingKeys = preferences.trackedUnreadItems
          .filter((item) => markAllReadMatchesItem(item, params))
          .map((item) => item.read_key)
        preferences.addReadKeys(matchingKeys)
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
    readKeys: preferences.readKeys,
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
