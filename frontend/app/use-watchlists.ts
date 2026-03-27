"use client"

import { FormEvent, MouseEvent, useEffect, useMemo, useState } from "react"

import { persistJson, readJson } from "@/lib/local-storage"
import { Watchlist } from "@/lib/types"

import { createWatchlistId } from "./page-helpers"

const DEFAULT_WATCHLISTS: Watchlist[] = [
  { id: "all", name: "All News", tickers: [] },
]

export type ContextMenuState = { watchlistId: string; x: number; y: number } | null

function normalizeStoredWatchlists(value: unknown): Watchlist[] | null {
  if (!Array.isArray(value)) return []

  return value
    .filter((item): item is Partial<Watchlist> & { id: unknown; name: unknown } => (
      typeof item === "object" && item !== null && "id" in item && "name" in item
    ))
    .map((item) => {
      const tickers = Array.isArray(item.tickers)
        ? Array.from(
            new Set(
              item.tickers
                .filter((symbol): symbol is string => typeof symbol === "string")
                .map((symbol) => symbol.trim().toUpperCase())
                .filter((symbol) => symbol.length > 0)
            )
          )
        : []

      return {
        id: String(item.id),
        name: String(item.name),
        provider:
          typeof item.provider === "string" && item.provider.trim().length > 0
            ? item.provider.trim()
            : undefined,
        q:
          typeof item.q === "string" && item.q.trim().length > 0
            ? item.q.trim()
            : undefined,
        tickers,
      }
    })
    .filter((item) => item.id === "all" || item.tickers.length > 0 || Boolean(item.provider) || Boolean(item.q))
}

export function useWatchlists({
  onSelectWatchlist,
}: {
  onSelectWatchlist?: (watchlist?: Watchlist) => void
}) {
  const [customWatchlists, setCustomWatchlists] = useState<Watchlist[]>([])
  const [activeWatchlistId, setActiveWatchlistId] = useState<string>("all")
  const [isCreatingWatchlist, setIsCreatingWatchlist] = useState(false)
  const [newWatchlistName, setNewWatchlistName] = useState("")
  const [newWatchlistProvider, setNewWatchlistProvider] = useState("")
  const [newWatchlistQuery, setNewWatchlistQuery] = useState("")
  const [selectedTickers, setSelectedTickers] = useState<Set<string>>(new Set())
  const [contextMenu, setContextMenu] = useState<ContextMenuState>(null)
  const [renamingWatchlistId, setRenamingWatchlistId] = useState<string | null>(null)
  const [renameValue, setRenameValue] = useState("")

  const activeWatchlist = useMemo(() => {
    return DEFAULT_WATCHLISTS.find((watchlist) => watchlist.id === activeWatchlistId) ||
      customWatchlists.find((watchlist) => watchlist.id === activeWatchlistId)
  }, [activeWatchlistId, customWatchlists])

  useEffect(() => {
    try {
      setCustomWatchlists(readJson("customWatchlists", normalizeStoredWatchlists) ?? [])
    } catch (error) {
      console.error("Failed to parse stored watchlists", error)
      setCustomWatchlists([])
    }
  }, [])

  useEffect(() => {
    persistJson("customWatchlists", customWatchlists)
  }, [customWatchlists])

  useEffect(() => {
    if (!contextMenu) return

    const handleClick = () => setContextMenu(null)
    const handleKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setContextMenu(null)
      }
    }

    document.addEventListener("click", handleClick)
    document.addEventListener("contextmenu", handleClick)
    document.addEventListener("keydown", handleKey)

    return () => {
      document.removeEventListener("click", handleClick)
      document.removeEventListener("contextmenu", handleClick)
      document.removeEventListener("keydown", handleKey)
    }
  }, [contextMenu])

  const selectWatchlist = (watchlistId: string, watchlist?: Watchlist) => {
    setActiveWatchlistId(watchlistId)
    onSelectWatchlist?.(watchlist)
  }

  const handleCreateWatchlist = (event: FormEvent) => {
    event.preventDefault()
    const name = newWatchlistName.trim()
    const provider = newWatchlistProvider.trim() || undefined
    const q = newWatchlistQuery.trim() || undefined
    const tickers = Array.from(selectedTickers)
    if (!name || (tickers.length === 0 && !provider && !q)) return

    const watchlist: Watchlist = {
      id: createWatchlistId(),
      name,
      provider,
      q,
      tickers,
    }

    setCustomWatchlists((previous) => [...previous, watchlist])
    setNewWatchlistName("")
    setNewWatchlistProvider("")
    setNewWatchlistQuery("")
    setSelectedTickers(new Set())
    setIsCreatingWatchlist(false)
    selectWatchlist(watchlist.id, watchlist)
  }

  const handleDeleteWatchlist = (watchlistId: string) => {
    setCustomWatchlists((previous) => previous.filter((watchlist) => watchlist.id !== watchlistId))
    if (activeWatchlistId === watchlistId) {
      selectWatchlist("all")
    }
  }

  const toggleTickerSelection = (symbol: string) => {
    setSelectedTickers((previous) => {
      const next = new Set(previous)
      if (next.has(symbol)) next.delete(symbol)
      else next.add(symbol)
      return next
    })
  }

  const handleWatchlistContextMenu = (event: MouseEvent, watchlistId: string) => {
    event.preventDefault()
    event.stopPropagation()
    setContextMenu({ watchlistId, x: event.clientX, y: event.clientY })
  }

  const closeContextMenu = () => setContextMenu(null)

  const handleStartRename = (watchlistId: string) => {
    const watchlist = customWatchlists.find((item) => item.id === watchlistId)
    if (!watchlist) return
    setRenamingWatchlistId(watchlistId)
    setRenameValue(watchlist.name)
    closeContextMenu()
  }

  const handleFinishRename = () => {
    if (renamingWatchlistId && renameValue.trim()) {
      setCustomWatchlists((previous) =>
        previous.map((watchlist) =>
          watchlist.id === renamingWatchlistId
            ? { ...watchlist, name: renameValue.trim() }
            : watchlist
        )
      )
    }
    setRenamingWatchlistId(null)
    setRenameValue("")
  }

  const handleContextDelete = (watchlistId: string) => {
    handleDeleteWatchlist(watchlistId)
    closeContextMenu()
  }

  return {
    activeWatchlist,
    activeWatchlistId,
    closeContextMenu,
    contextMenu,
    customWatchlists,
    handleContextDelete,
    handleCreateWatchlist,
    handleDeleteWatchlist,
    handleFinishRename,
    handleStartRename,
    handleWatchlistContextMenu,
    isCreatingWatchlist,
    newWatchlistName,
    newWatchlistProvider,
    newWatchlistQuery,
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
    toggleTickerSelection,
  }
}
