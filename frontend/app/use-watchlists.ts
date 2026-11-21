"use client"

import { FormEvent, KeyboardEvent as ReactKeyboardEvent, MouseEvent, useEffect, useMemo, useRef, useState } from "react"

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
  const contextTrigger = useRef<HTMLButtonElement | null>(null)
  const [renamingWatchlistId, setRenamingWatchlistId] = useState<string | null>(null)
  const [renameValue, setRenameValue] = useState("")
  const [hydrated, setHydrated] = useState(false)

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
    } finally {
      setHydrated(true)
    }
  }, [])

  useEffect(() => {
    if (!hydrated) return
    persistJson("customWatchlists", customWatchlists)
  }, [customWatchlists, hydrated])

  useEffect(() => {
    if (!contextMenu) return

    const handleClick = () => setContextMenu(null)
    const handleKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setContextMenu(null)
        contextTrigger.current?.focus()
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

  const handleWatchlistContextMenu = (event: MouseEvent | ReactKeyboardEvent, watchlistId: string) => {
    if ("key" in event && event.key !== "ContextMenu" && !(event.shiftKey && event.key === "F10")) return
    event.preventDefault()
    event.stopPropagation()
    const row = event.currentTarget
    contextTrigger.current = row.querySelector<HTMLButtonElement>(".watchlist-select")
    const rect = (contextTrigger.current ?? row).getBoundingClientRect()
    const point = "clientX" in event && (event.clientX || event.clientY)
      ? { x: event.clientX, y: event.clientY }
      : { x: rect.left, y: rect.bottom }
    setContextMenu({ watchlistId, ...point })
  }

  const closeContextMenu = () => {
    setContextMenu(null)
    contextTrigger.current?.focus()
  }

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
