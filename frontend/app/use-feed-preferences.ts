"use client"

import { MouseEvent, useEffect, useMemo, useState } from "react"

import { fetchNewsIds } from "@/lib/api"
import { persistJson, persistValue, readEnumValue, readJson } from "@/lib/local-storage"
import { markReadKeysByQuery, ReadQueryParams, trimSet } from "@/lib/read-state"
import { NewsItem } from "@/lib/types"

import { mergeUniqueIds, mergeUniqueNewsItems, toSafeExternalUrl } from "./page-helpers"

const MAX_PERSISTED_READ_KEYS = 20_000
const NEWS_IDS_PAGE_SIZE = 1000
const VIEW_MODES = ["list", "full"] as const

function parseStoredReadKeys(value: unknown): string[] | null {
  if (!Array.isArray(value)) return null
  return value.filter((key): key is string => typeof key === "string" && key.length > 0)
}

export function useFeedPreferences({
  items,
  pendingNewItems,
  totalCount,
  globalTrackedReadKeys,
}: {
  items: NewsItem[]
  pendingNewItems: NewsItem[]
  totalCount: number
  globalTrackedReadKeys: string[]
}) {
  const [readKeys, setReadKeys] = useState<Set<string>>(new Set())
  const [viewMode, setViewMode] = useState<"list" | "full">("list")
  const [expandedIds, setExpandedIds] = useState<Set<number>>(new Set())
  const [mounted, setMounted] = useState(false)

  useEffect(() => {
    try {
      const storedReadKeys = readJson("readNewsKeys", parseStoredReadKeys)
      if (storedReadKeys) {
        setReadKeys(trimSet(new Set(storedReadKeys), MAX_PERSISTED_READ_KEYS))
      }
    } catch (error) {
      console.error("Failed to parse readNewsKeys from local storage", error)
    }

    try {
      const storedViewMode = readEnumValue("newsViewMode", VIEW_MODES)
      if (storedViewMode) {
        setViewMode(storedViewMode)
      }
    } catch (error) {
      console.error("Failed to parse newsViewMode from local storage", error)
    }

    setMounted(true)
  }, [])

  useEffect(() => {
    if (!mounted) return
    persistJson("readNewsKeys", Array.from(readKeys))
  }, [mounted, readKeys])

  useEffect(() => {
    if (!mounted) return
    persistValue("newsViewMode", viewMode)
  }, [mounted, viewMode])

  const trackedUnreadItems = useMemo(
    () => mergeUniqueNewsItems(items, pendingNewItems),
    [items, pendingNewItems]
  )

  const trackedUnreadKeys = useMemo(
    () => mergeUniqueIds(
      items.map((item) => item.read_key),
      pendingNewItems.map((item) => item.read_key),
      globalTrackedReadKeys
    ),
    [items, pendingNewItems, globalTrackedReadKeys]
  )

  const unreadCount = useMemo(() => {
    let validReadCount = 0
    for (const key of trackedUnreadKeys) {
      if (readKeys.has(key)) validReadCount++
    }
    if (totalCount > 0) {
      const untrackedReads = readKeys.size - validReadCount
      const untrackedArticles = Math.max(0, totalCount - trackedUnreadKeys.length)
      const totalReads = validReadCount + Math.min(untrackedReads, untrackedArticles)
      return Math.max(0, totalCount - totalReads)
    }
    return trackedUnreadKeys.length - validReadCount
  }, [trackedUnreadKeys, readKeys, totalCount])

  const addReadKeys = (keys: Iterable<string>) => {
    setReadKeys((previous) => {
      const next = new Set(previous)
      for (const key of keys) {
        next.add(key)
      }
      return trimSet(next, MAX_PERSISTED_READ_KEYS)
    })
  }

  const toggleRead = (item: NewsItem, event?: MouseEvent) => {
    if (event) {
      event.stopPropagation()
      event.preventDefault()
    }

    setReadKeys((previous) => {
      const next = new Set(previous)
      if (next.has(item.read_key)) next.delete(item.read_key)
      else next.add(item.read_key)
      return trimSet(next, MAX_PERSISTED_READ_KEYS)
    })
  }

  const markAsReadAndOpen = (item: NewsItem, event: MouseEvent) => {
    event.stopPropagation()
    const safeUrl = toSafeExternalUrl(item.url)
    if (!safeUrl) {
      event.preventDefault()
      return
    }
    addReadKeys([item.read_key])
  }

  const toggleSummary = (item: NewsItem) => {
    if (viewMode === "full") return

    setExpandedIds((previous) => {
      const next = new Set(previous)
      if (next.has(item.id)) next.delete(item.id)
      else next.add(item.id)
      return next
    })
  }

  const markReadByQuery = async (params: ReadQueryParams) => {
    await markReadKeysByQuery({
      params,
      fetchIds: fetchNewsIds,
      setReadKeys,
      pageSize: NEWS_IDS_PAGE_SIZE,
      maxPersistedKeys: MAX_PERSISTED_READ_KEYS,
    })
  }

  return {
    addReadKeys,
    expandedIds,
    markAsReadAndOpen,
    markReadByQuery,
    mounted,
    readKeys,
    setViewMode,
    toggleRead,
    toggleSummary,
    trackedUnreadItems,
    unreadCount,
    viewMode,
  }
}
