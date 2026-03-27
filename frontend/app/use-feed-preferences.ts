"use client"

import { MouseEvent, useEffect, useMemo, useState } from "react"

import { fetchNewsIds } from "@/lib/api"
import { persistJson, persistValue, readEnumValue, readJson } from "@/lib/local-storage"
import { markReadIdsByQuery, ReadQueryParams, trimIdSet } from "@/lib/read-state"
import { NewsItem } from "@/lib/types"

import { mergeUniqueIds, mergeUniqueNewsItems, toSafeExternalUrl } from "./page-helpers"

const MAX_PERSISTED_READ_IDS = 20_000
const NEWS_IDS_PAGE_SIZE = 1000
const VIEW_MODES = ["list", "full"] as const

function parseStoredReadIds(value: unknown): number[] | null {
  if (!Array.isArray(value)) return null
  return value.filter((id): id is number => Number.isInteger(id))
}

export function useFeedPreferences({
  items,
  pendingNewItems,
  totalCount,
  globalTrackedIds,
}: {
  items: NewsItem[]
  pendingNewItems: NewsItem[]
  totalCount: number
  globalTrackedIds: number[]
}) {
  const [readIds, setReadIds] = useState<Set<number>>(new Set())
  const [viewMode, setViewMode] = useState<"list" | "full">("list")
  const [expandedIds, setExpandedIds] = useState<Set<number>>(new Set())
  const [mounted, setMounted] = useState(false)

  useEffect(() => {
    try {
      const storedReadIds = readJson("readNewsIds", parseStoredReadIds)
      if (storedReadIds) {
        setReadIds(trimIdSet(new Set(storedReadIds), MAX_PERSISTED_READ_IDS))
      }

      const storedViewMode = readEnumValue("newsViewMode", VIEW_MODES)
      if (storedViewMode) {
        setViewMode(storedViewMode)
      }
    } catch (error) {
      console.error("Failed to parse local storage", error)
    } finally {
      setMounted(true)
    }
  }, [])

  useEffect(() => {
    persistJson("readNewsIds", Array.from(readIds))
  }, [readIds])

  useEffect(() => {
    persistValue("newsViewMode", viewMode)
  }, [viewMode])

  const trackedUnreadItems = useMemo(
    () => mergeUniqueNewsItems(items, pendingNewItems),
    [items, pendingNewItems]
  )

  const trackedUnreadIds = useMemo(
    () => mergeUniqueIds(
      items.map((item) => item.id),
      pendingNewItems.map((item) => item.id),
      globalTrackedIds
    ),
    [items, pendingNewItems, globalTrackedIds]
  )

  const unreadCount = useMemo(() => {
    let validReadCount = 0
    for (const id of trackedUnreadIds) {
      if (readIds.has(id)) validReadCount++
    }
    if (totalCount > 0) {
      const untrackedReads = readIds.size - validReadCount
      const untrackedArticles = Math.max(0, totalCount - trackedUnreadIds.length)
      const totalReads = validReadCount + Math.min(untrackedReads, untrackedArticles)
      return Math.max(0, totalCount - totalReads)
    }
    return trackedUnreadIds.length - validReadCount
  }, [trackedUnreadIds, readIds, totalCount])

  const addReadIds = (ids: Iterable<number>) => {
    setReadIds((previous) => {
      const next = new Set(previous)
      for (const id of ids) {
        next.add(id)
      }
      return trimIdSet(next, MAX_PERSISTED_READ_IDS)
    })
  }

  const toggleRead = (id: number, event?: MouseEvent) => {
    if (event) {
      event.stopPropagation()
      event.preventDefault()
    }

    setReadIds((previous) => {
      const next = new Set(previous)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return trimIdSet(next, MAX_PERSISTED_READ_IDS)
    })
  }

  const markAsReadAndOpen = (item: NewsItem, event: MouseEvent) => {
    event.stopPropagation()
    const safeUrl = toSafeExternalUrl(item.url)
    if (!safeUrl) {
      event.preventDefault()
      return
    }
    addReadIds([item.id])
  }

  const toggleSummary = (item: NewsItem) => {
    addReadIds([item.id])

    if (viewMode === "full") return

    setExpandedIds((previous) => {
      const next = new Set(previous)
      if (next.has(item.id)) next.delete(item.id)
      else next.add(item.id)
      return next
    })
  }

  const markReadByQuery = async (params: ReadQueryParams) => {
    await markReadIdsByQuery({
      params,
      fetchIds: fetchNewsIds,
      setReadIds,
      pageSize: NEWS_IDS_PAGE_SIZE,
      maxPersistedIds: MAX_PERSISTED_READ_IDS,
    })
  }

  return {
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
  }
}
