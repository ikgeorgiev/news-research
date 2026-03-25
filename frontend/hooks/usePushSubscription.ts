"use client"

import { useEffect, useMemo, useState } from "react"

import {
  disablePushNotifications,
  enablePushNotifications,
  getPushStatus,
  isPushSupported,
  syncPushScopes,
} from "@/lib/push"
import { persistJson, persistValue } from "@/lib/local-storage"
import { PushAlertScopes, Watchlist } from "@/lib/types"

const ALERT_INCLUDE_ALL_STORAGE_KEY = "alertIncludeAllNews"
const ALERT_WATCHLIST_IDS_STORAGE_KEY = "alertWatchlistIds"

export function usePushSubscription({
  customWatchlists,
  mounted,
}: {
  customWatchlists: Watchlist[]
  mounted: boolean
}) {
  const [pushSubscribed, setPushSubscribed] = useState(false)
  const [pushError, setPushError] = useState<string | null>(null)
  const [alertIncludeAllNews, setAlertIncludeAllNews] = useState(true)
  const [alertWatchlistIds, setAlertWatchlistIds] = useState<Set<string>>(new Set())

  const alertWatchlistKey = useMemo(
    () => Array.from(alertWatchlistIds).sort().join(","),
    [alertWatchlistIds]
  )

  const activePushScopeNames = useMemo(() => {
    const names: string[] = []
    if (alertIncludeAllNews) names.push("All News")
    if (alertWatchlistIds.size > 0) {
      for (const watchlist of customWatchlists) {
        if (alertWatchlistIds.has(watchlist.id)) {
          names.push(watchlist.name)
        }
      }
    }
    return names
  }, [alertIncludeAllNews, alertWatchlistIds, customWatchlists])

  const activePushScopeCount = activePushScopeNames.length

  const pushScopes = useMemo<PushAlertScopes>(() => {
    const watchlists = Array.from(alertWatchlistIds)
      .map((watchlistId) => customWatchlists.find((watchlist) => watchlist.id === watchlistId))
      .filter((watchlist): watchlist is Watchlist => Boolean(watchlist))
      .map((watchlist) => ({
        id: watchlist.id,
        name: watchlist.name,
        tickers: watchlist.tickers.length > 0 ? [...watchlist.tickers] : undefined,
        provider: watchlist.provider || undefined,
        q: watchlist.q || undefined,
      }))

    return {
      include_all_news: alertIncludeAllNews,
      watchlists,
    }
  }, [alertIncludeAllNews, alertWatchlistKey, customWatchlists])

  useEffect(() => {
    try {
      const storedAlertIncludeAll = localStorage.getItem(ALERT_INCLUDE_ALL_STORAGE_KEY)
      if (storedAlertIncludeAll !== null) {
        setAlertIncludeAllNews(storedAlertIncludeAll === "true")
      }

      const storedAlertWatchlistIds = localStorage.getItem(ALERT_WATCHLIST_IDS_STORAGE_KEY)
      if (storedAlertWatchlistIds) {
        const parsed = JSON.parse(storedAlertWatchlistIds)
        if (Array.isArray(parsed)) {
          const validIds = parsed.filter((id): id is string => typeof id === "string" && id.length > 0)
          setAlertWatchlistIds(new Set(validIds))
        }
      }
    } catch (err) {
      console.error("Failed to parse push subscription storage", err)
    }
  }, [])

  useEffect(() => {
    let cancelled = false

    const refreshPushStatus = async () => {
      if (!isPushSupported()) {
        if (!cancelled) {
          setPushSubscribed(false)
        }
        return
      }
      try {
        const status = await getPushStatus()
        if (cancelled) return
        setPushSubscribed(status.subscribed)
      } catch {
        if (!cancelled) {
          setPushSubscribed(false)
        }
      }
    }

    void refreshPushStatus()
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    if (!mounted) return
    persistValue(ALERT_INCLUDE_ALL_STORAGE_KEY, String(alertIncludeAllNews))
  }, [alertIncludeAllNews, mounted])

  useEffect(() => {
    if (!mounted) return
    persistJson(ALERT_WATCHLIST_IDS_STORAGE_KEY, Array.from(alertWatchlistIds))
  }, [alertWatchlistIds, mounted])

  useEffect(() => {
    if (!mounted) return
    const validIds = new Set(customWatchlists.map((watchlist) => watchlist.id))
    setAlertWatchlistIds((previous) => {
      const filtered = Array.from(previous).filter((id) => validIds.has(id))
      if (filtered.length === previous.size) return previous
      return new Set(filtered)
    })
  }, [customWatchlists, mounted])

  useEffect(() => {
    if (!mounted || !pushSubscribed) return

    const run = async () => {
      try {
        await syncPushScopes(pushScopes)
      } catch {
        // Keep local UI responsive even if backend sync fails.
      }
    }

    void run()
  }, [mounted, pushSubscribed, pushScopes])

  const isPushScopeEnabled = (watchlistId: string): boolean => {
    if (watchlistId === "all") return alertIncludeAllNews
    return alertWatchlistIds.has(watchlistId)
  }

  const togglePushScope = (watchlistId: string) => {
    if (watchlistId === "all") {
      setAlertIncludeAllNews((previous) => !previous)
      return
    }
    setAlertWatchlistIds((previous) => {
      const next = new Set(previous)
      if (next.has(watchlistId)) next.delete(watchlistId)
      else next.add(watchlistId)
      return next
    })
  }

  const togglePushSubscription = async () => {
    if (pushSubscribed) {
      setPushError(null)
      try {
        await disablePushNotifications()
      } catch {
        setPushError("Failed to disable push notifications")
      }
      setPushSubscribed(false)
      return
    }

    setPushError(null)
    try {
      const result = await enablePushNotifications(pushScopes)
      if (result.pushActive) {
        setPushSubscribed(true)
        return
      }
      if (!result.permissionGranted) {
        setPushError("Browser notification permission denied")
      } else {
        setPushError(
          result.reason === "push-disabled"
            ? "Push not configured on server (VAPID keys missing)"
            : "Push notifications not supported in this browser"
        )
      }
    } catch (err) {
      setPushError(err instanceof Error ? err.message : "Failed to enable push notifications")
    }
  }

  return {
    alertIncludeAllNews,
    activePushScopeCount,
    activePushScopeNames,
    isPushScopeEnabled,
    pushError,
    pushSubscribed,
    togglePushScope,
    togglePushSubscription,
  }
}
