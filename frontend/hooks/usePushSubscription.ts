"use client"

import { useEffect, useMemo, useRef, useState } from "react"

import {
  disablePushNotifications,
  enablePushNotifications,
  getPushStatus,
  isPushSupported,
  syncPushScopes,
} from "@/lib/push"
import { persistJson, persistValue, readBooleanValue, readJson } from "@/lib/local-storage"
import { PushAlertScopes, Watchlist } from "@/lib/types"
import {
  preparePushWatchlistScope,
  PUSH_SCOPE_LIMITS,
} from "@/lib/watchlist-limits"

const ALERT_INCLUDE_ALL_STORAGE_KEY = "alertIncludeAllNews"
const ALERT_WATCHLIST_IDS_STORAGE_KEY = "alertWatchlistIds"

function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError"
}

function parseStoredAlertWatchlistIds(value: unknown): string[] | null {
  if (!Array.isArray(value)) return null
  return value.filter((id): id is string => typeof id === "string" && id.length > 0)
}

function formatPushScopeSyncError(error: unknown): string {
  const detail = error instanceof Error ? error.message.trim() : ""
  return detail
    ? `Failed to sync push notification scopes: ${detail}`
    : "Failed to sync push notification scopes"
}

function formatPushScopeSelectionError(watchlist: Watchlist, reason: string): string {
  const fullName = watchlist.name.trim() || "Unnamed watchlist"
  const name = Array.from(fullName).slice(0, 80).join("")
  const suffix = name.length < fullName.length ? "…" : ""
  return `Push is off for "${name}${suffix}": ${reason}.`
}

export function usePushSubscription({
  customWatchlists,
  mounted,
}: {
  customWatchlists: Watchlist[]
  mounted: boolean
}) {
  const [pushSubscribed, setPushSubscribed] = useState(false)
  const [pushError, setPushError] = useState<string | null>(null)
  const [pushScopeSelectionError, setPushScopeSelectionError] = useState<string | null>(null)
  const [pushScopeSyncError, setPushScopeSyncError] = useState<string | null>(null)
  const [alertIncludeAllNews, setAlertIncludeAllNews] = useState(true)
  const [alertWatchlistIds, setAlertWatchlistIds] = useState<Set<string>>(new Set())
  const pushStatusErrorLoggedRef = useRef(false)
  const pushStatusRefreshPromiseRef = useRef<Promise<void> | null>(null)
  const pushScopeSyncErrorLoggedRef = useRef(false)
  const pendingPushScopesRef = useRef<PushAlertScopes | null>(null)
  const inFlightPushScopeSyncRef = useRef<Promise<boolean> | null>(null)
  const pushScopeSyncActiveRef = useRef(false)
  const pushScopeSyncGenerationRef = useRef(0)
  const pushScopeSyncRunningRef = useRef(false)
  const pushTransitionRunningRef = useRef(false)

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

  const preparedPushScopes = useMemo(() => {
    const watchlistsById = new Map<string, Watchlist>()
    for (const watchlist of customWatchlists) {
      if (!watchlistsById.has(watchlist.id)) {
        watchlistsById.set(watchlist.id, watchlist)
      }
    }

    const validSelectedIds = new Set<string>()
    const watchlists: PushAlertScopes["watchlists"] = []
    let selectionError: string | null = null
    for (const watchlistId of alertWatchlistIds) {
      const watchlist = watchlistsById.get(watchlistId)
      if (!watchlist) continue

      if (watchlists.length === PUSH_SCOPE_LIMITS.watchlists) {
        selectionError ||= (
          `Only ${PUSH_SCOPE_LIMITS.watchlists} custom watchlists can use push at once.`
        )
        continue
      }

      const prepared = preparePushWatchlistScope(watchlist)
      if (prepared.scope === null) {
        selectionError ||= formatPushScopeSelectionError(watchlist, prepared.error)
        continue
      }

      validSelectedIds.add(watchlistId)
      watchlists.push(prepared.scope)
    }

    return {
      scopes: {
        include_all_news: alertIncludeAllNews,
        watchlists,
      } satisfies PushAlertScopes,
      selectionError,
      validSelectedIds,
    }
  }, [alertIncludeAllNews, alertWatchlistKey, customWatchlists])
  const pushScopes = preparedPushScopes.scopes
  const latestPushScopesRef = useRef(pushScopes)
  latestPushScopesRef.current = pushScopes

  const runPushScopeSyncQueue = async (generation: number): Promise<void> => {
    if (pushScopeSyncRunningRef.current) return
    pushScopeSyncRunningRef.current = true

    try {
      while (
        pushScopeSyncActiveRef.current
        && generation === pushScopeSyncGenerationRef.current
      ) {
        const scopes = pendingPushScopesRef.current
        if (!scopes) break
        pendingPushScopesRef.current = null

        for (let attempt = 0; attempt < 2; attempt += 1) {
          if (
            !pushScopeSyncActiveRef.current
            || generation !== pushScopeSyncGenerationRef.current
          ) {
            return
          }

          let request: Promise<boolean> | null = null
          try {
            request = syncPushScopes(scopes)
            inFlightPushScopeSyncRef.current = request
            const synced = await request
            if (!synced) {
              throw new Error("browser push subscription is unavailable")
            }
            if (
              !pushScopeSyncActiveRef.current
              || generation !== pushScopeSyncGenerationRef.current
            ) {
              return
            }
            pushScopeSyncErrorLoggedRef.current = false
            setPushScopeSyncError(null)
            break
          } catch (error) {
            if (
              isAbortError(error)
              || !pushScopeSyncActiveRef.current
              || generation !== pushScopeSyncGenerationRef.current
            ) {
              return
            }
            if (attempt === 1) {
              setPushScopeSyncError(formatPushScopeSyncError(error))
              if (!pushScopeSyncErrorLoggedRef.current) {
                pushScopeSyncErrorLoggedRef.current = true
                console.warn("Failed to sync push scopes", error)
              }
            }
          } finally {
            if (request && inFlightPushScopeSyncRef.current === request) {
              inFlightPushScopeSyncRef.current = null
            }
          }
        }
      }
    } finally {
      pushScopeSyncRunningRef.current = false
      if (pushScopeSyncActiveRef.current && pendingPushScopesRef.current) {
        void runPushScopeSyncQueue(pushScopeSyncGenerationRef.current)
      }
    }
  }

  useEffect(() => {
    try {
      const storedAlertIncludeAll = readBooleanValue(ALERT_INCLUDE_ALL_STORAGE_KEY)
      if (storedAlertIncludeAll !== null) {
        setAlertIncludeAllNews(storedAlertIncludeAll)
      }

      const storedAlertWatchlistIds = readJson(
        ALERT_WATCHLIST_IDS_STORAGE_KEY,
        parseStoredAlertWatchlistIds
      )
      if (storedAlertWatchlistIds) {
        setAlertWatchlistIds(new Set(storedAlertWatchlistIds))
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
        pushStatusErrorLoggedRef.current = false
        setPushSubscribed(status.subscribed)
      } catch (error) {
        if (isAbortError(error) || cancelled) return
        if (!pushStatusErrorLoggedRef.current) {
          pushStatusErrorLoggedRef.current = true
          console.warn("Failed to refresh push status", error)
        }
        if (!cancelled) {
          setPushSubscribed(false)
        }
      }
    }

    const refreshPromise = refreshPushStatus()
    pushStatusRefreshPromiseRef.current = refreshPromise
    void refreshPromise.finally(() => {
      if (pushStatusRefreshPromiseRef.current === refreshPromise) {
        pushStatusRefreshPromiseRef.current = null
      }
    })
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
    setAlertWatchlistIds((previous) => {
      const filtered = Array.from(previous).filter((id) =>
        preparedPushScopes.validSelectedIds.has(id)
      )
      if (filtered.length === previous.size) return previous
      return new Set(filtered)
    })
    if (preparedPushScopes.selectionError) {
      setPushScopeSelectionError(preparedPushScopes.selectionError)
    }
  }, [alertWatchlistKey, customWatchlists, mounted, preparedPushScopes])

  useEffect(() => {
    if (!mounted || !pushSubscribed) {
      pushScopeSyncActiveRef.current = false
      pendingPushScopesRef.current = null
      pushScopeSyncGenerationRef.current += 1
      setPushScopeSyncError(null)
      return
    }

    pushScopeSyncActiveRef.current = true
    pushScopeSyncGenerationRef.current += 1

    return () => {
      pushScopeSyncActiveRef.current = false
      pendingPushScopesRef.current = null
      pushScopeSyncGenerationRef.current += 1
    }
  }, [mounted, pushSubscribed])

  useEffect(() => {
    if (!mounted || !pushSubscribed || !pushScopeSyncActiveRef.current) return
    pendingPushScopesRef.current = pushScopes
    void runPushScopeSyncQueue(pushScopeSyncGenerationRef.current)
  }, [mounted, pushSubscribed, pushScopes])

  const isPushScopeEnabled = (watchlistId: string): boolean => {
    if (watchlistId === "all") return alertIncludeAllNews
    return alertWatchlistIds.has(watchlistId)
  }

  const togglePushScope = (watchlistId: string) => {
    if (watchlistId === "all") {
      setPushScopeSelectionError(null)
      setAlertIncludeAllNews((previous) => !previous)
      return
    }

    if (alertWatchlistIds.has(watchlistId)) {
      setPushScopeSelectionError(null)
      setAlertWatchlistIds((previous) => {
        const next = new Set(previous)
        next.delete(watchlistId)
        return next
      })
      return
    }

    const watchlist = customWatchlists.find((item) => item.id === watchlistId)
    if (!watchlist) return

    const prepared = preparePushWatchlistScope(watchlist)
    if (prepared.error) {
      setPushScopeSelectionError(
        formatPushScopeSelectionError(watchlist, prepared.error)
      )
      return
    }
    if (alertWatchlistIds.size >= PUSH_SCOPE_LIMITS.watchlists) {
      setPushScopeSelectionError(
        `Only ${PUSH_SCOPE_LIMITS.watchlists} custom watchlists can use push at once.`
      )
      return
    }

    setPushScopeSelectionError(null)
    setAlertWatchlistIds((previous) => new Set(previous).add(watchlistId))
  }

  const togglePushSubscription = async () => {
    if (pushTransitionRunningRef.current) return
    pushTransitionRunningRef.current = true

    try {
      // getPushStatus may be finishing a deferred browser unsubscribe from a
      // prior disable. Enabling before it settles could create a subscription
      // that the stale cleanup immediately removes.
      await pushStatusRefreshPromiseRef.current

      if (pushSubscribed) {
        // Stop accepting or retrying PUTs before starting the DELETE. Otherwise
        // a failed in-flight sync can retry after deletion and recreate the row.
        pushScopeSyncActiveRef.current = false
        pendingPushScopesRef.current = null
        pushScopeSyncGenerationRef.current += 1
        setPushScopeSyncError(null)
        setPushError(null)
        try {
          const inFlightSync = inFlightPushScopeSyncRef.current
          if (inFlightSync) {
            // The generation change prevents retries. Waiting for the issued PUT
            // itself prevents it from committing after DELETE and recreating the
            // subscription row.
            await inFlightSync.catch(() => undefined)
          }
          await disablePushNotifications()
          setPushSubscribed(false)
        } catch {
          setPushError("Failed to disable push notifications")
          if (mounted && pushSubscribed) {
            pushScopeSyncActiveRef.current = true
            pushScopeSyncGenerationRef.current += 1
            pendingPushScopesRef.current = latestPushScopesRef.current
            void runPushScopeSyncQueue(pushScopeSyncGenerationRef.current)
          }
        }
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
    } finally {
      pushTransitionRunningRef.current = false
    }
  }

  return {
    alertIncludeAllNews,
    activePushScopeCount,
    activePushScopeNames,
    isPushScopeEnabled,
    pushError,
    pushScopeSelectionError,
    pushScopeSyncError,
    pushSubscribed,
    togglePushScope,
    togglePushSubscription,
  }
}
