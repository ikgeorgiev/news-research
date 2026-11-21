"use client"

import { useEffect, useRef, useState } from "react"

/**
 * Connects to an SSE endpoint. Calls `onEvent` when a `new-articles` event
 * arrives or the subscription becomes ready, but only if the tab is visible.
 * Readiness catches up on notifications missed before/during reconnect.
 * Sets `connected` when the server
 * sends a `ready` event (meaning it has an active LISTEN subscription),
 * so callers can adapt behaviour (e.g. reduce polling frequency).
 */
export function useEventSource(
  url: string,
  onEvent: () => void,
  enabled: boolean,
) {
  const callbackRef = useRef(onEvent)
  const [connected, setConnected] = useState(false)

  useEffect(() => {
    callbackRef.current = onEvent
  }, [onEvent])

  useEffect(() => {
    if (!enabled || typeof EventSource !== "function") return

    let eventSource: EventSource
    try {
      eventSource = new EventSource(url)
    } catch {
      // Browsers can reject the connection before an error event is available.
      // Keeping connected false lets the feed continue polling.
      return
    }
    let active = true
    const refreshVisibleFeed = () => {
      if (active && document.visibilityState === "visible") callbackRef.current()
    }
    const markUnavailable = () => {
      if (active) setConnected(false)
    }

    eventSource.addEventListener("ready", () => {
      if (!active) return
      setConnected(true)
      refreshVisibleFeed()
    })
    eventSource.addEventListener("unavailable", markUnavailable)
    eventSource.addEventListener("error", markUnavailable)
    eventSource.addEventListener("new-articles", refreshVisibleFeed)

    return () => {
      active = false
      eventSource.close()
      setConnected(false)
    }
  }, [enabled, url])

  return connected
}
