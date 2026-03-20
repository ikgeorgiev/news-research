"use client"

import { useEffect, useRef, useState } from "react"

/**
 * Connects to an SSE endpoint. Calls `onEvent` when a `new-articles` event
 * arrives, but only if the tab is visible. Sets `connected` when the server
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
    if (!enabled) return

    const eventSource = new EventSource(url)

    eventSource.addEventListener("ready", () => setConnected(true))
    eventSource.addEventListener("unavailable", () => setConnected(false))
    eventSource.addEventListener("error", () => setConnected(false))

    eventSource.addEventListener("new-articles", () => {
      if (document.visibilityState === "visible") {
        callbackRef.current()
      }
    })

    return () => {
      eventSource.close()
      setConnected(false)
    }
  }, [enabled, url])

  return connected
}
