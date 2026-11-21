import { act, cleanup, renderHook } from "@testing-library/react"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"

import { useEventSource } from "./useEventSource"

type EventHandler = () => void

class MockEventSource {
  static instances: MockEventSource[] = []

  closed = false
  listeners = new Map<string, EventHandler[]>()

  constructor(public readonly url: string) {
    MockEventSource.instances.push(this)
  }

  addEventListener(type: string, listener: EventHandler) {
    const listeners = this.listeners.get(type) ?? []
    listeners.push(listener)
    this.listeners.set(type, listeners)
  }

  emit(type: string) {
    for (const listener of this.listeners.get(type) ?? []) {
      listener()
    }
  }

  close() {
    this.closed = true
  }
}

describe("useEventSource", () => {
  afterEach(() => {
    cleanup()
    vi.unstubAllGlobals()
  })

  beforeEach(() => {
    MockEventSource.instances = []
    vi.unstubAllGlobals()
    vi.stubGlobal("EventSource", MockEventSource)
    Object.defineProperty(document, "visibilityState", {
      configurable: true,
      value: "visible",
    })
  })

  it("marks the connection ready and clears it when the stream becomes unavailable", () => {
    const onEvent = vi.fn()
    const { result } = renderHook(() =>
      useEventSource("/api/v1/events/news", onEvent, true)
    )

    const eventSource = MockEventSource.instances[0]

    act(() => {
      eventSource.emit("ready")
    })
    expect(result.current).toBe(true)

    act(() => {
      eventSource.emit("unavailable")
    })
    expect(result.current).toBe(false)
  })

  it("clears the connection state when the stream errors", () => {
    const onEvent = vi.fn()
    const { result } = renderHook(() =>
      useEventSource("/api/v1/events/news", onEvent, true)
    )

    const eventSource = MockEventSource.instances[0]

    act(() => {
      eventSource.emit("ready")
    })
    expect(result.current).toBe(true)

    act(() => {
      eventSource.emit("error")
    })
    expect(result.current).toBe(false)
  })

  it.each(["error", "unavailable"])("catches up after initial subscription and recovery from %s", (event) => {
    const onEvent = vi.fn()
    const { result } = renderHook(() =>
      useEventSource("/api/v1/events/news", onEvent, true)
    )
    const eventSource = MockEventSource.instances[0]

    // A release can arrive between the first HTTP snapshot and LISTEN becoming
    // ready, or during an outage. Neither notification is replayed by SSE.
    act(() => { eventSource.emit("ready") })
    expect(onEvent).toHaveBeenCalledTimes(1)
    act(() => { eventSource.emit(event) })
    expect(result.current).toBe(false)
    expect(onEvent).toHaveBeenCalledTimes(1)
    act(() => { eventSource.emit("ready") })
    expect(result.current).toBe(true)
    expect(onEvent).toHaveBeenCalledTimes(2)
  })

  it("defers catch-up in a hidden tab to the caller's visibility refresh", () => {
    Object.defineProperty(document, "visibilityState", { configurable: true, value: "hidden" })
    const onEvent = vi.fn()
    const { result } = renderHook(() =>
      useEventSource("/api/v1/events/news", onEvent, true)
    )
    act(() => { MockEventSource.instances[0].emit("ready") })
    expect(result.current).toBe(true)
    expect(onEvent).not.toHaveBeenCalled()
  })

  it("uses the latest callback and ignores events queued by a closed connection", () => {
    const firstCallback = vi.fn()
    const nextCallback = vi.fn()
    const { result, rerender, unmount } = renderHook(
      ({ url, onEvent }) => useEventSource(url, onEvent, true),
      { initialProps: { url: "/first", onEvent: firstCallback } }
    )
    const first = MockEventSource.instances[0]
    rerender({ url: "/first", onEvent: nextCallback })
    act(() => { first.emit("ready") })
    expect(firstCallback).not.toHaveBeenCalled()
    expect(nextCallback).toHaveBeenCalledTimes(1)
    expect(MockEventSource.instances).toHaveLength(1)

    rerender({ url: "/second", onEvent: nextCallback })
    expect(first.closed).toBe(true)
    act(() => { first.emit("ready"); first.emit("new-articles") })
    expect(result.current).toBe(false)
    expect(nextCallback).toHaveBeenCalledTimes(1)
    const second = MockEventSource.instances[1]
    act(() => { second.emit("ready") })
    expect(result.current).toBe(true)
    expect(nextCallback).toHaveBeenCalledTimes(2)
    act(() => { first.emit("error") })
    expect(result.current).toBe(true)
    unmount()
    act(() => { second.emit("ready"); second.emit("new-articles") })
    expect(nextCallback).toHaveBeenCalledTimes(2)
  })

  it("only reacts to article events while the tab is visible", () => {
    const onEvent = vi.fn()
    renderHook(() => useEventSource("/api/v1/events/news", onEvent, true))

    const eventSource = MockEventSource.instances[0]

    Object.defineProperty(document, "visibilityState", {
      configurable: true,
      value: "hidden",
    })
    act(() => {
      eventSource.emit("new-articles")
    })

    Object.defineProperty(document, "visibilityState", {
      configurable: true,
      value: "visible",
    })
    act(() => {
      eventSource.emit("new-articles")
    })

    expect(onEvent).toHaveBeenCalledTimes(1)
  })

  it("closes the EventSource and resets state when disabled", () => {
    const onEvent = vi.fn()
    const { result, rerender } = renderHook(
      ({ enabled }: { enabled: boolean }) =>
        useEventSource("/api/v1/events/news", onEvent, enabled),
      { initialProps: { enabled: true } }
    )

    const eventSource = MockEventSource.instances[0]

    act(() => {
      eventSource.emit("ready")
    })
    expect(result.current).toBe(true)

    act(() => {
      rerender({ enabled: false })
    })

    expect(eventSource.closed).toBe(true)
    expect(result.current).toBe(false)
    expect(MockEventSource.instances).toHaveLength(1)
  })
})
