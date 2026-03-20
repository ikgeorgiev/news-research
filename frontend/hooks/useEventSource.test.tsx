import { act, renderHook } from "@testing-library/react"
import { beforeEach, describe, expect, it, vi } from "vitest"

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
