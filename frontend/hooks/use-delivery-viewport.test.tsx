import { useRef } from "react"
import { act, cleanup, render } from "@testing-library/react"
import { afterEach, expect, it, vi } from "vitest"
import { deliveryCheck } from "@/lib/delivery-check"
import { useDeliveryViewport } from "./use-delivery-viewport"
import type { NewsItem } from "@/lib/types"

const item = { id: 2, title: "Fund distribution", url: "https://example.com/2", provider: "BW", published_at: "2026-11-09T15:00:00Z", first_seen_at: "2026-11-09T15:00:01Z", alert_sent_at: null } as NewsItem
const items = [item]
let intersect: (entries: { target: Element; isIntersecting: boolean }[]) => void
let frameId = 0
let frames = new Map<number, FrameRequestCallback>()
let disconnect = vi.fn()
function Harness() {
  const ref = useRef<HTMLElement | null>(null)
  useDeliveryViewport(ref, items)
  return <section ref={ref}><article data-news-id="2">Headline</article></section>
}
function setup() {
  vi.useFakeTimers(); vi.setSystemTime(new Date("2026-11-09T15:00:00Z"))
  frames = new Map(); disconnect = vi.fn()
  vi.stubGlobal("IntersectionObserver", class {
    constructor(callback: typeof intersect) { intersect = callback }
    observe() {} disconnect = disconnect
  })
  vi.stubGlobal("requestAnimationFrame", (callback: FrameRequestCallback) => { frames.set(++frameId, callback); return frameId })
  vi.stubGlobal("cancelAnimationFrame", (id: number) => frames.delete(id))
  vi.spyOn(document, "visibilityState", "get").mockReturnValue("visible")
  vi.spyOn(Element.prototype, "getBoundingClientRect").mockReturnValue({ top: 0, left: 0, right: 600, bottom: 100, width: 600, height: 100, x: 0, y: 0, toJSON() {} })
  vi.spyOn(window, "getComputedStyle").mockReturnValue({ visibility: "visible", display: "block", opacity: "1" } as CSSStyleDeclaration)
  const view = render(<Harness />)
  act(() => { deliveryCheck.start([]); deliveryCheck.response(deliveryCheck.beginRequest("automatic"), items) })
  return { ...view, row: view.container.querySelector("article")! }
}
function frame() { const callbacks = [...frames.values()]; frames.clear(); act(() => callbacks.forEach(callback => callback(1))) }
afterEach(() => { cleanup(); deliveryCheck.clear(); vi.unstubAllGlobals(); vi.restoreAllMocks(); vi.useRealTimers() })

it("requires an intersection and two frames before recording a row", () => {
  const { row } = setup()
  expect(deliveryCheck.getSnapshot().viewportObserved).toBe(0)
  act(() => intersect([{ target: row, isIntersecting: true }]))
  frame(); expect(deliveryCheck.getSnapshot().viewportObserved).toBe(0)
  frame(); expect(deliveryCheck.getSnapshot().viewportObserved).toBe(1)
})
it("does not record a hidden tab or a row covered by the article reader", () => {
  const { row } = setup()
  vi.spyOn(document, "visibilityState", "get").mockReturnValue("hidden")
  act(() => intersect([{ target: row, isIntersecting: true }]))
  frame(); frame(); expect(deliveryCheck.getSnapshot().viewportObserved).toBe(0)
  vi.spyOn(document, "visibilityState", "get").mockReturnValue("visible")
  const dialog = document.createElement("dialog"); dialog.setAttribute("open", ""); document.body.appendChild(dialog)
  act(() => intersect([{ target: row, isIntersecting: true }]))
  frame(); frame(); expect(deliveryCheck.getSnapshot().viewportObserved).toBe(0)
  dialog.remove()
})
it("rechecks geometry and cancels pending observations on unmount", () => {
  const { row, unmount } = setup()
  act(() => intersect([{ target: row, isIntersecting: true }]))
  vi.spyOn(row, "getBoundingClientRect").mockReturnValue({ top: 2000, left: 0, bottom: 2100, right: 600, width: 600, height: 100 } as DOMRect)
  frame(); frame(); expect(deliveryCheck.getSnapshot().viewportObserved).toBe(0)
  act(() => intersect([{ target: row, isIntersecting: true }]))
  unmount(); expect(disconnect).toHaveBeenCalled(); expect(frames.size).toBe(0)
})
