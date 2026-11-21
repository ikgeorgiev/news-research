import { afterEach, describe, expect, it, vi } from "vitest"
import { DeliveryCheck, EMPTY_CHECK } from "./delivery-check"
import type { NewsItem } from "./types"

const START = Date.parse("2026-11-09T15:00:00Z")
function article(id: number, after = 0): NewsItem {
  return { id, title: `Fund ${id} distribution`, url: `https://example.com/${id}`, source: "BW", provider: "FinancialContent",
    first_seen_at: new Date(START + after).toISOString(), published_at: new Date(START - 2000).toISOString(),
    alert_sent_at: null, tickers: ["AWF"], summary: null, read_key: String(id), dedupe_group: String(id) }
}
function setup() {
  vi.useFakeTimers()
  let elapsed = 0, skew = 0
  const store = new DeliveryCheck(() => ({ wall: START + elapsed + skew, mono: 1000 + elapsed }))
  return { store, advance: (ms: number) => { elapsed += ms }, skew: (ms: number) => { skew = ms } }
}
afterEach(() => vi.useRealTimers())

describe("local delivery check", () => {
  it("does nothing until explicitly started and uses a stable snapshot", () => {
    const { store } = setup()
    expect(store.getSnapshot()).toBe(EMPTY_CHECK)
    expect(store.beginRequest("automatic")).toBeNull()
    store.signal("sse"); store.viewport(1)
    expect(store.export().events).toEqual([])
    expect(store.getSnapshot()).toBe(EMPTY_CHECK)
  })
  it("keeps first receipt and distinguishes baseline, automatic and manual history", () => {
    const { store, advance } = setup()
    store.start([article(1, -5000)])
    advance(1000)
    const token = store.beginRequest("automatic")
    advance(125)
    store.response(token, [article(1), article(2, 500), article(3, -5000)])
    advance(50); store.viewport(2)
    advance(500); store.response(store.beginRequest("automatic"), [article(2, 500)])
    const rows = store.export().articles
    expect(rows.find(r => r.article_id === 1)?.classification).toBe("baseline")
    expect(rows.find(r => r.article_id === 3)?.classification).toBe("manual_or_history")
    expect(rows.find(r => r.article_id === 2)).toMatchObject({ classification: "automatic_new", receipt_to_viewport_ms: 50,
      received_at: "2026-11-09T15:00:01.125Z" })
    expect(store.getSnapshot()).toMatchObject({ automaticNew: 1, viewportObserved: 1 })
    store.stop()
  })
  it("never counts queued articles as automatic screen delivery", () => {
    const { store, advance } = setup(); store.start([])
    advance(100); store.response(store.beginRequest("queued"), [article(2, 50)])
    advance(5000); store.viewport(2)
    expect(store.getSnapshot()).toMatchObject({ automaticNew: 1, viewportObserved: 0 })
    expect(store.export().articles[0]).toMatchObject({ awaiting_user: true, receipt_to_viewport_ms: 5000 })
    store.stop()
  })
  it("excludes new filter baselines and manual fetches from automatic samples", () => {
    const { store } = setup(); store.start([])
    store.response(store.beginRequest("initial"), [article(1)])
    store.response(store.beginRequest("manual"), [article(2)])
    store.response(store.beginRequest("more"), [article(3)])
    expect(store.getSnapshot().automaticNew).toBe(0)
    store.stop()
  })
  it("rejects a response from a previous session", () => {
    const { store } = setup(); store.start([])
    const old = store.beginRequest("automatic")
    store.stop(); store.start([]); store.response(old, [article(2)])
    expect(store.getSnapshot().articles).toBe(0)
    store.stop()
  })
  it("bounds memory and reports omitted records", () => {
    const { store } = setup(); store.start([])
    store.response(store.beginRequest("automatic"), Array.from({ length: 405 }, (_, id) => article(id)))
    for (let i = 0; i < 1300; i++) store.signal("sse")
    expect(store.export().articles).toHaveLength(400)
    expect(store.export().events).toHaveLength(1200)
    expect(store.getSnapshot().dropped).toBeGreaterThan(5)
    store.stop()
  })
  it("expires even when a suspended timer has not fired", () => {
    const { store, advance } = setup(); store.start([])
    advance(20 * 60 * 1000)
    expect(store.beginRequest("automatic")).toBeNull()
    expect(store.getSnapshot()).toMatchObject({ active: false, stoppedReason: "time_limit" })
  })
  it("stops on its timer and does not persist an active session", () => {
    const { store } = setup(); store.start([])
    vi.advanceTimersByTime(20 * 60 * 1000)
    expect(store.getSnapshot().active).toBe(false)
    expect(new DeliveryCheck().getSnapshot()).toBe(EMPTY_CHECK)
  })
  it("uses monotonic local duration and flags wall clock changes", () => {
    const { store, advance, skew } = setup(); store.start([])
    store.response(store.beginRequest("automatic"), [article(2)])
    advance(50); skew(60_000); store.viewport(2)
    expect(store.export().articles[0].receipt_to_viewport_ms).toBe(50)
    expect(store.export().clock_warning).toBe(true)
    store.stop()
  })
  it("unsubscribes cleanly and clears exported metadata", () => {
    const { store } = setup(); const listener = vi.fn(); const off = store.subscribe(listener)
    store.start([article(1)]); off(); listener.mockClear(); store.clear()
    expect(listener).not.toHaveBeenCalled()
    expect(store.export().articles).toEqual([])
    expect(store.getSnapshot()).toBe(EMPTY_CHECK)
  })
})
