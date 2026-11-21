import type { NewsItem } from "./types"

const DURATION_MS = 20 * 60 * 1000
const MAX_ROWS = 400
const MAX_EVENTS = 1200
type Clock = { wall: number; mono: number }
type ResponseKind = "initial" | "automatic" | "manual" | "more" | "queued"
type Row = {
  article_id: number; title: string; url: string; provider: string
  publication_label: string; server_first_seen_label: string; server_alert_label: string | null
  classification: "baseline" | "automatic_new" | "manual_or_history"
  received_at: string; received_mono_ms: number; request_id: number | null
  awaiting_user: boolean; viewport_at: string | null; receipt_to_viewport_ms: number | null
}
type RequestToken = { session: number; id: number; kind: ResponseKind; started: Clock }
type Event = { type: string; at: string; elapsed_ms: number; request_id?: number; detail?: string }
export type DeliveryCheckSnapshot = {
  active: boolean; startedAt: string | null; expiresAt: string | null
  articles: number; automaticNew: number; viewportObserved: number; dropped: number
  stoppedReason: string | null
}
export const EMPTY_CHECK: DeliveryCheckSnapshot = Object.freeze({ active: false, startedAt: null,
  expiresAt: null, articles: 0, automaticNew: 0, viewportObserved: 0, dropped: 0, stoppedReason: null })
const clock = (): Clock => ({ wall: Date.now(), mono: performance.now() })
const round = (value: number) => Math.round(value * 1000) / 1000

/** Optional local instrumentation of the existing app. No fetch, socket or storage. */
export class DeliveryCheck {
  private listeners = new Set<() => void>()
  private snapshot: DeliveryCheckSnapshot = EMPTY_CHECK
  private rows = new Map<number, Row>()
  private events: Event[] = []
  private started: Clock | null = null
  private session = 0
  private requestId = 0
  private dropped = 0
  private maxClockDrift = 0
  private timer: ReturnType<typeof setTimeout> | null = null

  constructor(private now: () => Clock = clock) {}
  subscribe = (listener: () => void) => { this.listeners.add(listener); return () => { this.listeners.delete(listener) } }
  getSnapshot = () => this.snapshot
  private emit() {
    const rows = [...this.rows.values()]
    this.snapshot = { ...this.snapshot, articles: rows.length, dropped: this.dropped,
      automaticNew: rows.filter(r => r.classification === "automatic_new").length,
      viewportObserved: rows.filter(r => r.classification === "automatic_new" && r.viewport_at !== null && !r.awaiting_user).length }
    this.listeners.forEach(listener => listener())
  }
  start(items: NewsItem[]) {
    if (this.snapshot.active) return
    this.clear()
    this.session++
    this.started = this.now()
    this.snapshot = { ...EMPTY_CHECK, active: true, startedAt: new Date(this.started.wall).toISOString(),
      expiresAt: new Date(this.started.wall + DURATION_MS).toISOString() }
    this.addItems(items, "initial", null, this.started)
    this.event("session_started")
    this.timer = setTimeout(() => this.stop("time_limit"), DURATION_MS)
    this.emit()
  }
  stop(reason = "manual") {
    if (this.timer !== null) clearTimeout(this.timer)
    this.timer = null
    if (!this.snapshot.active) return
    this.event("session_stopped", undefined, reason)
    this.snapshot = { ...this.snapshot, active: false, stoppedReason: reason }
    this.emit()
  }
  clear() {
    if (this.timer !== null) clearTimeout(this.timer)
    this.timer = null
    this.snapshot = EMPTY_CHECK
    this.rows.clear(); this.events = []; this.started = null
    this.requestId = 0; this.dropped = 0; this.maxClockDrift = 0
    this.listeners.forEach(listener => listener())
  }
  private active() {
    if (!this.snapshot.active || !this.started) return false
    if (this.now().mono - this.started.mono >= DURATION_MS) { this.stop("time_limit"); return false }
    return true
  }
  private event(type: string, requestId?: number, detail?: string) {
    if (!this.started) return
    if (this.events.length >= MAX_EVENTS) { this.dropped++; return }
    const now = this.now()
    this.maxClockDrift = Math.max(this.maxClockDrift, Math.abs((now.wall - this.started.wall) - (now.mono - this.started.mono)))
    this.events.push({ type, at: new Date(now.wall).toISOString(), elapsed_ms: round(now.mono - this.started.mono),
      ...(requestId === undefined ? {} : { request_id: requestId }), ...(detail ? { detail } : {}) })
  }
  signal(type: "sse" | "visible" | "hidden") {
    if (!this.active()) return
    this.event(type); this.emit()
  }
  beginRequest(kind: ResponseKind): RequestToken | null {
    if (!this.active()) return null
    const token = { session: this.session, id: ++this.requestId, kind, started: this.now() }
    this.event("request_started", token.id, kind)
    return token
  }
  response(token: RequestToken | null, items: NewsItem[]) {
    if (!token || !this.active() || token.session !== this.session) return
    this.addItems(items, token.kind, token.id, this.now())
    this.event("response_received", token.id, token.kind)
    this.emit()
  }
  private addItems(items: NewsItem[], kind: ResponseKind, requestId: number | null, at: Clock) {
    for (const item of items) {
      if (this.rows.has(item.id)) continue // Never overwrite an independent first sighting.
      if (this.rows.size >= MAX_ROWS) { this.dropped++; continue }
      const firstSeen = Date.parse(item.first_seen_at)
      const isNew = (kind === "automatic" || kind === "queued") && this.started !== null
        && Number.isFinite(firstSeen) && firstSeen >= this.started.wall
      this.rows.set(item.id, { article_id: item.id, title: item.title.slice(0, 1000), url: item.url.slice(0, 2000),
        provider: item.provider, publication_label: item.published_at, server_first_seen_label: item.first_seen_at,
        server_alert_label: item.alert_sent_at, classification: kind === "initial" ? "baseline" : isNew ? "automatic_new" : "manual_or_history",
        received_at: new Date(at.wall).toISOString(), received_mono_ms: at.mono, request_id: requestId,
        awaiting_user: kind === "queued", viewport_at: null, receipt_to_viewport_ms: null })
    }
  }
  viewport(articleId: number) {
    if (!this.active()) return
    const row = this.rows.get(articleId)
    if (!row || row.viewport_at !== null) return
    const now = this.now()
    row.viewport_at = new Date(now.wall).toISOString()
    row.receipt_to_viewport_ms = round(Math.max(0, now.mono - row.received_mono_ms))
    this.event("viewport_after_two_frames", row.request_id ?? undefined, String(articleId))
    this.emit()
  }
  export() {
    return { version: 1, type: "cef_local_delivery_check", generated_at: new Date(this.now().wall).toISOString(),
      session: this.snapshot, limits: { duration_ms: DURATION_MS, article_rows: MAX_ROWS, events: MAX_EVENTS },
      max_wall_vs_monotonic_drift_ms: round(this.maxClockDrift), clock_warning: this.maxClockDrift > 2000,
      limitations: ["Local response receipt and viewport visibility after two animation frames; not physical display or human perception.",
        "Publication, server first-seen and server alert values are labels, not measured first public availability or server commit timestamps.",
        "Browser/server clock synchronization is unverified. Local durations use the monotonic browser clock.",
        "Baseline, manual/history responses and articles awaiting user action are excluded from automatic viewport counts.",
        "No additional requests, SSE observer or remote telemetry. This export contains public article metadata from this tab."],
      articles: [...this.rows.values()].map(({ received_mono_ms: _mono, ...row }) => row), events: [...this.events] }
  }
}

export const deliveryCheck = new DeliveryCheck()
