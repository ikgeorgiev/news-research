export type NewsItem = {
  id: number
  title: string
  url: string
  source: string
  provider: string
  summary: string | null
  published_at: string
  first_seen_at: string
  alert_sent_at: string | null
  tickers: string[]
  dedupe_group: string
}

export type GlobalNewsSummary = {
  total: number
  tracked_ids: number[]
  tracked_limit: number
}

export type NewsResponse = {
  items: NewsItem[]
  next_cursor: string | null
  meta: {
    count: number
    limit: number
    sort: string
  }
  global_summary?: GlobalNewsSummary | null
}

export type NewsIdsResponse = {
  ids: number[]
  next_cursor: string | null
}

export type TickerItem = {
  symbol: string
  fund_name: string | null
  sponsor: string | null
  active: boolean
}

export type TickerResponse = {
  items: TickerItem[]
  total: number
}

export type Watchlist = {
  id: string
  name: string
  provider?: string
  q?: string
  tickers: string[]
}

export type PushSubscriptionKeys = {
  p256dh: string
  auth: string
}

export type PushSubscriptionPayload = {
  endpoint: string
  expiration_time: number | null
  keys: PushSubscriptionKeys
}

export type PushWatchlistScope = {
  id: string
  name?: string | null
  tickers?: string[]
  provider?: string | null
  q?: string | null
}

export type PushAlertScopes = {
  include_all_news: boolean
  watchlists: PushWatchlistScope[]
}

export type PushVapidKeyResponse = {
  enabled: boolean
  public_key: string | null
}

export type PushUpsertRequest = {
  subscription: PushSubscriptionPayload
  scopes: PushAlertScopes
  manage_token: string | null
}

export type PushUpsertResponse = {
  id: number
  active: boolean
  created: boolean
  manage_token: string | null
  seeded_last_notified: Record<string, number>
}
