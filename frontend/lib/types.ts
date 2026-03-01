export type NewsItem = {
  id: number
  title: string
  url: string
  source: string
  provider: string
  summary: string | null
  published_at: string
  tickers: string[]
  dedupe_group: string
}

export type NewsResponse = {
  items: NewsItem[]
  next_cursor: string | null
  meta: {
    count: number
    limit: number
    sort: string
  }
}

export type NewsCountResponse = {
  total: number
}

export type NewsIdsResponse = {
  ids: number[]
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
