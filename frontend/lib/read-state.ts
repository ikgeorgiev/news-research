import type { NewsIdsResponse } from "./types"

export type ReadQueryParams = {
  tickers?: string[]
  includeUnmappedFromProvider?: string
}

type FetchNewsIdsPage = (params: ReadQueryParams & {
  cursor?: string
  limit?: number
}) => Promise<NewsIdsResponse>

type SetReadIds = (updater: (previous: Set<number>) => Set<number>) => void


export function trimIdSet(input: Set<number>, maxSize: number): Set<number> {
  if (input.size <= maxSize) return input
  const trimmed = Array.from(input).slice(input.size - maxSize)
  return new Set(trimmed)
}


export async function markReadIdsByQuery({
  params,
  fetchIds,
  setReadIds,
  pageSize,
  maxPersistedIds,
}: {
  params: ReadQueryParams
  fetchIds: FetchNewsIdsPage
  setReadIds: SetReadIds
  pageSize: number
  maxPersistedIds: number
}): Promise<void> {
  let cursor: string | undefined
  const collectedIds = new Set<number>()
  const applyCollectedIds = (): void => {
    if (collectedIds.size === 0) {
      return
    }

    setReadIds((previous) => {
      const next = new Set(previous)
      for (const id of collectedIds) {
        next.add(id)
      }
      return trimIdSet(next, maxPersistedIds)
    })
  }

  try {
    for (;;) {
      const data = await fetchIds({
        ...params,
        limit: pageSize,
        cursor,
      })
      for (const id of data.ids) {
        collectedIds.add(id)
      }
      if (!data.next_cursor || data.ids.length === 0) {
        break
      }
      cursor = data.next_cursor
    }
  } catch (error) {
    applyCollectedIds()
    throw error
  }

  applyCollectedIds()
}
