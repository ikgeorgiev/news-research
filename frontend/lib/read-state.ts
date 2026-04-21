import type { NewsFilterParams, NewsIdsRequestParams } from "./api"
import type { NewsIdsResponse } from "./types"

import { buildNewsRequestParams } from "./api"

export type ReadQueryParams = NewsFilterParams

type FetchNewsIdsPage = (params: NewsIdsRequestParams) => Promise<NewsIdsResponse>

type SetReadKeys = (updater: (previous: Set<string>) => Set<string>) => void


export function trimSet<T>(input: Set<T>, maxSize: number): Set<T> {
  if (input.size <= maxSize) return input
  const trimmed = Array.from(input).slice(input.size - maxSize)
  return new Set(trimmed)
}


export async function markReadKeysByQuery({
  params,
  fetchIds,
  setReadKeys,
  pageSize,
  maxPersistedKeys,
}: {
  params: ReadQueryParams
  fetchIds: FetchNewsIdsPage
  setReadKeys: SetReadKeys
  pageSize: number
  maxPersistedKeys: number
}): Promise<void> {
  let cursor: string | undefined
  const collectedKeys = new Set<string>()
  const applyCollectedKeys = (): void => {
    if (collectedKeys.size === 0) {
      return
    }

    setReadKeys((previous) => {
      const next = new Set(previous)
      for (const key of collectedKeys) {
        next.add(key)
      }
      return trimSet(next, maxPersistedKeys)
    })
  }

  try {
    for (;;) {
      const data = await fetchIds(buildNewsRequestParams(params, {
        limit: pageSize,
        cursor,
      }))
      for (const key of data.read_keys) {
        collectedKeys.add(key)
      }
      if (!data.next_cursor || data.ids.length === 0) {
        break
      }
      cursor = data.next_cursor
    }
  } catch (error) {
    applyCollectedKeys()
    throw error
  }

  applyCollectedKeys()
}
