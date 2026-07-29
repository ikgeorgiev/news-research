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
  if (maxPersistedKeys <= 0) {
    return
  }

  let cursor: string | undefined
  const collectedKeys: string[] = []
  const collectedKeySet = new Set<string>()
  const applyCollectedKeys = (): void => {
    if (collectedKeys.length === 0) {
      return
    }

    setReadKeys((previous) => {
      const next = new Set(previous)
      // The API returns newest articles first. Insert the collected keys in
      // reverse order so trimSet keeps the newest keys at the end of the Set.
      for (let index = collectedKeys.length - 1; index >= 0; index -= 1) {
        const key = collectedKeys[index]
        // Re-adding an existing key does not change Set insertion order.
        // Delete it first so a newly marked key receives the correct priority.
        next.delete(key)
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
        if (collectedKeySet.has(key)) continue
        collectedKeySet.add(key)
        collectedKeys.push(key)
        if (collectedKeys.length >= maxPersistedKeys) {
          break
        }
      }
      if (collectedKeys.length >= maxPersistedKeys) {
        break
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
