import { describe, expect, it, vi } from "vitest"

import { markReadIdsByQuery } from "./read-state"

describe("markReadIdsByQuery", () => {
  it("batches paginated ids into a single read-state update", async () => {
    const fetchIds = vi
      .fn()
      .mockResolvedValueOnce({
        ids: [1, 2],
        next_cursor: "cursor-2",
      })
      .mockResolvedValueOnce({
        ids: [3],
        next_cursor: null,
      })
    const setReadIds = vi.fn()

    await markReadIdsByQuery({
      params: {
        tickers: ["GOF"],
      },
      fetchIds,
      setReadIds,
      pageSize: 1000,
      maxPersistedIds: 20_000,
    })

    expect(fetchIds).toHaveBeenCalledTimes(2)
    expect(setReadIds).toHaveBeenCalledTimes(1)

    const updater = setReadIds.mock.calls[0][0] as (previous: Set<number>) => Set<number>
    expect(Array.from(updater(new Set([9])))).toEqual([9, 1, 2, 3])
  })

  it("keeps already fetched ids if a later page fails", async () => {
    const fetchIds = vi
      .fn()
      .mockResolvedValueOnce({
        ids: [1, 2],
        next_cursor: "cursor-2",
      })
      .mockRejectedValueOnce(new Error("page 2 failed"))
    const setReadIds = vi.fn()

    await expect(
      markReadIdsByQuery({
        params: {
          includeUnmappedFromProvider: "Business Wire",
        },
        fetchIds,
        setReadIds,
        pageSize: 1000,
        maxPersistedIds: 20_000,
      })
    ).rejects.toThrow("page 2 failed")

    expect(fetchIds).toHaveBeenCalledTimes(2)
    expect(setReadIds).toHaveBeenCalledTimes(1)

    const updater = setReadIds.mock.calls[0][0] as (previous: Set<number>) => Set<number>
    expect(Array.from(updater(new Set([9])))).toEqual([9, 1, 2])
  })
})
