import { describe, expect, it, vi } from "vitest"

import { markReadKeysByQuery } from "./read-state"

describe("markReadKeysByQuery", () => {
  it("batches paginated read keys into a single read-state update", async () => {
    const fetchIds = vi
      .fn()
      .mockResolvedValueOnce({
        ids: [1, 2],
        read_keys: ["key-1", "key-2"],
        next_cursor: "cursor-2",
      })
      .mockResolvedValueOnce({
        ids: [3],
        read_keys: ["key-3"],
        next_cursor: null,
      })
    const setReadKeys = vi.fn()

    await markReadKeysByQuery({
      params: {
        tickers: ["GOF"],
      },
      fetchIds,
      setReadKeys,
      pageSize: 1000,
      maxPersistedKeys: 20_000,
    })

    expect(fetchIds).toHaveBeenCalledTimes(2)
    expect(setReadKeys).toHaveBeenCalledTimes(1)

    const updater = setReadKeys.mock.calls[0][0] as (previous: Set<string>) => Set<string>
    expect(Array.from(updater(new Set(["old-key"])))).toEqual(["old-key", "key-1", "key-2", "key-3"])
  })

  it("keeps already fetched read keys if a later page fails", async () => {
    const fetchIds = vi
      .fn()
      .mockResolvedValueOnce({
        ids: [1, 2],
        read_keys: ["key-1", "key-2"],
        next_cursor: "cursor-2",
      })
      .mockRejectedValueOnce(new Error("page 2 failed"))
    const setReadKeys = vi.fn()

    await expect(
      markReadKeysByQuery({
        params: {
          includeUnmappedFromProvider: "Business Wire",
        },
        fetchIds,
        setReadKeys,
        pageSize: 1000,
        maxPersistedKeys: 20_000,
      })
    ).rejects.toThrow("page 2 failed")

    expect(fetchIds).toHaveBeenCalledTimes(2)
    expect(setReadKeys).toHaveBeenCalledTimes(1)

    const updater = setReadKeys.mock.calls[0][0] as (previous: Set<string>) => Set<string>
    expect(Array.from(updater(new Set(["old-key"])))).toEqual(["old-key", "key-1", "key-2"])
  })
})
