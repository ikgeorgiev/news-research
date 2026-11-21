// @vitest-environment node
import { mkdtemp, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { afterEach, beforeEach, expect, it, vi } from "vitest"
import { GET } from "./[id]/route"

let directory: string
const body = { version: 1, article_id: 1634, wire_key: "bw:20261030641475:en", title: "BlackRock", source_url: "https://example.com/a",
  original_url: "https://www.businesswire.com/news/home/20261030641475/en/", published_at: "2026-10-30T20:30:00Z", blocks: [{ type: "paragraph", text: "Saved text." }] }

beforeEach(async () => {
  directory = await mkdtemp(path.join(tmpdir(), "cef-reader-"))
  vi.stubEnv("ARTICLE_BODY_ARCHIVE_DIR", directory)
  vi.stubGlobal("fetch", vi.fn(() => { throw new Error("Reader must not fetch publisher pages") }))
})
afterEach(async () => {
  vi.unstubAllEnvs(); vi.unstubAllGlobals()
  if (!path.resolve(directory).startsWith(path.resolve(tmpdir()) + path.sep + "cef-reader-")) throw new Error("Unexpected test cleanup target")
  await rm(directory, { recursive: true })
})
const request = (id: string) => GET(new Request(`http://localhost/article-body/${id}`), { params: Promise.resolve({ id }) })

it("serves only a validated local article and never requests the publisher", async () => {
  await writeFile(path.join(directory, "1634.json"), JSON.stringify(body))
  const response = await request("1634")
  expect(response.status).toBe(200)
  expect(await response.json()).toEqual(body)
  expect(fetch).not.toHaveBeenCalled()
})
it.each(["..", "../1634", "1634.json", "0", "-1"])("rejects path or identifier %s", async id => {
  expect((await request(id)).status).toBe(400)
  expect(fetch).not.toHaveBeenCalled()
})
it("returns missing locally without fetching a replacement body", async () => {
  expect((await request("1634")).status).toBe(404)
  expect(fetch).not.toHaveBeenCalled()
})
it.each(["wrong-id", "oversized", "broken-json"])("rejects %s archive", async kind => {
  const value = kind === "wrong-id" ? JSON.stringify({ ...body, article_id: 1623 }) : kind === "oversized" ? "x".repeat(1024 * 1024 + 1) : "{"
  await writeFile(path.join(directory, "1634.json"), value)
  expect((await request("1634")).status).toBe(422)
})
