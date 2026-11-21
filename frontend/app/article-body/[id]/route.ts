import { open } from "node:fs/promises"
import path from "node:path"
import { parseArticleBody } from "@/lib/article-body"

export const runtime = "nodejs"
// Keep local archives outside the /api/* rewrite to the ingestion backend.
const MAX_BYTES = 1024 * 1024

export async function GET(_request: Request, context: { params: Promise<{ id: string }> }) {
  const { id } = await context.params
  if (!/^[1-9]\d{0,9}$/.test(id)) return new Response(null, { status: 400 })
  const directory = process.env.ARTICLE_BODY_ARCHIVE_DIR ?? path.resolve(process.cwd(), "../data/article-bodies")
  let file: Awaited<ReturnType<typeof open>> | undefined
  try {
    file = await open(path.join(directory, `${id}.json`), "r")
    if ((await file.stat()).size > MAX_BYTES) return new Response(null, { status: 422 })
    const content = await file.readFile("utf8")
    if (Buffer.byteLength(content) > MAX_BYTES) return new Response(null, { status: 422 })
    const document = parseArticleBody(JSON.parse(content), Number(id))
    if (!document) return new Response(null, { status: 422 })
    return Response.json(document, { headers: { "Cache-Control": "private, max-age=60" } })
  } catch (error) {
    const status = (error as NodeJS.ErrnoException).code === "ENOENT" ? 404 : error instanceof SyntaxError ? 422 : 503
    return new Response(null, { status, headers: { "Cache-Control": "no-store" } })
  } finally {
    await file?.close()
  }
}
