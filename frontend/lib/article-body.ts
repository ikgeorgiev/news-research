import { originalWireLink } from "./article-link"

export type BodyCell = { text: string; header: boolean; colspan: number; rowspan: number }
export type BodyBlock =
  | { type: "paragraph"; text: string }
  | { type: "heading"; text: string; level: number }
  | { type: "list"; ordered: boolean; items: string[] }
  | { type: "table"; caption: string; rows: BodyCell[][] }
export type ArticleBody = {
  version: 1
  article_id: number
  wire_key: string
  title: string
  source_url: string
  original_url: string
  published_at: string
  blocks: BodyBlock[]
}

const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)
const text = (value: unknown): value is string => typeof value === "string" && value.length <= 60_000
const span = (value: unknown): value is number => Number.isInteger(value) && Number(value) >= 1 && Number(value) <= 40

export function parseArticleBody(value: unknown, articleId: number, title?: string): ArticleBody | null {
  if (!record(value) || value.version !== 1 || value.article_id !== articleId
    || !text(value.title) || (title !== undefined && value.title.trim() !== title.trim())
    || typeof value.wire_key !== "string" || !/^(?:(?:bw|gnw):\d+:[a-z]{2}|prn:\d+)$/.test(value.wire_key)
    || !text(value.source_url) || !text(value.original_url) || !text(value.published_at)
    || !Array.isArray(value.blocks) || value.blocks.length < 1 || value.blocks.length > 2000) return null
  if (originalWireLink(value.original_url)?.wireKey !== value.wire_key
    || !/T.+(?:Z|[+-]\d{2}:\d{2})$/.test(value.published_at)
    || !Number.isFinite(Date.parse(value.published_at))) return null
  let cells = 0
  for (const block of value.blocks) {
    if (!record(block)) return null
    switch (block.type) {
      case "paragraph": if (!text(block.text)) return null; break
      case "heading": if (!text(block.text) || ![2, 3, 4].includes(Number(block.level))) return null; break
      case "list":
        if (typeof block.ordered !== "boolean" || !Array.isArray(block.items)
          || block.items.length > 2000 || !block.items.every(text)) return null
        break
      case "table":
        if (!text(block.caption) || !Array.isArray(block.rows) || block.rows.length > 2000) return null
        for (const row of block.rows) {
          if (!Array.isArray(row) || row.length > 80) return null
          cells += row.length
          if (cells > 15_000) return null
          if (!row.every(cell => record(cell) && text(cell.text) && typeof cell.header === "boolean"
            && span(cell.colspan) && span(cell.rowspan))) return null
        }
        break
      default: return null
    }
  }
  return value as ArticleBody
}
