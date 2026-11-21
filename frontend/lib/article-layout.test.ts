import { describe, expect, it } from "vitest"
import { readableParagraphs, readableTable, TableBlock } from "./article-layout"
import type { BodyCell } from "./article-body"

const cell = (text: string, colspan = 1, rowspan = 1): BodyCell => ({ text, colspan, rowspan, header: false })
const values = (table: TableBlock) => table.rows.flat().map(c => c.text).filter(t => t.trim())

describe("reader layout without evidence mutation", () => {
  it("removes financial-table gutters while preserving numbers, symbols, footnotes and spanning headers", () => {
    const table: TableBlock = { type: "table", caption: "Balance sheet", rows: [
      [cell(""), cell("As of October 31, 2026", 4)],
      [cell(""), cell(""), cell(""), cell(""), cell("")],
      [cell("Investments"), cell("$"), cell("1,578.8"), cell(""), cell("78.97")],
      [cell("Other assets*"), cell(""), cell("16.8"), cell(""), cell("0.84")],
      [cell("* Subject to change", 5)],
    ] }
    const before = JSON.stringify(table)
    const result = readableTable(table)
    expect(values(result)).toEqual(values(table))
    expect(result.rows).toHaveLength(4)
    expect(result.rows[0][1].colspan).toBe(3)
    expect(result.rows[1].map(c => c.text)).toEqual(["Investments", "$", "1,578.8", "78.97"])
    expect(JSON.stringify(table)).toBe(before)
  })
  it("preserves complex row spans unchanged", () => {
    const table: TableBlock = { type: "table", caption: "", rows: [[cell("Fund", 1, 2), cell("Value")], [cell("")]] }
    expect(readableTable(table)).toBe(table)
  })
  it("does not remove a label occupying a column that is otherwise empty", () => {
    const table: TableBlock = { type: "table", caption: "", rows: [[cell("", 2), cell("Footnote")], [cell("Name"), cell("1.20"), cell("")]] }
    expect(values(readableTable(table))).toEqual(values(table))
  })
  it("reflows a long flat body without changing its words or decimal values", () => {
    const body = "The distribution is $0.069700 per share. ".repeat(80) + "Final note: Inc. and Ltd. retain their names."
    const paragraphs = readableParagraphs(body)
    expect(paragraphs.length).toBeGreaterThan(1)
    expect(paragraphs.join(" ")).toBe(body)
  })
})
