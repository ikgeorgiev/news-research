import type { BodyBlock, BodyCell } from "./article-body"

export type TableBlock = Extract<BodyBlock, { type: "table" }>

/** Remove only proven layout spacers; retain every nonempty cell and its order.
 * Tables with row spans stay intact because removing rows could shift values.
 */
export function readableTable(table: TableBlock): TableBlock {
  if (table.rows.some(row => row.some(cell => cell.rowspan !== 1))) return table
  const rows = table.rows.filter(row => row.some(cell => cell.text.trim()))
  if (!rows.length) return table
  const positions = rows.map(row => {
    let column = 0
    return row.map(cell => { const start = column; column += cell.colspan; return { cell, start, end: column } })
  })
  // Empty singleton columns are layout gutters, even when a spanning heading
  // runs across them. Never remove all columns supporting a nonempty heading.
  const gutters = new Set<number>()
  const occupied = new Set<number>()
  const cells = positions.flat()
  for (const { cell, start, end } of cells) {
    if (end === start + 1) (cell.text.trim() ? occupied : gutters).add(start)
  }
  for (const col of occupied) gutters.delete(col)
  for (const { cell, start, end } of cells) {
    if (cell.text.trim() && Array.from({ length: end - start }, (_, i) => start + i).every(c => gutters.has(c))) gutters.delete(start)
  }
  const cleaned: BodyCell[][] = positions.map(row => row.flatMap(({ cell, start, end }) => {
    const colspan = Array.from({ length: end - start }, (_, i) => start + i).filter(c => !gutters.has(c)).length
    return colspan ? [{ ...cell, colspan }] : []
  }))
  return { ...table, rows: cleaned }
}

/** Reflow a flattened saved body without inventing headings or table values. */
export function readableParagraphs(text: string): string[] {
  return text.trim().split(/\n\s*\n|\r?\n/).filter(Boolean).flatMap(paragraph => {
    if (paragraph.length <= 900) return [paragraph]
    const sentences = paragraph.split(/(?<=[.!?])\s+(?=[A-Z“])/u)
    const groups: string[] = []
    let current = ""
    for (const sentence of sentences) {
      if (current.length >= 450) { groups.push(current); current = "" }
      current += (current ? " " : "") + sentence
    }
    if (current) groups.push(current)
    return groups
  })
}
