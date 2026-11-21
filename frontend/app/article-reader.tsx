"use client"

import { CSSProperties, MouseEvent, useEffect, useRef, useState } from "react"
import { createPortal } from "react-dom"
import { ArticleBody, parseArticleBody } from "@/lib/article-body"
import { articleLink } from "@/lib/article-link"
import { readableParagraphs, readableTable } from "@/lib/article-layout"
import { NewsItem } from "@/lib/types"

function publicationTime(iso: string) {
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) return "Publication time unavailable"
  return new Intl.DateTimeFormat(undefined, { year: "numeric", month: "long", day: "numeric",
    hour: "numeric", minute: "2-digit", timeZoneName: "short" }).format(date)
}

export function ArticleBodyContent({ document }: { document: ArticleBody }) {
  let tableNumber = 0
  return <>{document.blocks.map((block, index) => {
    if (block.type === "paragraph") return <p key={index}>{block.text}</p>
    if (block.type === "heading") {
      const Heading = ("h" + block.level) as "h2" | "h3" | "h4"
      return <Heading key={index}>{block.text}</Heading>
    }
    if (block.type === "list") {
      const Tag = block.ordered ? "ol" : "ul"
      return <Tag key={index}>{block.items.map((item, n) => <li key={n}>{item}</li>)}</Tag>
    }
    const table = readableTable(block)
    tableNumber++
    return <figure className="reader-table" data-table-number={tableNumber} key={index}>
      <figcaption><span>{table.caption || "Table " + tableNumber}</span><span>Scroll table to see all columns</span></figcaption>
      <div className="reader-table-scroll" tabIndex={0} role="region" aria-label={table.caption || "Article table " + tableNumber}>
        <table>
          {table.caption && <caption className="sr-only">{table.caption}</caption>}
          <tbody>{table.rows.map((row, n) => <tr key={n}>{row.map((cell, c) => {
            const Tag = cell.header ? "th" : "td"
            const number = /^[($+\-−]?\s*\d[\d,.\s%)*]*$/.test(cell.text) || /^[($%)]$/.test(cell.text)
            return <Tag key={c} colSpan={cell.colspan} rowSpan={cell.rowspan}
              className={number ? "reader-number" : undefined}>{cell.text}</Tag>
          })}</tr>)}</tbody>
        </table>
      </div>
    </figure>
  })}</>
}

type Props = { item: NewsItem; onClose: () => void; openArticle: (item: NewsItem, event: MouseEvent) => void }

export function ArticleReader({ item, onClose, openArticle }: Props) {
  const dialog = useRef<HTMLDialogElement>(null)
  const closeButton = useRef<HTMLButtonElement>(null)
  const scroll = useRef<HTMLDivElement>(null)
  const [document, setDocument] = useState<ArticleBody | null>(null)
  const [loading, setLoading] = useState(true)
  const [fontSize, setFontSize] = useState(18)
  const [theme, setTheme] = useState("paper")
  const [progress, setProgress] = useState(0)
  const link = articleLink(item, document)
  const publisher = link?.original ? link.publisher : item.provider || item.source
  const plainText = item.summary?.trim() ?? ""
  const shortExcerpt = !document && plainText.length <= 900
  const words = document ? document.blocks.map(block => block.type === "paragraph" || block.type === "heading" ? block.text : "").join(" ") : plainText
  const minutes = Math.max(1, Math.ceil(words.split(/\s+/).length / 220))
  const tables = document?.blocks.filter(block => block.type === "table") ?? []

  useEffect(() => {
    const element = dialog.current!
    const previousFocus = window.document.activeElement
    const previousOverflow = window.document.body.style.overflow
    window.document.body.style.overflow = "hidden"
    if (typeof element.showModal === "function") element.showModal()
    else element.setAttribute("open", "")
    closeButton.current?.focus()
    try {
      if (localStorage.getItem("cef-reader-theme") === "dark") setTheme("dark")
      const size = Number(localStorage.getItem("cef-reader-size"))
      if ([16, 18, 20, 22].includes(size)) setFontSize(size)
    } catch { /* Reading still works when local storage is unavailable. */ }
    return () => {
      if (typeof element.close === "function") element.close()
      window.document.body.style.overflow = previousOverflow
      if (previousFocus instanceof HTMLElement && previousFocus.isConnected) previousFocus.focus()
    }
  }, [])

  useEffect(() => {
    const abort = new AbortController()
    setDocument(null); setLoading(true); setProgress(0)
    scroll.current?.scrollTo?.(0, 0)
    const timeout = setTimeout(() => { abort.abort(); setDocument(null); setLoading(false) }, 5000)
    fetch("/article-body/" + item.id, { signal: abort.signal })
      .then(async response => response.ok ? parseArticleBody(await response.json(), item.id, item.title) : null)
      .then(body => { if (!abort.signal.aborted) setDocument(body) })
      .catch(() => { if (!abort.signal.aborted) setDocument(null) })
      .finally(() => { clearTimeout(timeout); if (!abort.signal.aborted) setLoading(false) })
    return () => { clearTimeout(timeout); abort.abort() }
  }, [item.id, item.title])

  function resize(size: number) {
    setFontSize(size)
    try { localStorage.setItem("cef-reader-size", String(size)) } catch { /* Optional preference. */ }
  }
  function toggleTheme() {
    const next = theme === "paper" ? "dark" : "paper"
    setTheme(next)
    try { localStorage.setItem("cef-reader-theme", next) } catch { /* Optional preference. */ }
  }
  function jumpToTable(number: string) {
    const table = scroll.current?.querySelector<HTMLElement>(`.reader-table[data-table-number="${number}"]`)
    table?.scrollIntoView?.({ block: "start" })
    table?.querySelector<HTMLElement>(".reader-table-scroll")?.focus({ preventScroll: true })
  }

  return createPortal(
    <dialog ref={dialog} className={"article-reader reader-theme-" + theme} aria-labelledby={"reader-title-" + item.id}
      onCancel={event => { event.preventDefault(); onClose() }}
      onClick={event => { if (event.target === event.currentTarget) onClose() }}>
      <div className="reader-shell" style={{ "--reader-font-size": fontSize + "px" } as CSSProperties}>
        <div className="reader-toolbar">
          <div className="reader-brand"><span className="reader-brand-mark" aria-hidden="true">W</span><span>WIRE DECK<span className="reader-brand-caption">ARTICLE READER</span></span></div>
          <div className="reader-tools">
            <div className="reader-type-controls" role="group" aria-label="Article text size">
              <button onClick={() => resize(fontSize - 2)} disabled={fontSize <= 16} aria-label="Decrease text size">A−</button>
              <button onClick={() => resize(fontSize + 2)} disabled={fontSize >= 22} aria-label="Increase text size">A+</button>
            </div>
            <button className="reader-theme-button" onClick={toggleTheme} aria-label={theme === "paper" ? "Use dark reading theme" : "Use light reading theme"}>{theme === "paper" ? "Dark" : "Light"}</button>
            <button ref={closeButton} className="reader-close" onClick={onClose} aria-label="Close article">×</button>
          </div>
        </div>
        <div className="reader-progress" aria-hidden="true"><span style={{ width: progress + "%" }} /></div>
        <div ref={scroll} className="reader-scroll" onScroll={event => {
          const el = event.currentTarget
          setProgress(el.scrollHeight > el.clientHeight ? el.scrollTop / (el.scrollHeight - el.clientHeight) * 100 : 100)
        }}>
          <article className="reader-document">
            <header className="reader-header">
              <div className="reader-eyebrow"><span>{publisher}</span><span>PRESS & NEWS</span></div>
              <h1 id={"reader-title-" + item.id}>{item.title}</h1>
              <div className="reader-meta">
                <time dateTime={item.published_at}>{publicationTime(item.published_at)}</time>
                {!loading && <span>{shortExcerpt ? "News excerpt" : minutes + " min read"}</span>}
                {tables.length > 0 && <select className="reader-table-jump" aria-label="Jump to article table" value=""
                  onChange={event => jumpToTable(event.target.value)}>
                  <option value="" disabled>{tables.length === 1 ? "View table" : `Jump to ${tables.length} tables`}</option>
                  {tables.map((table, n) => <option key={n} value={n + 1}>
                    {`Table ${n + 1}${table.caption ? ": " + table.caption : ""}`}
                  </option>)}
                </select>}
                {link?.original && item.provider !== publisher && <span>Received via {item.provider}</span>}
              </div>
              <div className="reader-source-row">
                <div className="reader-tickers">{item.tickers.map(ticker => <span key={ticker}>{ticker}</span>)}</div>
                {link && <a className="reader-publisher-link" href={link.url} target="_blank" rel="noopener noreferrer"
                  onClick={event => openArticle({ ...item, url: link.url }, event)}>Open {link.publisher} <span aria-hidden="true">↗</span></a>}
              </div>
            </header>
            <div className="reader-content">
              {loading ? <div className="reader-loading" role="status"><p>Opening article…</p><span /><span /><span /></div>
                : document ? <ArticleBodyContent document={document} /> : <>
                  <aside className="reader-fallback-note">{shortExcerpt
                    ? "Only a news excerpt is available here. Use the source link for the full article."
                    : "Reading the saved text. Some tables and formatting may be unavailable; the source link has the publisher’s layout."}</aside>
                  {plainText ? readableParagraphs(plainText).map((paragraph, n) => <p key={n}>{paragraph}</p>) : <p>No article text is available yet.</p>}
                </>}
            </div>
            {!loading && <footer className="reader-footer"><span>{document ? "End of article" : "End of saved text"}</span>
              {link && <a href={link.url} target="_blank" rel="noopener noreferrer" onClick={event => openArticle({ ...item, url: link.url }, event)}>Continue at {link.publisher} ↗</a>}
            </footer>}
          </article>
        </div>
      </div>
    </dialog>, window.document.body,
  )
}
