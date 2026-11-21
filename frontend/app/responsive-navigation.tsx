"use client"

import { useRef, useState } from "react"
import { WatchlistSidebar } from "./watchlist-sidebar"
import type { WatchlistSidebarModel } from "./use-page-controller"

/** Inline mobile disclosure; desktop keeps the existing full-height sidebar. */
export function ResponsiveNavigation({ sidebar }: { sidebar: WatchlistSidebarModel }) {
  const [open, setOpen] = useState(false)
  const toggle = useRef<HTMLButtonElement>(null)
  function close() { setOpen(false); toggle.current?.focus() }
  return <>
    <header className="mobile-navigation-bar">
      <span>CEF News</span>
      <button ref={toggle} aria-expanded={open} aria-controls="watchlist-navigation"
        onClick={() => setOpen(value => !value)}>{open ? "Close watchlists" : "Watchlists"}
        <span aria-hidden="true">{open ? " −" : " +"}</span>
      </button>
    </header>
    <div id="watchlist-navigation" className="watchlist-navigation" data-open={open}
      onKeyDown={event => { if (open && event.key === "Escape" && !event.defaultPrevented) close() }}>
      <WatchlistSidebar sidebar={{ ...sidebar, selectWatchlist: (...args) => {
        sidebar.selectWatchlist(...args)
        if (open) close()
      } }} />
    </div>
  </>
}
