import { cleanup, fireEvent, render, screen } from "@testing-library/react"
import { afterEach, describe, expect, it, vi } from "vitest"
import type { WatchlistSidebarModel } from "./use-page-controller"
import { ResponsiveNavigation } from "./responsive-navigation"

vi.mock("./watchlist-sidebar", () => ({ WatchlistSidebar: ({ sidebar }: { sidebar: WatchlistSidebarModel }) =>
  <button onClick={() => sidebar.selectWatchlist("all")}>All News</button> }))

afterEach(cleanup)

describe("mobile watchlist navigation", () => {
  it("closes after selecting a watchlist and restores keyboard focus", () => {
    const selectWatchlist = vi.fn()
    render(<ResponsiveNavigation sidebar={{ selectWatchlist } as unknown as WatchlistSidebarModel} />)
    const toggle = screen.getByRole("button", { name: /Watchlists/ })
    expect(toggle).toHaveAttribute("aria-expanded", "false")
    fireEvent.click(toggle)
    expect(toggle).toHaveAttribute("aria-expanded", "true")
    fireEvent.click(screen.getByRole("button", { name: "All News" }))
    expect(selectWatchlist).toHaveBeenCalledWith("all")
    expect(toggle).toHaveAttribute("aria-expanded", "false")
    expect(toggle).toHaveFocus()
  })
  it("allows Escape to close the disclosure", () => {
    render(<ResponsiveNavigation sidebar={{ selectWatchlist: vi.fn() } as unknown as WatchlistSidebarModel} />)
    const toggle = screen.getByRole("button", { name: /Watchlists/ })
    fireEvent.click(toggle)
    fireEvent.keyDown(screen.getByRole("button", { name: "All News" }), { key: "Escape" })
    expect(toggle).toHaveAttribute("aria-expanded", "false")
    expect(toggle).toHaveFocus()
  })
})
