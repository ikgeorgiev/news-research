import { cleanup, fireEvent, render, screen } from "@testing-library/react"
import { afterEach, expect, it, vi } from "vitest"
import { DeliveryCheckPanel } from "./delivery-check-panel"
import { deliveryCheck } from "@/lib/delivery-check"
import type { NewsItem } from "@/lib/types"

afterEach(() => { cleanup(); deliveryCheck.clear(); vi.useRealTimers(); vi.restoreAllMocks() })
it("records only after Start and makes a local downloadable result available", () => {
  vi.useFakeTimers()
  const fetch = vi.spyOn(globalThis, "fetch")
  const item = { id: 1, title: "Saved headline", url: "https://example.com/1", provider: "BW", first_seen_at: new Date().toISOString(), published_at: new Date().toISOString(), alert_sent_at: null } as NewsItem
  render(<DeliveryCheckPanel items={[item]} />)
  expect(screen.getByText("Off")).toBeInTheDocument()
  expect(screen.getByRole("button", { name: "Download results" })).toBeDisabled()
  fireEvent.click(screen.getByRole("button", { name: "Start 20-minute check" }))
  expect(screen.getByText("Recording in this tab")).toBeInTheDocument()
  expect(screen.getByRole("button", { name: "Download results" })).toBeEnabled()
  expect(fetch).not.toHaveBeenCalled()
  fireEvent.click(screen.getByRole("button", { name: "Stop" }))
  expect(screen.getByText("Session stopped")).toBeInTheDocument()
  fireEvent.click(screen.getByRole("button", { name: "Clear" }))
  expect(screen.getByText("Off")).toBeInTheDocument()
})
