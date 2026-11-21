"use client"

import { useSyncExternalStore } from "react"
import { deliveryCheck, EMPTY_CHECK } from "@/lib/delivery-check"
import type { NewsItem } from "@/lib/types"

export function DeliveryCheckPanel({ items }: { items: NewsItem[] }) {
  const state = useSyncExternalStore(deliveryCheck.subscribe, deliveryCheck.getSnapshot, () => EMPTY_CHECK)
  function download() {
    const blob = new Blob([JSON.stringify(deliveryCheck.export(), null, 2)], { type: "application/json" })
    const url = URL.createObjectURL(blob)
    const link = document.createElement("a")
    link.href = url; link.download = "cef-delivery-check-" + new Date().toISOString().replace(/[:.]/g, "-") + ".json"
    link.click()
    setTimeout(() => URL.revokeObjectURL(url), 1000)
  }
  return <details className="delivery-check-panel">
    <summary><span>Delivery check</span><span className={state.active ? "delivery-check-active" : "delivery-check-state"}>
      {state.active ? "Recording in this tab" : state.stoppedReason ? "Session stopped" : "Off"}</span></summary>
    <div className="delivery-check-content">
      <p>Record when this tab receives new articles and when their rows become visible. Runs for up to 20 minutes and stays on this device. Opening a new page ends the session.</p>
      <div className="delivery-check-actions">
        <button disabled={state.active} onClick={() => deliveryCheck.start(items)}>Start 20-minute check</button>
        <button disabled={!state.active} onClick={() => deliveryCheck.stop()}>Stop</button>
        <button disabled={state.articles === 0} onClick={download}>Download results</button>
        <button disabled={state.active || state.articles === 0} onClick={() => deliveryCheck.clear()}>Clear</button>
      </div>
      <p role="status">{state.automaticNew} new automatic arrivals · {state.viewportObserved} observed in view
        {state.dropped > 0 && ` · ${state.dropped} records omitted at the size limit`}</p>
      <small>Existing articles and manually loaded history are excluded. Publication time and server “seen” time are separate labels; this check does not establish when a publisher first made a story public.</small>
    </div>
  </details>
}
