"use client"

import { useEffect, useSyncExternalStore, type RefObject } from "react"
import { deliveryCheck, EMPTY_CHECK } from "@/lib/delivery-check"

/** Observe only rows in the ordinary app; never create another network observer. */
export function useDeliveryViewport(container: RefObject<HTMLElement | null>, items: readonly { id: number }[]) {
  const active = useSyncExternalStore(deliveryCheck.subscribe, () => deliveryCheck.getSnapshot().active, () => EMPTY_CHECK.active)
  useEffect(() => {
    const root = container.current
    if (!active || !root || typeof IntersectionObserver === "undefined") return
    const frames = new Set<number>()
    const observed = new Set<Element>()
    function check(node: Element) {
      const first = requestAnimationFrame(() => {
        frames.delete(first)
        const second = requestAnimationFrame(() => {
          frames.delete(second)
          if (!node.isConnected || document.visibilityState !== "visible" || document.querySelector("dialog[open]")) return
          const bounds = node.getBoundingClientRect(), parent = root!.getBoundingClientRect()
          const style = getComputedStyle(node)
          const left = Math.max(0, parent.left), right = Math.min(innerWidth, parent.right)
          const top = Math.max(0, parent.top), bottom = Math.min(innerHeight, parent.bottom)
          if (style.visibility !== "visible" || style.display === "none" || Number(style.opacity) === 0
            || bounds.width <= 0 || bounds.height <= 0 || bounds.bottom <= top || bounds.top >= bottom
            || bounds.right <= left || bounds.left >= right) return
          deliveryCheck.viewport(Number(node.getAttribute("data-news-id")))
        })
        frames.add(second)
      })
      frames.add(first)
    }
    const observer = new IntersectionObserver(entries => {
      for (const entry of entries) {
        if (entry.isIntersecting) { observed.add(entry.target); check(entry.target) }
        else observed.delete(entry.target)
      }
    }, { root, threshold: 0.01 })
    root.querySelectorAll("[data-news-id]").forEach(node => observer.observe(node))
    function visibility() {
      deliveryCheck.signal(document.visibilityState === "visible" ? "visible" : "hidden")
      if (document.visibilityState === "visible") observed.forEach(check)
    }
    function afterDialog() { if (!document.querySelector("dialog[open]")) observed.forEach(check) }
    document.addEventListener("visibilitychange", visibility)
    // Closing the reader may uncover rows without changing their intersection.
    const dialogs = new MutationObserver(afterDialog)
    dialogs.observe(document.body, { subtree: true, childList: true, attributes: true, attributeFilter: ["open"] })
    return () => {
      observer.disconnect(); dialogs.disconnect()
      document.removeEventListener("visibilitychange", visibility)
      frames.forEach(cancelAnimationFrame)
    }
  }, [active, container, items])
}
