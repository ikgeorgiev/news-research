self.addEventListener("install", (event) => {
  event.waitUntil(self.skipWaiting())
})

self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim())
})

self.addEventListener("push", (event) => {
  event.waitUntil(
    (async () => {
      let payload = {}
      try {
        payload = event.data ? event.data.json() : {}
      } catch {
        payload = {}
      }

      const title = payload.title || "CEF News"
      const body = payload.body || "New article available"
      const url = payload.url || "/"
      const tag = payload.tag || "cef-news"
      const data = {
        ...(payload || {}),
        url,
      }

      // Always show OS notification (tag dedupes across tabs)
      await self.registration.showNotification(title, {
        body,
        icon: "/icon.svg",
        badge: "/icon.svg",
        tag,
        renotify: true,
        requireInteraction: true,
        data,
      })

      // Notify one visible tab so it can update its feed (avoid N duplicate fetches)
      const clients = await self.clients.matchAll({
        type: "window",
        includeUncontrolled: true,
      })
      const visibleClient = clients.find((c) => c.visibilityState === "visible")
      if (visibleClient) {
        visibleClient.postMessage({
          type: "push-notification",
          payload: data,
        })
      }
    })()
  )
})

self.addEventListener("notificationclick", (event) => {
  event.notification.close()
  event.waitUntil(
    (async () => {
      const targetUrl = event.notification?.data?.url || "/"
      const clients = await self.clients.matchAll({
        type: "window",
        includeUncontrolled: true,
      })

      // Prefer an already-visible tab; fall back to the first available
      const visibleClient = clients.find((c) => c.visibilityState === "visible")
      const target = visibleClient || clients.find((c) => "focus" in c)
      if (target) {
        target.postMessage({
          type: "push-notification-click",
          payload: event.notification?.data || {},
        })
        await target.focus()
        return
      }

      if (self.clients.openWindow) {
        await self.clients.openWindow(targetUrl)
      }
    })()
  )
})
