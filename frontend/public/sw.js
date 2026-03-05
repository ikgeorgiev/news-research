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

      const clients = await self.clients.matchAll({
        type: "window",
        includeUncontrolled: true,
      })
      const visibleClient = clients.find((client) => client.visibilityState === "visible")
      if (visibleClient) {
        visibleClient.postMessage({
          type: "push-notification",
          payload: data,
        })
        return
      }

      await self.registration.showNotification(title, {
        body,
        icon: "/icon.svg",
        badge: "/icon.svg",
        tag,
        renotify: true,
        requireInteraction: true,
        data,
      })
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

      for (const client of clients) {
        if ("focus" in client) {
          client.postMessage({
            type: "push-notification-click",
            payload: event.notification?.data || {},
          })
          await client.focus()
          return
        }
      }

      if (self.clients.openWindow) {
        await self.clients.openWindow(targetUrl)
      }
    })()
  )
})
