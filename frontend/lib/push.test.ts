import { beforeEach, describe, expect, it, vi } from "vitest"

import { disablePushNotifications, enablePushNotifications } from "./push"

type MockSubscription = {
  endpoint: string
  toJSON: () => {
    endpoint: string
    expirationTime: number | null
    keys: { p256dh: string; auth: string }
  }
  unsubscribe: ReturnType<typeof vi.fn>
}

type MockRegistration = {
  pushManager: {
    getSubscription: ReturnType<typeof vi.fn>
    subscribe: ReturnType<typeof vi.fn>
  }
}

function installNotificationPermission(permission: NotificationPermission): void {
  vi.stubGlobal("Notification", {
    permission,
    requestPermission: vi.fn(),
  })
}

function installPushSupport(): void {
  vi.stubGlobal("PushManager", function PushManager() {})
}

function installServiceWorkerRegistration(registration: MockRegistration): void {
  const serviceWorker = {
    getRegistration: vi.fn().mockResolvedValue(registration),
    getRegistrations: vi.fn().mockResolvedValue([registration]),
    register: vi.fn().mockResolvedValue(registration),
    ready: Promise.resolve(registration),
  }

  Object.defineProperty(navigator, "serviceWorker", {
    configurable: true,
    value: serviceWorker,
  })
}

function createSubscription(endpoint = "https://push.example/subscription"): MockSubscription {
  return {
    endpoint,
    toJSON: vi.fn(() => ({
      endpoint,
      expirationTime: null,
      keys: {
        p256dh: "p256dh",
        auth: "auth",
      },
    })),
    unsubscribe: vi.fn().mockResolvedValue(undefined),
  }
}

function createRegistration(subscription: MockSubscription | null): MockRegistration {
  return {
    pushManager: {
      getSubscription: vi.fn().mockResolvedValue(subscription),
      subscribe: vi.fn().mockResolvedValue(createSubscription("https://push.example/new")),
    },
  }
}

function mockFetchSequence(...responses: Array<{ ok: boolean; status: number; json?: unknown }>): void {
  const fetchMock = vi.fn()
  for (const response of responses) {
    fetchMock.mockResolvedValueOnce({
      ok: response.ok,
      status: response.status,
      json: vi.fn().mockResolvedValue(response.json ?? {}),
    })
  }
  vi.stubGlobal("fetch", fetchMock)
}

describe("push notifications", () => {
  beforeEach(() => {
    localStorage.clear()
    vi.unstubAllGlobals()
    vi.clearAllMocks()
  })

  it("cleans up a newly created subscription when backend upsert fails", async () => {
    installNotificationPermission("granted")
    installPushSupport()

    const registration = createRegistration(null)
    installServiceWorkerRegistration(registration)
    mockFetchSequence(
      { ok: true, status: 200, json: { enabled: true, public_key: "AQID" } },
      { ok: false, status: 500 }
    )

    await expect(
      enablePushNotifications({
        include_all_news: true,
        watchlists: [],
      })
    ).rejects.toThrow("Push subscription upsert failed: 500")

    expect(registration.pushManager.subscribe).toHaveBeenCalledTimes(1)
    const createdSubscription = await registration.pushManager.subscribe.mock.results[0].value
    expect(createdSubscription.unsubscribe).toHaveBeenCalledTimes(1)
  })

  it("keeps an existing subscription in place when backend upsert fails", async () => {
    installNotificationPermission("granted")
    installPushSupport()

    const existingSubscription = createSubscription("https://push.example/existing")
    const registration = createRegistration(existingSubscription)
    installServiceWorkerRegistration(registration)
    mockFetchSequence(
      { ok: true, status: 200, json: { enabled: true, public_key: "AQID" } },
      { ok: false, status: 500 }
    )

    await expect(
      enablePushNotifications({
        include_all_news: false,
        watchlists: [],
      })
    ).rejects.toThrow("Push subscription upsert failed: 500")

    expect(registration.pushManager.subscribe).not.toHaveBeenCalled()
    expect(existingSubscription.unsubscribe).not.toHaveBeenCalled()
  })

  it("still unsubscribes locally when disabling without a manage token", async () => {
    installNotificationPermission("granted")
    installPushSupport()

    const subscription = createSubscription()
    const registration = createRegistration(subscription)
    installServiceWorkerRegistration(registration)
    vi.stubGlobal("fetch", vi.fn())

    await disablePushNotifications()

    expect(subscription.unsubscribe).toHaveBeenCalledTimes(1)
    expect(fetch).not.toHaveBeenCalled()
  })
})
