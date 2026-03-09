"use client"

import {
  PushAlertScopes,
  PushSubscriptionPayload,
  PushUpsertRequest,
  PushUpsertResponse,
  PushVapidKeyResponse,
} from "./types"
import { buildApiUrl } from "./api-base"
const PUSH_MANAGE_TOKEN_KEY = "pushManageToken"

export type PushStatus = {
  supported: boolean
  subscribed: boolean
}

export type EnablePushResult = {
  permissionGranted: boolean
  pushActive: boolean
  reason?: string
}

function isWindowAvailable(): boolean {
  return typeof window !== "undefined"
}

export function isPushSupported(): boolean {
  return (
    isWindowAvailable() &&
    "Notification" in window &&
    "serviceWorker" in navigator &&
    "PushManager" in window
  )
}

function getStoredManageToken(): string | null {
  if (!isWindowAvailable()) return null
  const value = localStorage.getItem(PUSH_MANAGE_TOKEN_KEY)
  return value && value.length > 0 ? value : null
}

function setStoredManageToken(token: string | null): void {
  if (!isWindowAvailable()) return
  if (token && token.length > 0) {
    localStorage.setItem(PUSH_MANAGE_TOKEN_KEY, token)
    return
  }
  localStorage.removeItem(PUSH_MANAGE_TOKEN_KEY)
}

function urlBase64ToUint8Array(value: string): Uint8Array {
  const padded = value + "=".repeat((4 - (value.length % 4 || 4)) % 4)
  const base64 = padded.replace(/-/g, "+").replace(/_/g, "/")
  const raw = window.atob(base64)
  const output = new Uint8Array(raw.length)
  for (let index = 0; index < raw.length; index += 1) {
    output[index] = raw.charCodeAt(index)
  }
  return output
}

async function getKnownServiceWorkerRegistration(): Promise<ServiceWorkerRegistration | null> {
  if (!("serviceWorker" in navigator)) return null
  const scoped = await navigator.serviceWorker.getRegistration("/")
  if (scoped) return scoped
  const registrations = await navigator.serviceWorker.getRegistrations()
  return registrations.find((item) => item.active?.scriptURL.endsWith("/sw.js")) ?? null
}

async function ensureServiceWorkerRegistration(): Promise<ServiceWorkerRegistration> {
  const existing = await getKnownServiceWorkerRegistration()
  if (existing) return existing
  const registered = await navigator.serviceWorker.register("/sw.js", { scope: "/" })
  await navigator.serviceWorker.ready
  return registered
}

async function fetchVapidKey(): Promise<PushVapidKeyResponse> {
  const response = await fetch(buildApiUrl("/api/v1/push/vapid-key"), {
    method: "GET",
    cache: "no-store",
  })
  if (!response.ok) {
    throw new Error(`VAPID key request failed: ${response.status}`)
  }
  return response.json()
}

function toBackendSubscription(subscription: PushSubscription): PushSubscriptionPayload {
  const serialized = subscription.toJSON()
  const keys = serialized.keys ?? {}
  return {
    endpoint: serialized.endpoint ?? subscription.endpoint,
    expiration_time:
      typeof serialized.expirationTime === "number"
        ? serialized.expirationTime
        : null,
    keys: {
      p256dh: keys.p256dh ?? "",
      auth: keys.auth ?? "",
    },
  }
}

async function upsertBackendSubscription({
  subscription,
  scopes,
}: {
  subscription: PushSubscription
  scopes: PushAlertScopes
}): Promise<PushUpsertResponse> {
  const body: PushUpsertRequest = {
    subscription: toBackendSubscription(subscription),
    scopes,
    manage_token: getStoredManageToken(),
  }
  const response = await fetch(buildApiUrl("/api/v1/push/subscription"), {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  })
  if (!response.ok) {
    throw new Error(`Push subscription upsert failed: ${response.status}`)
  }
  const payload: PushUpsertResponse = await response.json()
  setStoredManageToken(payload.manage_token ?? body.manage_token ?? null)
  return payload
}

async function deleteBackendSubscription(endpoint: string): Promise<void> {
  const manageToken = getStoredManageToken()
  if (!manageToken) {
    return
  }
  const response = await fetch(buildApiUrl("/api/v1/push/subscription"), {
    method: "DELETE",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      endpoint,
      manage_token: manageToken,
    }),
  })
  if (!response.ok && response.status !== 404) {
    throw new Error(`Push subscription delete failed: ${response.status}`)
  }
}

export async function getPushStatus(): Promise<PushStatus> {
  if (!isPushSupported()) {
    return { supported: false, subscribed: false }
  }
  const registration = await getKnownServiceWorkerRegistration()
  if (!registration) {
    return { supported: true, subscribed: false }
  }
  const subscription = await registration.pushManager.getSubscription()
  return {
    supported: true,
    subscribed: subscription !== null,
  }
}

export async function enablePushNotifications(scopes: PushAlertScopes): Promise<EnablePushResult> {
  if (typeof Notification === "undefined") {
    return { permissionGranted: false, pushActive: false, reason: "notification-unavailable" }
  }

  let permission = Notification.permission
  if (permission === "default") {
    permission = await Notification.requestPermission()
  }
  if (permission !== "granted") {
    return { permissionGranted: false, pushActive: false, reason: "permission-denied" }
  }

  if (!isPushSupported()) {
    return { permissionGranted: true, pushActive: false, reason: "push-unsupported" }
  }

  const vapid = await fetchVapidKey()
  if (!vapid.enabled || !vapid.public_key) {
    return { permissionGranted: true, pushActive: false, reason: "push-disabled" }
  }

  const registration = await ensureServiceWorkerRegistration()
  let subscription = await registration.pushManager.getSubscription()
  if (!subscription) {
    subscription = await registration.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: urlBase64ToUint8Array(vapid.public_key),
    })
  }

  await upsertBackendSubscription({ subscription, scopes })
  return { permissionGranted: true, pushActive: true }
}

export async function disablePushNotifications(): Promise<void> {
  if (!isPushSupported()) {
    setStoredManageToken(null)
    return
  }
  const registration = await getKnownServiceWorkerRegistration()
  if (!registration) {
    setStoredManageToken(null)
    return
  }
  const subscription = await registration.pushManager.getSubscription()
  if (!subscription) {
    setStoredManageToken(null)
    return
  }
  try {
    await deleteBackendSubscription(subscription.endpoint)
  } finally {
    try {
      await subscription.unsubscribe()
    } finally {
      setStoredManageToken(null)
    }
  }
}

export async function syncPushScopes(scopes: PushAlertScopes): Promise<boolean> {
  if (!isPushSupported()) {
    return false
  }
  const registration = await getKnownServiceWorkerRegistration()
  if (!registration) {
    return false
  }
  const subscription = await registration.pushManager.getSubscription()
  if (!subscription) {
    return false
  }
  await upsertBackendSubscription({ subscription, scopes })
  return true
}
