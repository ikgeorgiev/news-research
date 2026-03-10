"use client"

export function persistJson(key: string, value: unknown): void {
  try {
    localStorage.setItem(key, JSON.stringify(value))
  } catch (err) {
    console.warn(`Failed to persist ${key} to localStorage`, err)
  }
}

export function persistValue(key: string, value: string): void {
  try {
    localStorage.setItem(key, value)
  } catch (err) {
    console.warn(`Failed to persist ${key} to localStorage`, err)
  }
}
