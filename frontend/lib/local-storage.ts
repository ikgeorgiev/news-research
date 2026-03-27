"use client"

type StorageValueParser<T> = (value: string) => T | null
type StorageJsonParser<T> = (value: unknown) => T | null

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

export function readValue<T>(key: string, parse: StorageValueParser<T>): T | null {
  const stored = localStorage.getItem(key)
  if (stored === null) return null
  return parse(stored)
}

export function readJson<T>(key: string, parse: StorageJsonParser<T>): T | null {
  return readValue(key, (stored) => parse(JSON.parse(stored)))
}

export function readEnumValue<T extends string>(key: string, allowed: readonly T[]): T | null {
  return readValue(key, (stored) => (
    allowed.includes(stored as T) ? stored as T : null
  ))
}

export function readBooleanValue(key: string): boolean | null {
  return readValue(key, (stored) => stored === "true")
}
