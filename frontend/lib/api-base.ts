const API_BASE = (process.env.NEXT_PUBLIC_API_BASE ?? "").trim().replace(/\/+$/, "")

export function buildApiUrl(path: string): string {
  return API_BASE ? `${API_BASE}${path}` : path
}
