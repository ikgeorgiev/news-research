import type { Metadata } from "next"
import { Bebas_Neue, IBM_Plex_Mono, IBM_Plex_Sans_Condensed } from "next/font/google"
import "./globals.css"

const display = Bebas_Neue({
  weight: "400",
  variable: "--font-display",
  subsets: ["latin"],
})

const body = IBM_Plex_Sans_Condensed({
  weight: ["300", "400", "500", "600", "700"],
  variable: "--font-body",
  subsets: ["latin"],
})

const mono = IBM_Plex_Mono({
  weight: ["400", "500", "600"],
  variable: "--font-mono",
  subsets: ["latin"],
})

export const metadata: Metadata = {
  title: "CEF Wire Deck",
  description: "Closed-end fund news feed dashboard",
}

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body className={`${display.variable} ${body.variable} ${mono.variable}`}>{children}</body>
    </html>
  )
}
