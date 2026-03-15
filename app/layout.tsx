import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: {
    default: "Vinylofy",
    template: "%s | Vinylofy",
  },
  description: "Vinyl offers for you — prijsvergelijker voor vinylplaten.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="nl">
      <body>{children}</body>
    </html>
  );
}