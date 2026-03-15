import Link from "next/link";
import { Logo } from "@/components/logo";

export function SiteHeader() {
  return (
    <header className="sticky top-0 z-30 border-b border-neutral-200 bg-white/95 backdrop-blur">
      <div className="mx-auto flex max-w-6xl items-center justify-between px-6 py-4">
        <Link href="/" className="inline-flex items-center">
          <Logo size="sm" />
        </Link>

        <nav className="hidden items-center gap-6 text-sm text-neutral-500 md:flex">
          <Link href="/shops" className="hover:text-neutral-900">
            Shops
          </Link>
          <Link href="/over" className="hover:text-neutral-900">
            Over
          </Link>
        </nav>
      </div>
    </header>
  );
}