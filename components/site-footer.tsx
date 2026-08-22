import Link from "next/link";

export function SiteFooter() {
  return (
    <footer className="mt-16 border-t border-neutral-200 bg-white">
      <div className="mx-auto flex max-w-6xl flex-col gap-4 px-6 py-8 md:flex-row md:items-center md:justify-between">
        <p className="text-sm text-neutral-500">
          © 2026 Vinylofy — Vinyl offers for you
        </p>

        <div className="flex flex-wrap items-center gap-x-6 gap-y-2 text-sm text-neutral-500">
          <Link href="/over" className="hover:text-neutral-900">
            Over
          </Link>
          <Link href="/shops" className="hover:text-neutral-900">
            Shops
          </Link>
          <Link href="/privacy" className="hover:text-neutral-900">
            Privacy
          </Link>
          <div className="flex flex-col items-start gap-1">
            <Link href="/contact" className="hover:text-neutral-900">
              Contact
            </Link>
            <Link
              href="/rvw/4cd7a8cf23027e2a1f0cafb75483c4a638fee4bac4533a07a6d6c5ce1bc834a2/artwork"
              className="text-xs text-neutral-400 hover:text-neutral-900"
            >
              Admin
            </Link>
          </div>
        </div>
      </div>
    </footer>
  );
}
