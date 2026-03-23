import Image from "next/image";
import Link from "next/link";
import type { ReactNode } from "react";

type SiteHeaderProps = {
  searchSlot?: ReactNode;
};

export function SiteHeader({ searchSlot }: SiteHeaderProps) {
  if (searchSlot) {
    return (
      <header className="sticky top-0 z-30 border-b border-neutral-200 bg-white/95 backdrop-blur">
        <div className="mx-auto max-w-7xl px-6 py-3 md:py-4">
          <div className="flex flex-col gap-3 lg:grid lg:grid-cols-[220px_minmax(0,1fr)] lg:items-center lg:gap-6">
            <div className="flex items-center">
              <Link href="/" className="inline-flex items-center" aria-label="Ga naar home">
                <Image
                  src="/vinylofy-header-logo.png"
                  alt="Vinylofy"
                  width={320}
                  height={100}
                  priority
                  className="h-auto w-[90px] md:w-[110px]"
                />
              </Link>
            </div>

            <div className="min-w-0">
              <div className="w-full max-w-[920px]">
                {searchSlot}
              </div>
            </div>
          </div>
        </div>
      </header>
    );
  }

  return (
    <header className="sticky top-0 z-30 border-b border-neutral-200 bg-white/95 backdrop-blur">
      <div className="mx-auto flex max-w-7xl items-center justify-between px-6 py-4">
        <Link href="/" className="inline-flex items-center" aria-label="Ga naar home">
          <Image
            src="/vinylofy-header-logo.png"
            alt="Vinylofy"
            width={320}
            height={100}
            priority
            className="h-auto w-[90px] md:w-[110px]"
          />
        </Link>

        <nav className="flex items-center gap-6 text-sm text-neutral-500">
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
