import Image from "next/image";
import Link from "next/link";
import type { ReactNode } from "react";
import { MarketSelector } from "@/components/market-selector";
import { getActiveMarkets } from "@/lib/markets";
import { marketHref } from "@/lib/market-url";

type SiteHeaderProps = {
  searchSlot?: ReactNode;
  marketCode?: string;
};

const headerLinkClassName =
  "focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-orange-600";

const navLinkClassName =
  "hover:text-neutral-900 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-orange-600";

export async function SiteHeader({ searchSlot, marketCode = "NL" }: SiteHeaderProps) {
  const markets = await getActiveMarkets();
  const selected = markets.some((market) => market.country_code === marketCode)
    ? marketCode : "NL";
  if (searchSlot) {
    return (
      <header className="sticky top-0 z-30 border-b border-neutral-200 bg-white/95 backdrop-blur">
        <div className="mx-auto max-w-7xl px-4 py-3 sm:px-6 md:py-4">
          <div className="grid grid-cols-[minmax(0,1fr)_auto] items-center gap-x-3 gap-y-3 lg:grid-cols-[140px_minmax(0,1fr)_auto] lg:gap-x-6">
            <Link href={marketHref("/", selected)} className={`col-start-1 row-start-1 inline-flex w-fit items-center ${headerLinkClassName}`} aria-label="Ga naar home">
              <Image
                src="/vinylofy-header-logo.png"
                alt="Vinylofy"
                width={320}
                height={100}
                priority
                className="h-auto w-[90px] md:w-[110px]"
              />
            </Link>

            <div className="col-span-2 row-start-2 min-w-0 lg:col-span-1 lg:col-start-2 lg:row-start-1">
              <div className="w-full max-w-[920px]">
                {searchSlot}
              </div>
            </div>
            <div className="col-start-2 row-start-1 justify-self-end lg:col-start-3">
              <MarketSelector markets={markets} selected={selected} />
            </div>
          </div>
        </div>
      </header>
    );
  }

  return (
    <header className="sticky top-0 z-30 border-b border-neutral-200 bg-white/95 backdrop-blur">
      <div className="mx-auto grid max-w-7xl grid-cols-[minmax(0,1fr)_auto] items-center gap-x-3 gap-y-2 px-4 py-3 sm:flex sm:px-6 sm:py-4">
        <Link href={marketHref("/", selected)} className={`col-start-1 row-start-1 inline-flex w-fit items-center ${headerLinkClassName}`} aria-label="Ga naar home">
          <Image
            src="/vinylofy-header-logo.png"
            alt="Vinylofy"
            width={320}
            height={100}
            priority
            className="h-auto w-[90px] md:w-[110px]"
          />
        </Link>

        <nav className="col-span-2 row-start-2 flex items-center justify-end gap-4 text-sm text-neutral-500 sm:ml-auto">
          <Link href={marketHref("/shops", selected)} className={navLinkClassName}>
            Shops
          </Link>
          <Link href="/over" className={navLinkClassName}>
            Over
          </Link>
        </nav>
        <div className="col-start-2 row-start-1 justify-self-end">
          <MarketSelector markets={markets} selected={selected} />
        </div>
      </div>
    </header>
  );
}
