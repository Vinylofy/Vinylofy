import Image from "next/image";

import { GlobalSearchBar } from "@/components/global-search-bar";
import { HomeActionCards } from "@/components/home/home-action-cards";
import { MarketSelector } from "@/components/market-selector";
import type { PublicMarket } from "@/lib/markets";

export function HeroSearch({ marketCode, markets }: { marketCode: string; markets: PublicMarket[] }) {
  return (
    <section className="px-4 pb-4 pt-3 sm:px-6 md:pb-6">
      <div className="mx-auto flex max-w-6xl justify-end">
        <MarketSelector markets={markets} selected={marketCode} />
      </div>
      <div className="mx-auto mt-5 flex max-w-6xl flex-col items-center text-center">
        <div className="mb-3 w-full max-w-[220px] md:mb-4 md:max-w-[270px]">
          <Image
            src="/vinylofy-hero-logo-white3.png"
            alt="Vinylofy"
            width={1536}
            height={1152}
            priority
            className="mx-auto h-auto w-full"
          />
        </div>

        <div className="w-full max-w-[920px]">
          <GlobalSearchBar marketCode={marketCode} />
        </div>

        <p className="mt-2 text-sm text-neutral-500">
          Zoek op artiest of albumtitel. Suggesties tonen alleen artiesten.
        </p>

        <div className="mt-6 w-full max-w-[920px] md:mt-8">
          <HomeActionCards marketCode={marketCode} />
        </div>

      </div>
    </section>
  );
}
