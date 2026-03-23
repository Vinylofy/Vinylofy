import Image from "next/image";

import { GlobalSearchBar } from "@/components/global-search-bar";
import { HomeActionCards } from "@/components/home/home-action-cards";

export function HeroSearch() {
  return (
    <section className="px-6 pb-4 pt-4 md:pb-6 md:pt-6">
      <div className="mx-auto flex max-w-6xl flex-col items-center text-center">
        <div className="mb-3 w-full max-w-[220px] md:mb-4 md:max-w-[270px]">
          <Image
            src="/vinylofy-hero-logo.png"
            alt="Vinylofy"
            width={1536}
            height={1152}
            priority
            className="mx-auto h-auto w-full"
          />
        </div>

        <div className="w-full max-w-[920px]">
          <GlobalSearchBar />
        </div>

        <p className="mt-2 text-sm text-neutral-500">
          Zoek direct op artiest, album of nummer.
        </p>

        <div className="mt-4 w-full max-w-[920px]">
          <HomeActionCards />
        </div>
      </div>
    </section>
  );
}