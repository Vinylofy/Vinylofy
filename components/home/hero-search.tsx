import Image from "next/image";
import Link from "next/link";

import { GlobalSearchBar } from "@/components/global-search-bar";
import { HomeActionCards } from "@/components/home/home-action-cards";

export function HeroSearch() {
  return (
    <section className="px-6 pb-4 pt-4 md:pb-6 md:pt-6">
      <div className="mx-auto flex max-w-6xl flex-col items-center text-center">
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
          <GlobalSearchBar />
        </div>

        <p className="mt-2 text-sm text-neutral-500">
          Zoek op artiest of albumtitel. Suggesties tonen alleen artiesten.
        </p>

        <div className="mt-4 w-full max-w-[920px]">
          <HomeActionCards />
        </div>

        <div className="mt-8 w-full max-w-[920px] rounded-3xl border border-neutral-200 bg-white p-5 text-left shadow-sm md:p-6">
          <p className="text-xs font-medium uppercase tracking-[0.16em] text-orange-600">Follow the Groove</p>
          <div className="mt-2 flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
            <p className="text-sm text-neutral-600">Ontdek muziek via echte connecties tussen artiesten.</p>
            <Link
              href="/follow-the-groove"
              className="inline-flex min-h-11 shrink-0 items-center justify-center rounded-full border border-neutral-300 px-5 py-2 text-sm font-medium text-neutral-800 transition hover:border-orange-300 hover:bg-orange-50 hover:text-orange-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-orange-300 focus-visible:ring-offset-2"
            >
              Start je groove →
            </Link>
          </div>
        </div>
      </div>
    </section>
  );
}
