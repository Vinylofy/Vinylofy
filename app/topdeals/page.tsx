import type { Metadata } from "next";
import Link from "next/link";
import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import { TopDealCard } from "@/components/topdeals/top-deal-card";
import { getTopDeals } from "@/lib/vinylofy-data";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "Top 45 Deals",
  description: "De grootste prijsverschillen op vinyl, gevonden door Vinylofy.",
};

export default async function TopDealsPage() {
  const deals = (await getTopDeals(45))
    .filter((deal) => deal.priceDifference > 0 && deal.lowestOffer && deal.highestOffer)
    .slice(0, 45);

  return (
    <>
      <SiteHeader />
      <main className="mx-auto w-full max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
        <section className="rounded-[2rem] border border-neutral-200 bg-white p-6 shadow-sm sm:p-8">
          <p className="text-xs font-semibold uppercase tracking-[0.2em] text-neutral-500">
            Vinylofy deals
          </p>
          <div className="mt-4 max-w-3xl">
            <h1 className="text-3xl font-semibold tracking-tight text-neutral-950 sm:text-5xl">
              Top 45 Deals
            </h1>
            <p className="mt-4 text-base leading-7 text-neutral-600 sm:text-lg">
              De grootste prijsverschillen op vinyl, gevonden door Vinylofy.
            </p>
            <p className="mt-3 text-sm font-medium text-neutral-900">
              Zelfde plaat. Zelfde EAN. Andere prijs.
            </p>
            <p className="mt-2 text-sm text-neutral-500">
              We vergelijken actuele productprijzen. Verzendkosten tellen niet mee in deze ranking.
            </p>
          </div>
        </section>

        {deals.length > 0 ? (
          <section className="mt-8 grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            {deals.map((deal, index) => (
              <TopDealCard key={deal.id} deal={deal} rank={index + 1} />
            ))}
          </section>
        ) : (
          <section className="mt-8 rounded-[2rem] border border-dashed border-neutral-300 bg-white p-8 text-center">
            <p className="text-lg font-semibold text-neutral-950">
              Nog niet genoeg actuele prijsverschillen gevonden.
            </p>
            <p className="mx-auto mt-2 max-w-xl text-sm leading-6 text-neutral-500">
              Zodra meerdere aanbieders dezelfde plaat actueel met een prijs tonen, verschijnt hier de Top 45.
            </p>
            <Link
              href="/search"
              className="mt-6 inline-flex rounded-full bg-neutral-950 px-5 py-3 text-sm font-semibold text-white transition hover:bg-neutral-800"
            >
              Zoek vinyl
            </Link>
          </section>
        )}
      </main>
      <SiteFooter />
    </>
  );
}
