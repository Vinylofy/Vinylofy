import type { Metadata } from "next";

import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import { TopVinylList } from "@/components/home/top-vinyl-list";
import { CoverQueueBeacon } from "@/components/cover-queue-beacon";
import { getHomePageData } from "@/lib/vinylofy-data";

export const metadata: Metadata = {
  title: "Top 25",
};

export default async function Top25Page() {
  const { top25 } = await getHomePageData();

  return (
    <main className="min-h-screen bg-neutral-50 text-neutral-950">
      <SiteHeader />

      <section className="mx-auto max-w-6xl px-6 pb-4 pt-8 md:pt-10">
        <div className="max-w-3xl">
          <div className="mb-3 text-xs font-semibold uppercase tracking-[0.18em] text-orange-600">
            Landingpagina
          </div>
          <h1 className="text-4xl font-semibold tracking-tight text-neutral-950">
            Populaire vinyl
          </h1>
          <p className="mt-3 text-base leading-7 text-neutral-600">
            Bekijk de top 25 van nu. Deze pagina gebruikt nu de bestaande top 25-data van de homepage en is bedoeld als eerste dummy landingspagina voor een latere, rijkere lijstweergave.
          </p>
        </div>
      </section>

      <TopVinylList items={top25} />
      <CoverQueueBeacon productIds={top25.map((item) => item.id)} source="homepage" priorityBump={650} />
      <SiteFooter />
    </main>
  );
}
