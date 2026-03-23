import type { Metadata } from "next";

import { NewReleasesGrid } from "@/components/home/new-releases-grid";
import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import { getHomePageData } from "@/lib/vinylofy-data";

export const metadata: Metadata = {
  title: "Nieuwe releases",
};

export default async function NieuweReleasesPage() {
  const { newReleases } = await getHomePageData();

  return (
    <main className="min-h-screen bg-neutral-50 text-neutral-950">
      <SiteHeader />

      <section className="mx-auto max-w-6xl px-6 pb-4 pt-8 md:pt-10">
        <div className="max-w-3xl">
          <div className="mb-3 text-xs font-semibold uppercase tracking-[0.18em] text-orange-600">
            Landingpagina
          </div>
          <h1 className="text-4xl font-semibold tracking-tight text-neutral-950">
            Nieuwe releases
          </h1>
          <p className="mt-3 text-base leading-7 text-neutral-600">
            De nieuwste titels op een rij. Deze pagina gebruikt nu dezelfde dataset als de homepage en kan later worden uitgebreid met filters, sortering en een eigen release-logica.
          </p>
        </div>
      </section>

      <NewReleasesGrid items={newReleases} />
      <SiteFooter />
    </main>
  );
}
