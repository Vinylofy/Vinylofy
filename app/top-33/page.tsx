import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";

import { SearchControls } from "@/components/search/search-controls";
import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import {
  getDutchchartsVinylTop33,
  type DutchchartsVinylTop33Item,
} from "@/lib/dutchcharts-vinyl-top33";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "Vinyl Top 33",
  description: "De actuele Vinyl Top 33 op Vinylofy.",
};

function buildAlbumSearchHref(title: string): string {
  const params = new URLSearchParams();
  params.set("q", title.trim());
  return `/search?${params.toString()}`;
}

function Top33Row({ item }: { item: DutchchartsVinylTop33Item }) {
  const href = buildAlbumSearchHref(item.title);

  return (
    <li className="group rounded-3xl border border-neutral-200 bg-white p-4 shadow-sm transition hover:border-orange-200 hover:shadow-md sm:p-5">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex min-w-0 items-start gap-4">
          <div className="flex h-11 w-11 shrink-0 items-center justify-center rounded-full bg-orange-600 text-sm font-bold text-white">
            {item.rank}
          </div>

          <div className="min-w-0">
            <h2 className="text-xl font-semibold tracking-tight text-neutral-950 sm:text-2xl">
              {item.artist}
            </h2>

            <p className="mt-1 text-base font-medium leading-6 text-neutral-600 sm:text-lg">
              <Link className="hover:text-orange-600" href={href}>
                {item.title}
              </Link>
            </p>
          </div>
        </div>

        <Link
          className="inline-flex shrink-0 items-center justify-center rounded-full bg-orange-600 px-5 py-2.5 text-sm font-semibold !text-white transition hover:bg-orange-700 hover:!text-white focus:!text-white visited:!text-white focus:outline-none focus:ring-2 focus:ring-orange-500 focus:ring-offset-2"
          href={href}
          style={{ color: "#ffffff" }}
        >
          Zoek album
        </Link>
      </div>
    </li>
  );
}

export default async function Top33Page() {
  let items: DutchchartsVinylTop33Item[] = [];
  let loadError = false;

  try {
    items = await getDutchchartsVinylTop33();
  } catch (error) {
    loadError = true;
    console.warn("[vinylofy] Dutchcharts Vinyl 33 kon niet worden geladen", {
      message: error instanceof Error ? error.message : String(error),
    });
  }

  return (
    <>
      <SiteHeader searchSlot={<SearchControls initialQuery="" />} />

      <main className="mx-auto max-w-5xl px-4 py-10 sm:px-6 lg:px-8">
        <section className="rounded-[2rem] border border-neutral-200 bg-neutral-50 p-6 sm:p-10">
          <p className="inline-flex rounded-full bg-orange-100 px-3 py-1 text-xs font-semibold uppercase tracking-[0.2em] text-orange-700">
            Vinylofy chart
          </p>

          <div className="mt-6 flex justify-center">
            <Image
              src="/home-cards/top-25-white.png"
              alt="Vinyl Top 33"
              width={260}
              height={160}
              priority
              className="h-auto w-full max-w-[13rem]"
            />
          </div>

          <p className="mt-4 max-w-2xl text-base leading-7 text-neutral-600">
            Zoek direct vanuit de Vinyl Top 33 naar de juiste albumrelease.
          </p>

          <p className="mt-4 text-xs text-neutral-500">
            Bron: Dutchcharts Vinyl 33
          </p>
        </section>

        <section className="mt-8">
          {items.length > 0 ? (
            <ol className="space-y-3">
              {items.map((item) => (
                <Top33Row
                  key={`${item.rank}-${item.artist}-${item.title}`}
                  item={item}
                />
              ))}
            </ol>
          ) : (
            <div className="rounded-3xl border border-neutral-200 bg-white p-6 text-neutral-600">
              {loadError
                ? "De Vinyl Top 33 kon nu niet worden geladen."
                : "De Vinyl Top 33 is nog niet beschikbaar."}
            </div>
          )}
        </section>
      </main>

      <SiteFooter />
    </>
  );
}
