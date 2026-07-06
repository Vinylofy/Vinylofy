import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";

import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import { getReleaseCalendarItems } from "@/lib/vinylofy-data";

export const metadata: Metadata = {
  title: "Nieuwe releases",
};

function formatReleaseDate(value: string): string {
  return new Intl.DateTimeFormat("nl-NL", {
    day: "numeric",
    month: "long",
    year: "numeric",
  }).format(new Date(`${value}T00:00:00Z`));
}

function sourceLabel(sourceShop: string): string {
  if (sourceShop === "bobsvinyl") return "Bob's Vinyl";
  return sourceShop;
}

export default async function NieuweReleasesPage() {
  const releases = await getReleaseCalendarItems();

  return (
    <main className="min-h-screen bg-neutral-50 text-neutral-950">
      <SiteHeader />

      <section className="mx-auto max-w-6xl px-6 pb-4 pt-8 md:pt-10">
        <div className="max-w-3xl">
          <div className="mb-3 text-xs font-semibold uppercase tracking-[0.18em] text-orange-600">
            Releasekalender
          </div>

          <h1 className="text-4xl font-semibold tracking-tight text-neutral-950">
            Nieuwe releases
          </h1>

          <p className="mt-3 text-base leading-7 text-neutral-600">
            Aankomende en recente vinyl releases, verzameld uit release-informatie van aangesloten bronnen.
          </p>
        </div>
      </section>

      <section className="mx-auto max-w-6xl px-6 pb-16">
        {releases.length === 0 ? (
          <div className="rounded-3xl border border-neutral-200 bg-white p-6 text-sm text-neutral-600">
            Nog geen releases beschikbaar.
          </div>
        ) : (
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            {releases.map((release) => {
              const card = (
                <article className="h-full overflow-hidden rounded-3xl border border-neutral-200 bg-white shadow-sm">
                  <div className="aspect-square bg-neutral-100">
                    {release.imageUrl ? (
                      <Image
                        src={release.imageUrl}
                        alt={`${release.artist} - ${release.title}`}
                        width={600}
                        height={600}
                        className="h-full w-full object-cover"
                      />
                    ) : (
                      <div className="flex h-full items-center justify-center px-6 text-center text-sm text-neutral-400">
                        Geen afbeelding beschikbaar
                      </div>
                    )}
                  </div>

                  <div className="p-4">
                    <div className="text-xs font-semibold uppercase tracking-[0.14em] text-orange-600">
                      {formatReleaseDate(release.releaseDate)}
                    </div>

                    <h2 className="mt-2 text-sm font-semibold text-neutral-950">
                      {release.artist}
                    </h2>

                    <p className="mt-1 text-sm leading-6 text-neutral-700">
                      {release.title}
                    </p>

                    <div className="mt-4 flex items-center justify-between gap-3 text-xs text-neutral-500">
                      <span>{sourceLabel(release.sourceShop)}</span>
                      {release.productId ? (
                        <span className="font-medium text-neutral-700">Bekijk prijzen</span>
                      ) : (
                        <span>Release-info</span>
                      )}
                    </div>
                  </div>
                </article>
              );

              if (release.productId) {
                return (
                  <Link key={release.id} href={`/product/${release.productId}`} className="block">
                    {card}
                  </Link>
                );
              }

              return (
                <a
                  key={release.id}
                  href={release.sourceUrl}
                  target="_blank"
                  rel="noreferrer"
                  className="block"
                >
                  {card}
                </a>
              );
            })}
          </div>
        )}
      </section>

      <SiteFooter />
    </main>
  );
}
