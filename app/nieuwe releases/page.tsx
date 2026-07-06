import type { Metadata } from "next";
import Link from "next/link";

import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import { getReleaseCalendarItems, type ReleaseCalendarItem } from "@/lib/vinylofy-data";

export const metadata: Metadata = {
  title: "Nieuwe releases",
};

function formatReleaseDate(value: string): string {
  return new Intl.DateTimeFormat("nl-NL", {
    weekday: "long",
    day: "numeric",
    month: "long",
    year: "numeric",
  }).format(new Date(`${value}T00:00:00Z`));
}

function sourceLabel(sourceShop: string): string {
  if (sourceShop === "bobsvinyl") return "Bob's Vinyl";
  return sourceShop;
}

function groupByReleaseDate(releases: ReleaseCalendarItem[]) {
  const groups = new Map<string, ReleaseCalendarItem[]>();

  for (const release of releases) {
    const existing = groups.get(release.releaseDate) ?? [];
    existing.push(release);
    groups.set(release.releaseDate, existing);
  }

  return Array.from(groups.entries()).map(([releaseDate, items]) => ({
    releaseDate,
    items,
  }));
}

function ReleaseCard({ release }: { release: ReleaseCalendarItem }) {
  const href = release.productId ? `/product/${release.productId}` : release.sourceUrl;
  const isExternal = !release.productId;

  const content = (
    <article className="group h-full overflow-hidden rounded-3xl border border-neutral-200 bg-white shadow-sm transition hover:-translate-y-0.5 hover:shadow-md">
      <div className="aspect-square bg-neutral-100">
        {release.imageUrl ? (
          <img
            src={release.imageUrl}
            alt={`${release.artist} - ${release.title}`}
            className="h-full w-full object-cover"
            loading="lazy"
          />
        ) : (
          <div className="flex h-full items-center justify-center px-6 text-center text-sm text-neutral-400">
            Geen afbeelding beschikbaar
          </div>
        )}
      </div>

      <div className="p-4">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <h2 className="line-clamp-1 text-sm font-semibold text-neutral-950">
              {release.artist}
            </h2>

            <p className="mt-1 line-clamp-2 text-sm leading-6 text-neutral-700">
              {release.title}
            </p>
          </div>

          <span className="shrink-0 rounded-full bg-neutral-100 px-2.5 py-1 text-[11px] font-medium text-neutral-600">
            {sourceLabel(release.sourceShop)}
          </span>
        </div>

        <div className="mt-4 space-y-1 text-xs text-neutral-500">
          {release.ean ? <div>EAN {release.ean}</div> : null}
          {release.label ? <div>{release.label}</div> : null}
          {release.format ? <div>{release.format}</div> : null}
        </div>

        <div className="mt-4 text-xs font-medium text-neutral-800">
          {release.productId ? "Bekijk op Vinylofy" : "Bekijk bij bron"}
        </div>
      </div>
    </article>
  );

  if (isExternal) {
    return (
      <a href={href} target="_blank" rel="noreferrer" className="block h-full">
        {content}
      </a>
    );
  }

  return (
    <Link href={href} className="block h-full">
      {content}
    </Link>
  );
}

export default async function NieuweReleasesPage() {
  const releases = await getReleaseCalendarItems(180);
  const groupedReleases = groupByReleaseDate(releases);
  const linkedCount = releases.filter((release) => release.productId).length;

  return (
    <main className="min-h-screen bg-neutral-50 text-neutral-950">
      <SiteHeader />

      <section className="mx-auto max-w-6xl px-6 pb-6 pt-8 md:pt-10">
        <div className="max-w-3xl">
          <div className="mb-3 text-xs font-semibold uppercase tracking-[0.18em] text-orange-600">
            Releasekalender
          </div>

          <h1 className="text-4xl font-semibold tracking-tight text-neutral-950">
            Nieuwe releases
          </h1>

          <p className="mt-3 text-base leading-7 text-neutral-600">
            Aankomende en recente vinyl releases uit aangesloten releasebronnen. Releases met een bekende EAN kunnen naar een Vinylofy productpagina linken.
          </p>

          <div className="mt-5 flex flex-wrap gap-2 text-xs text-neutral-600">
            <span className="rounded-full border border-neutral-200 bg-white px-3 py-1">
              {releases.length} releases
            </span>
            <span className="rounded-full border border-neutral-200 bg-white px-3 py-1">
              {linkedCount} gekoppeld aan Vinylofy
            </span>
          </div>
        </div>
      </section>

      <section className="mx-auto max-w-6xl px-6 pb-16">
        {groupedReleases.length === 0 ? (
          <div className="rounded-3xl border border-neutral-200 bg-white p-6 text-sm text-neutral-600">
            Nog geen releases beschikbaar.
          </div>
        ) : (
          <div className="space-y-10">
            {groupedReleases.map((group) => (
              <section key={group.releaseDate}>
                <div className="mb-4 flex items-end justify-between gap-4 border-b border-neutral-200 pb-3">
                  <div>
                    <h2 className="text-xl font-semibold text-neutral-950">
                      {formatReleaseDate(group.releaseDate)}
                    </h2>
                    <p className="mt-1 text-sm text-neutral-500">
                      {group.items.length} releases
                    </p>
                  </div>
                </div>

                <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
                  {group.items.map((release) => (
                    <ReleaseCard key={release.id} release={release} />
                  ))}
                </div>
              </section>
            ))}
          </div>
        )}
      </section>

      <SiteFooter />
    </main>
  );
}
