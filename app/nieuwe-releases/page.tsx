import type { Metadata } from "next";
import Link from "next/link";

import { CoverImage } from "@/components/cover-image";

import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import {
  formatEuro,
  getReleaseCalendarItems,
  getUpcomingReleaseCalendarItems,
  type ReleaseCalendarItem,
} from "@/lib/vinylofy-data";

export const dynamic = "force-dynamic";

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

function formatCompactReleaseDate(value: string): string {
  return new Intl.DateTimeFormat("nl-NL", {
    day: "numeric",
    month: "short",
    year: "numeric",
  }).format(new Date(`${value}T00:00:00Z`));
}

function formatVanafPrice(value: number | null | undefined): string | null {
  if (value === null || value === undefined) return null;
  return `Vanaf ${formatEuro(value)}`;
}

function groupByReleaseDate(releases: ReleaseCalendarItem[]) {
  const groups = new Map<string, ReleaseCalendarItem[]>();

  for (const release of releases) {
    groups.set(release.releaseDate, [...(groups.get(release.releaseDate) ?? []), release]);
  }

  return Array.from(groups.entries()).map(([releaseDate, items]) => ({
    releaseDate,
    items,
  }));
}


function cleanReleaseTitle(value: string): string {
  return value
    .replace(/\s+\|\s*Bob'?s Vinyl\s*$/i, "")
    .replace(/\s+\|\s*Bobsvinyl\s*$/i, "")
    .trim();
}

function ReleaseTitleLink({ release }: { release: ReleaseCalendarItem }) {
  const displayTitle = cleanReleaseTitle(release.title);

  if (release.productId) {
    return (
      <Link
        href={`/product/${release.productId}`}
        className="font-medium text-neutral-950 transition hover:text-orange-600"
      >
        {displayTitle}
      </Link>
    );
  }

  return (
    <a
      href={release.sourceUrl}
      target="_blank"
      rel="noreferrer"
      className="font-medium text-neutral-950 transition hover:text-orange-600"
    >
      {displayTitle}
    </a>
  );
}

function ReleaseCard({ release }: { release: ReleaseCalendarItem }) {
  const href = release.productId ? `/product/${release.productId}` : release.sourceUrl;
  const displayTitle = cleanReleaseTitle(release.title);
  const displayPrice = formatVanafPrice(release.lowestPrice);

  const card = (
    <article className="h-full overflow-hidden rounded-3xl border border-neutral-200 bg-white shadow-sm">
      <div className="aspect-square bg-neutral-100">
        <CoverImage
          src={release.imageUrl}
          storagePath={release.imageStoragePath}
          alt={`${release.artist} - ${displayTitle}`}
          className="h-full w-full object-cover data-[cover-fallback=true]:object-contain"
          loading="lazy"
        />
      </div>

      <div className="p-4">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <h2 className="truncate text-sm font-semibold text-neutral-950">
              {release.artist}
            </h2>
            <p className="mt-1 line-clamp-2 min-h-12 text-sm leading-6 text-neutral-700">
              {displayTitle}
            </p>
          </div>
        </div>

        <div className="mt-3 min-h-5 space-y-1 text-xs text-neutral-500">
          {release.label ? <div className="truncate">{release.label}</div> : null}
          {release.format ? <div className="truncate">{release.format}</div> : null}
        </div>

        <div className="mt-4 flex flex-col gap-3">
          {displayPrice ? (
            <div className="text-lg font-semibold text-neutral-950">
              {displayPrice}
            </div>
          ) : null}

          {release.productId ? (
            <span className="inline-flex w-fit items-center justify-center rounded-full bg-orange-500 px-4 py-2 text-sm font-medium !text-white transition hover:bg-orange-600 hover:!text-white">
              Bekijk alle aanbiedingen
            </span>
          ) : (
            <span className="text-xs font-medium text-neutral-500">
              Bekijk bij bron
            </span>
          )}
        </div>
      </div>
    </article>
  );

  if (release.productId) {
    return (
      <Link href={href} className="block h-full">
        {card}
      </Link>
    );
  }

  return (
    <a href={href} target="_blank" rel="noreferrer" className="block h-full">
      {card}
    </a>
  );
}
export default async function NieuweReleasesPage() {
  const [releases, upcomingReleases] = await Promise.all([
    getReleaseCalendarItems(180),
    getUpcomingReleaseCalendarItems(500),
  ]);
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
            Recente vinyl releases uit aangesloten releasebronnen, aangevuld met een compacte kalender voor wat eraan komt.
          </p>

          <div className="mt-5 flex flex-wrap gap-2 text-xs text-neutral-600">
            <span className="rounded-full border border-neutral-200 bg-white px-3 py-1">
              {releases.length} releases
            </span>
            <span className="rounded-full border border-neutral-200 bg-white px-3 py-1">
              {linkedCount} gekoppeld aan Vinylofy
            </span>
            <span className="rounded-full border border-neutral-200 bg-white px-3 py-1">
              {upcomingReleases.length} verwacht
            </span>
          </div>
        </div>
      </section>

      <section className="mx-auto max-w-6xl px-6 pb-10">
        {groupedReleases.length === 0 ? (
          <div className="rounded-2xl border border-neutral-200 bg-white p-6 text-sm text-neutral-600">
            Nog geen releases beschikbaar.
          </div>
        ) : (
          <div className="space-y-10">
            {groupedReleases.map((group) => (
              <section key={group.releaseDate}>
                <div className="mb-4 border-b border-neutral-200 pb-3">
                  <h2 className="text-xl font-semibold text-neutral-950">
                    {formatReleaseDate(group.releaseDate)}
                  </h2>
                  <p className="mt-1 text-sm text-neutral-500">
                    {group.items.length} releases
                  </p>
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

      <section className="mx-auto max-w-6xl px-6 pb-16">
        <div className="mb-4 border-b border-neutral-200 pb-3">
          <div className="text-xs font-semibold uppercase tracking-[0.18em] text-orange-600">
            Binnenkort op vinyl
          </div>
          <h2 className="mt-2 text-2xl font-semibold tracking-tight text-neutral-950">
            Verwachte releases
          </h2>
        </div>

        {upcomingReleases.length === 0 ? (
          <div className="rounded-2xl border border-neutral-200 bg-white p-6 text-sm text-neutral-600">
            Nog geen toekomstige releases beschikbaar.
          </div>
        ) : (
          <div className="overflow-hidden rounded-2xl border border-neutral-200 bg-white shadow-sm">
            <div className="hidden md:block">
              <table className="w-full border-collapse text-left text-sm">
                <thead className="bg-neutral-100 text-xs font-semibold uppercase tracking-[0.12em] text-neutral-500">
                  <tr>
                    <th scope="col" className="w-36 px-4 py-3">
                      Datum
                    </th>
                    <th scope="col" className="px-4 py-3">
                      Artiest
                    </th>
                    <th scope="col" className="px-4 py-3">
                      Titel
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-neutral-100">
                  {upcomingReleases.map((release) => (
                    <tr key={release.id} className="align-top transition hover:bg-orange-50/50">
                      <td className="whitespace-nowrap px-4 py-3 text-neutral-600">
                        {formatCompactReleaseDate(release.releaseDate)}
                      </td>
                      <td className="px-4 py-3 font-medium text-neutral-950">
                        {release.artist}
                      </td>
                      <td className="px-4 py-3">
                        <ReleaseTitleLink release={release} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <div className="divide-y divide-neutral-100 md:hidden">
              {upcomingReleases.map((release) => (
                <article key={release.id} className="px-4 py-4">
                  <div className="text-xs font-medium uppercase tracking-[0.12em] text-neutral-500">
                    {formatCompactReleaseDate(release.releaseDate)}
                  </div>
                  <h3 className="mt-2 text-sm font-semibold text-neutral-950">
                    {release.artist}
                  </h3>
                  <p className="mt-1 text-sm leading-6 text-neutral-700">
                    <ReleaseTitleLink release={release} />
                  </p>
                </article>
              ))}
            </div>
          </div>
        )}
      </section>

      <SiteFooter />
    </main>
  );
}
