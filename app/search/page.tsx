import Link from "next/link";
import { ProductResultCard } from "@/components/search/product-result-card";
import { SearchControls } from "@/components/search/search-controls";
import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import { searchProducts, type SearchResultItem } from "@/lib/vinylofy-data";

type SearchPageProps = {
  searchParams: Promise<{
    q?: string;
    artist_filter?: string;
  }>;
};

type ArtistFilterOption = {
  key: string;
  artist: string;
  count: number;
  score: number;
};

function normalizeValue(value: string) {
  return value.toLowerCase().replace(/\s+/g, " ").trim();
}

function normalizeArtistDisplay(value: string) {
  const trimmed = value.trim();
  if (!trimmed) return trimmed;

  const isAllCaps = trimmed === trimmed.toUpperCase();
  if (!isAllCaps) return trimmed;

  const lower = trimmed.toLowerCase();
  return lower.charAt(0).toUpperCase() + lower.slice(1);
}

function scoreArtistOption(artist: string, query: string) {
  const value = normalizeValue(artist);
  const needle = normalizeValue(query);

  if (!needle) return 0;
  if (value === needle) return 1000;
  if (value.startsWith(`${needle} `)) return 850;
  if (value.startsWith(needle)) return 800;
  if (value.includes(` ${needle}`)) return 500;
  if (value.includes(needle)) return 250;
  return 0;
}

function getArtistFilterOptions(
  results: SearchResultItem[],
  query: string,
): ArtistFilterOption[] {
  const grouped = new Map<string, ArtistFilterOption>();

  for (const item of results) {
    const key = normalizeValue(item.artist);
    const existing = grouped.get(key);

    if (existing) {
      existing.count += 1;
      if (item.artist.length < existing.artist.length) {
        existing.artist = item.artist;
      }
      existing.score = Math.max(existing.score, scoreArtistOption(item.artist, query));
    } else {
      grouped.set(key, {
        key,
        artist: item.artist,
        count: 1,
        score: scoreArtistOption(item.artist, query),
      });
    }
  }

  return Array.from(grouped.values()).sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    if (b.count !== a.count) return b.count - a.count;
    return a.artist.localeCompare(b.artist, "nl", { sensitivity: "base" });
  });
}

function buildSearchHref(query: string, artistFilter?: string) {
  const params = new URLSearchParams();
  if (query) params.set("q", query);
  if (artistFilter) params.set("artist_filter", artistFilter);
  return `/search${params.toString() ? `?${params.toString()}` : ""}`;
}

function ArtistSidebar({
  query,
  results,
  activeArtistFilter,
}: {
  query: string;
  results: SearchResultItem[];
  activeArtistFilter: string;
}) {
  const artistOptions = getArtistFilterOptions(results, query);

  return (
    <aside className="h-fit rounded-3xl border border-neutral-200 bg-white p-5 shadow-sm">
      <div className="space-y-4">
        <h2 className="text-sm font-semibold text-neutral-900">Gevonden artiesten</h2>

        <div className="space-y-2 text-sm">
          <Link
            href={buildSearchHref(query)}
            className={`flex items-start justify-between gap-3 py-1 transition ${
              !activeArtistFilter
                ? "font-semibold text-neutral-900"
                : "text-neutral-600 hover:text-neutral-900"
            }`}
          >
            <span className="min-w-0 truncate">Alle resultaten</span>
            <span className="shrink-0 text-neutral-400">{results.length}</span>
          </Link>

          <div className="border-t border-neutral-100 pt-2">
            {artistOptions.length > 0 ? (
              <div className="space-y-1">
                {artistOptions.map((option) => {
                  const active =
                    normalizeValue(option.artist) === normalizeValue(activeArtistFilter);

                  return (
                    <Link
                      key={option.key}
                      href={buildSearchHref(query, option.artist)}
                      className={`flex items-start justify-between gap-3 py-2 transition ${
                        active
                          ? "font-semibold text-neutral-900"
                          : "text-neutral-600 hover:text-neutral-900"
                      }`}
                    >
                      <span className="min-w-0 truncate">
                        {normalizeArtistDisplay(option.artist)}
                      </span>
                      <span className="shrink-0 text-neutral-400">{option.count}</span>
                    </Link>
                  );
                })}
              </div>
            ) : (
              <p className="py-2 text-neutral-500">Geen artiesten gevonden</p>
            )}
          </div>
        </div>
      </div>
    </aside>
  );
}

export default async function SearchPage({ searchParams }: SearchPageProps) {
  const params = await searchParams;
  const query = params.q?.trim() || "";
  const activeArtistFilter = params.artist_filter?.trim() || "";
  const results = query ? await searchProducts(query) : [];
  const filteredResults = activeArtistFilter
    ? results.filter((item) => normalizeValue(item.artist) === normalizeValue(activeArtistFilter))
    : results;

  return (
    <div className="min-h-screen bg-[#f8f7f4] text-neutral-900">
      <SiteHeader searchSlot={<SearchControls initialQuery={query} />} />

      <main className="mx-auto max-w-7xl px-6 py-8 md:py-10">
        <section className="space-y-5">
          <div className="space-y-2">
            <h1 className="text-2xl font-semibold tracking-tight">
              Resultaten voor: {query ? `“${query}”` : "…"}
            </h1>
            <p className="text-sm text-neutral-500">Zoeken op artiest en titel</p>
          </div>

          {!query ? (
            <div className="rounded-3xl border border-neutral-200 bg-white p-8 text-sm text-neutral-600 shadow-sm">
              Typ een artiest of albumtitel om te zoeken.
            </div>
          ) : (
            <div className="grid gap-6 lg:grid-cols-[220px_minmax(0,1fr)]">
              <ArtistSidebar
                query={query}
                results={results}
                activeArtistFilter={activeArtistFilter}
              />

              <div className="max-w-[920px] space-y-4">
                {filteredResults.length === 0 ? (
                  <div className="rounded-3xl border border-neutral-200 bg-white p-8 text-sm text-neutral-600 shadow-sm">
                    Geen resultaten gevonden voor {query}. Probeer een artiest, albumtitel of een
                    kortere zoekterm.
                  </div>
                ) : (
                  filteredResults.map((item) => (
                    <ProductResultCard key={item.id} item={item} />
                  ))
                )}
              </div>
            </div>
          )}
        </section>
      </main>

      <SiteFooter />
    </div>
  );
}
