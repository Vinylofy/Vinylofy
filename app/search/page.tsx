import Link from "next/link";
import { ProductResultCard } from "@/components/search/product-result-card";
import { SearchControls } from "@/components/search/search-controls";
import { SearchSortSelect } from "@/components/search/search-sort-select";
import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import { CoverQueueBeacon } from "@/components/cover-queue-beacon";
import { GrooveSearchBlock } from "@/components/follow-the-groove/groove-search-block";
import { searchProducts, type SearchResultItem } from "@/lib/vinylofy-data";
import { getFollowTheGroovePage } from "@/lib/follow-the-groove/data";
import {
  parseSearchSort,
  sortSearchResults,
  type SearchSort,
} from "@/lib/search-sort";

type SearchPageProps = {
  searchParams: Promise<{
    q?: string;
    artist_filter?: string;
    sort?: string;
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

function buildSearchHref(
  query: string,
  sort: SearchSort,
  artistFilter?: string,
) {
  const params = new URLSearchParams();

  if (query) {
    params.set("q", query);
  }

  if (artistFilter) {
    params.set("artist_filter", artistFilter);
  }

  params.set("sort", sort);

  return `/search?${params.toString()}`;
}

function ArtistSidebar({
  query,
  results,
  activeArtistFilter,
  activeSort,
}: {
  query: string;
  results: SearchResultItem[];
  activeArtistFilter: string;
  activeSort: SearchSort;
}) {
  const artistOptions = getArtistFilterOptions(results, query);

  return (
    <aside className="h-fit rounded-3xl border border-neutral-200 bg-white p-5 shadow-sm">
      <div className="space-y-4">
        <h2 className="text-sm font-semibold text-neutral-900">Gevonden artiesten</h2>

        <div className="space-y-2 text-sm">
          <Link
            href={buildSearchHref(query, activeSort)}
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
                      href={buildSearchHref(
                  query,
                  activeSort,
                  option.artist,
                )}
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
  const activeArtistFilter =
    params.artist_filter?.trim() || "";

  const activeSort = parseSearchSort(params.sort);
  const results = query
    ? await searchProducts(query, { limit: null })
    : [];
  const filteredResults = activeArtistFilter
    ? results.filter(
        (item) =>
          normalizeValue(item.artist) ===
          normalizeValue(activeArtistFilter),
      )
    : results;

  const visibleResults = sortSearchResults(
    filteredResults,
    activeSort,
  ).slice(0, 24);
  const unfilteredArtistOptions = !activeArtistFilter ? getArtistFilterOptions(results, query) : [];
  const inferredArtistContext = activeArtistFilter ||
    (unfilteredArtistOptions.length === 1 ? unfilteredArtistOptions[0].artist : "");
  const grooveData = inferredArtistContext && visibleResults.length > 0
    ? await getFollowTheGroovePage({
        trailMbids: [],
        artistName: inferredArtistContext,
        mode: "search",
        limit: 3,
      }).catch(() => null)
    : null;
  const firstResults = visibleResults.slice(0, 5);
  const remainingResults = visibleResults.slice(5);

  return (
    <div className="min-h-screen bg-[#f8f7f4] text-neutral-900">
      <SiteHeader searchSlot={<SearchControls initialQuery={query} />} />

      <main className="mx-auto max-w-7xl px-6 py-8 md:py-10">
        <section className="space-y-5">
          <div className="space-y-2">
            <h1 className="text-2xl font-semibold tracking-tight">
              Resultaten voor: {query ? `“${query}”` : "…"}
            </h1>
            <p className="text-sm text-neutral-500">Zoek op artiest of albumtitel. Suggesties tonen alleen artiesten.</p>
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
      activeSort={activeSort}
              />

              <div className="max-w-[920px] space-y-4">
                {filteredResults.length === 0 ? (
                  <div className="rounded-3xl border border-neutral-200 bg-white p-8 text-sm text-neutral-600 shadow-sm">
                    Geen resultaten gevonden voor {query}. Probeer een artiest, albumtitel of een
                    kortere zoekterm.
                  </div>
                ) : (
                  <>
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              <p className="text-xs text-neutral-500">
                {visibleResults.length === filteredResults.length
                  ? `${filteredResults.length} resultaten`
                  : `${visibleResults.length} van ${filteredResults.length} resultaten`}
              </p>

              <SearchSortSelect
                value={activeSort}
                query={query}
                artistFilter={activeArtistFilter}
              />
            </div>
                {firstResults.map((item) => (
                  <ProductResultCard key={item.id} item={item} />
                ))}
                {grooveData && grooveData.candidates.length > 0 ? (
                  <GrooveSearchBlock
                    activeArtistName={grooveData.artist.name}
                    activeArtistMbid={grooveData.artist.mbid}
                    candidates={grooveData.candidates}
                  />
                ) : null}
                {remainingResults.map((item) => (
                  <ProductResultCard key={item.id} item={item} />
                ))}
              </>
                )}
              </div>
            </div>
          )}
        </section>
      </main>

      {visibleResults.length > 0 ? (
        <CoverQueueBeacon
          productIds={visibleResults.map((item) => item.id)}
          source="search"
          priorityBump={400}
        />
      ) : null}

      <SiteFooter />
    </div>
  );
}
