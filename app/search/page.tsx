import { GlobalSearchBar } from "@/components/global-search-bar";
import { ProductResultCard } from "@/components/search/product-result-card";
import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import { searchProducts } from "@/lib/vinylofy-data";

type SearchPageProps = {
  searchParams: Promise<{
    q?: string;
  }>;
};

export default async function SearchPage({ searchParams }: SearchPageProps) {
  const params = await searchParams;
  const query = params.q?.trim() || "";
  const results = query ? await searchProducts(query) : [];

  return (
    <div className="min-h-screen bg-white text-neutral-900">
      <SiteHeader />

      <main className="mx-auto max-w-6xl px-6 py-8">
        <div className="mb-8">
          <div className="max-w-3xl">
            <GlobalSearchBar defaultValue={query} compact />
          </div>
        </div>

        <div className="mb-8">
          <h1 className="text-2xl font-semibold tracking-tight md:text-3xl">
            Resultaten voor: {query ? `"${query}"` : '"..."'}
          </h1>
          <p className="mt-2 text-sm text-neutral-500">
            Gesorteerd op relevantie en laagste prijs
          </p>
        </div>

        {!query ? (
          <div className="rounded-2xl border border-neutral-200 bg-white p-6 text-sm text-neutral-500">
            Typ een artiest of albumtitel om te zoeken.
          </div>
        ) : results.length === 0 ? (
          <div className="rounded-2xl border border-neutral-200 bg-white p-6 text-sm text-neutral-500">
            <p>
              Geen resultaten gevonden voor <strong>{query}</strong>.
            </p>
            <p className="mt-2">
              Probeer een artiest, albumtitel of een kortere zoekterm.
            </p>
          </div>
        ) : (
          <div className="space-y-4">
            {results.map((item) => (
              <ProductResultCard key={item.id} item={item} />
            ))}
          </div>
        )}
      </main>

      <SiteFooter />
    </div>
  );
}