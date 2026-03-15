import { GlobalSearchBar } from "@/components/global-search-bar";
import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";

type SearchPageProps = {
  searchParams: Promise<{
    q?: string;
  }>;
};

const mockResults = [
  {
    artist: "Fleetwood Mac",
    title: "Rumours (LP)",
    price: "€19,95",
    shops: [
      { name: "HHV", price: "€19,95" },
      { name: "Juno", price: "€21,50" },
      { name: "Amazon", price: "€22,90" },
      { name: "Bol", price: "€23,10" },
      { name: "Platomania", price: "€23,95" },
    ],
    totalShops: 8,
    freshness: "vandaag gecontroleerd",
  },
  {
    artist: "Fleetwood Mac",
    title: "Rumours (Deluxe Edition)",
    price: "€29,95",
    shops: [
      { name: "HHV", price: "€29,95" },
      { name: "Juno", price: "€31,50" },
      { name: "Bol", price: "€33,10" },
    ],
    totalShops: 4,
    freshness: "1 dag oud",
  },
];

export default async function SearchPage({ searchParams }: SearchPageProps) {
  const params = await searchParams;
  const query = params.q?.trim() || "";

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
            Gesorteerd op laagste prijs
          </p>
        </div>

        <div className="space-y-4">
          {mockResults.map((item) => (
            <article
              key={`${item.artist}-${item.title}`}
              className="rounded-2xl border border-neutral-200 bg-white p-4 md:p-6"
            >
              <div className="flex flex-col gap-5 md:flex-row">
                <div className="h-32 w-32 shrink-0 rounded-xl bg-neutral-100" />

                <div className="min-w-0 flex-1">
                  <p className="text-lg font-semibold">{item.artist}</p>
                  <p className="text-neutral-600">{item.title}</p>

                  <div className="mt-4 text-2xl font-semibold tracking-tight text-orange-600">
                    Vanaf {item.price}
                  </div>

                  <div className="mt-4 space-y-2">
                    {item.shops.map((shop) => (
                      <div
                        key={`${item.title}-${shop.name}`}
                        className="flex items-center justify-between rounded-lg bg-neutral-50 px-3 py-2 text-sm"
                      >
                        <span className="font-medium">{shop.name}</span>
                        <span className="font-semibold">{shop.price}</span>
                      </div>
                    ))}
                  </div>

                  <div className="mt-4 flex flex-col gap-3 text-sm text-neutral-500 md:flex-row md:items-center md:justify-between">
                    <span>gevonden in {item.totalShops} winkels</span>
                    <span>{item.freshness}</span>
                  </div>

                  <div className="mt-5">
                    <a
                      href="#"
                      className="inline-flex rounded-full bg-orange-600 px-5 py-2 text-sm font-medium text-white transition hover:bg-orange-700"
                    >
                      ga naar shop
                    </a>
                  </div>
                </div>
              </div>
            </article>
          ))}
        </div>
      </main>

      <SiteFooter />
    </div>
  );
}