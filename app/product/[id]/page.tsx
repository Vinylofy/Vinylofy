import { notFound } from "next/navigation";
import { PriceHistoryCard } from "@/components/product/price-history-card";
import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import {
  formatEuro,
  getProductDetail,
  getProductPriceHistory,
} from "@/lib/vinylofy-data";

type ProductPageProps = {
  params: Promise<{
    id: string;
  }>;
};

export default async function ProductDetailPage({ params }: ProductPageProps) {
  const { id } = await params;

  const [product, priceHistory] = await Promise.all([
    getProductDetail(id),
    getProductPriceHistory(id),
  ]);

  if (!product) {
    notFound();
  }

  const coverSrc = product.coverUrl ?? "/placeholders/vinylofy-cover-placeholder.png";
  const hasRealCover = Boolean(product.coverUrl);

  return (
    <div className="min-h-screen bg-[#f8f7f4] text-neutral-900">
      <SiteHeader />

      <main className="mx-auto max-w-7xl px-6 py-8 md:py-10">
        <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_380px]">
          <section className="space-y-6">
            <div className="rounded-[28px] border border-neutral-200 bg-white p-5 shadow-sm md:p-6">
              <div className="grid gap-5 md:grid-cols-[180px_minmax(0,1fr)] md:gap-6">
                <div className="flex items-start justify-center md:justify-start">
                  <div className="flex h-[180px] w-[180px] items-center justify-center overflow-hidden rounded-[24px] bg-neutral-50 shadow-inner">
                    <img
                      src={coverSrc}
                      alt={`${product.artist} - ${product.title}`}
                      className={
                        hasRealCover
                          ? "h-[180px] w-[180px] object-cover"
                          : "h-[146px] w-[146px] object-contain"
                      }
                    />
                  </div>
                </div>

                <div className="min-w-0">
                  <p className="text-sm font-medium text-orange-600">{product.artist}</p>
                  <h1 className="mt-2 text-3xl font-semibold leading-tight tracking-tight text-neutral-950 md:text-4xl">
                    {product.title}
                  </h1>

                  <div className="mt-3 flex flex-wrap gap-2 text-sm text-neutral-600">
                    {product.formatLabel ? (
                      <span className="rounded-full bg-neutral-100 px-3 py-1">
                        {product.formatLabel}
                      </span>
                    ) : null}
                    {product.ean ? (
                      <span className="rounded-full bg-neutral-100 px-3 py-1">
                        EAN {product.ean}
                      </span>
                    ) : null}
                    {product.freshnessLabel ? (
                      <span className="rounded-full bg-orange-50 px-3 py-1 text-orange-700">
                        {product.freshnessLabel}
                      </span>
                    ) : null}
                  </div>

                  <div className="mt-6 grid gap-3 md:grid-cols-3">
                    <div className="rounded-3xl border border-[rgba(234,88,12,0.22)] bg-[#fffaf6] px-4 py-4">
                      <p className="text-sm text-neutral-600">Laagste actuele prijs</p>
                      <p className="mt-2 text-3xl font-semibold tracking-tight text-neutral-950">
                        {formatEuro(product.lowestPrice)}
                      </p>
                    </div>

                    <div className="rounded-3xl border border-[rgba(234,88,12,0.22)] bg-[#fffaf6] px-4 py-4">
                      <p className="text-sm text-neutral-600">Nu gevonden bij</p>
                      <p className="mt-2 text-3xl font-semibold tracking-tight text-neutral-950">
                        {product.freshShopCount} winkels
                      </p>
                    </div>

                    <div className="rounded-3xl border border-[rgba(234,88,12,0.22)] bg-[#fffaf6] px-4 py-4">
                      <p className="text-sm text-neutral-600">Totaal bekende shops</p>
                      <p className="mt-2 text-3xl font-semibold tracking-tight text-neutral-950">
                        {product.totalShopCount}
                      </p>
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <section className="rounded-[28px] border border-neutral-200 bg-white p-5 shadow-sm md:p-6">
              <div className="flex items-end justify-between gap-3 border-b border-neutral-200 pb-4">
                <div>
                  <h2 className="text-2xl font-semibold tracking-tight text-neutral-950">
                    Aanbiedingen
                  </h2>
                  <p className="mt-1 text-sm text-neutral-500">
                    Actuele prijzen van winkels die recent zijn gecontroleerd.
                  </p>
                </div>
              </div>

              {product.shops.length === 0 ? (
                <div className="py-8 text-sm text-neutral-500">
                  Nog geen actuele shopprijzen beschikbaar voor dit product.
                </div>
              ) : (
                <div className="mt-5 divide-y divide-neutral-100">
                  {product.shops.map((shop) => (
                    <div
                      key={`${shop.name}-${shop.productUrl}`}
                      className="grid gap-3 py-4 md:grid-cols-[minmax(0,1fr)_120px_160px] md:items-center"
                    >
                      <div className="min-w-0">
                        <p className="truncate text-base font-medium text-neutral-950">{shop.name}</p>
                        <p className="truncate text-sm text-neutral-500">{shop.domain}</p>
                      </div>

                      <p className="text-left text-xl font-semibold tracking-tight text-neutral-950 md:text-right">
                        {formatEuro(shop.price)}
                      </p>

                      <div className="md:text-right">
                        <a
                          href={shop.productUrl}
                          target="_blank"
                          rel="noreferrer"
                          className="inline-flex items-center justify-center rounded-full bg-orange-500 px-4 py-2 text-sm font-medium text-white transition hover:bg-orange-600"
                        >
                          Ga naar shop
                        </a>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </section>

            <PriceHistoryCard currentPrice={product.lowestPrice} points={priceHistory} />
          </section>

          <aside className="space-y-6">
            <div className="rounded-[28px] border border-neutral-200 bg-white p-5 shadow-sm md:p-6">
              <h2 className="text-xl font-semibold tracking-tight text-neutral-950">Waarom deze grafiek?</h2>
              <div className="mt-4 space-y-3 text-sm leading-6 text-neutral-600">
                <p>
                  Vinylofy toont hier bewust alleen de laagste waargenomen dagprijs. Daardoor blijft
                  de pagina rustig en zie je in één oogopslag of dit een goed koopmoment is.
                </p>
                <p>
                  Er worden geen drukke lijnen per winkel getoond. De actuele aanbiedingen blijven
                  leidend; de grafiek is ondersteunende context.
                </p>
              </div>
            </div>

            <div className="rounded-[28px] border border-neutral-200 bg-white p-5 shadow-sm md:p-6">
              <h2 className="text-xl font-semibold tracking-tight text-neutral-950">Databron</h2>
              <div className="mt-4 space-y-3 text-sm leading-6 text-neutral-600">
                <p>
                  De importers schrijven al prijswaarnemingen weg naar de historische tabel. Voor de
                  grafiek wordt daar per kalenderdag één laagste prijs uit afgeleid.
                </p>
                <p>
                  Dat maakt de grafiek snel, consistent en eenvoudig uit te leggen aan gebruikers.
                </p>
              </div>
            </div>
          </aside>
        </div>
      </main>

      <SiteFooter />
    </div>
  );
}
