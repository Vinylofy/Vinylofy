import Link from "next/link";
import { notFound } from "next/navigation";
import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import { formatEuro, getProductDetail } from "@/lib/vinylofy-data";

type ProductDetailPageProps = {
  params: Promise<{
    id: string;
  }>;
};

export default async function ProductDetailPage({ params }: ProductDetailPageProps) {
  const { id } = await params;
  const product = await getProductDetail(id);

  if (!product) {
    notFound();
  }

  const cheapestOffer = product.shops[0] ?? null;
  const coverSrc = product.coverUrl ?? "/placeholders/vinylofy-cover-placeholder.png";
  const hasRealCover = Boolean(product.coverUrl);

  return (
    <div className="min-h-screen bg-[#f8f7f4] text-neutral-900">
      <SiteHeader />

      <main className="mx-auto max-w-6xl px-6 py-8 md:py-10">
        <div className="mb-6">
          <Link
            href="/search"
            className="text-sm font-medium text-neutral-500 transition hover:text-neutral-900"
          >
            ← Terug naar zoeken
          </Link>
        </div>

        <section className="rounded-3xl border border-neutral-200 bg-white p-6 shadow-sm md:p-8">
          <div className="grid gap-6 md:grid-cols-[220px_minmax(0,1fr)]">
            <div className="flex items-start">
              <div className="flex h-[220px] w-[220px] items-center justify-center overflow-hidden rounded-3xl bg-neutral-50">
                <img
                  src={coverSrc}
                  alt={`${product.artist} - ${product.title}`}
                  className={
                    hasRealCover
                      ? "h-[220px] w-[220px] object-cover"
                      : "h-[170px] w-[170px] object-contain"
                  }
                />
              </div>
            </div>

            <div className="min-w-0">
              <p className="text-base text-neutral-500">{product.artist}</p>

              <h1 className="mt-1 text-3xl font-semibold tracking-tight text-neutral-950 md:text-4xl">
                {product.title}
              </h1>

              <div className="mt-3 flex flex-wrap items-center gap-2 text-sm text-neutral-600">
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
                  <span className="rounded-full bg-neutral-100 px-3 py-1">
                    {product.freshnessLabel}
                  </span>
                ) : null}
              </div>

              <div className="mt-6 grid gap-4 sm:grid-cols-2">
                <div className="rounded-2xl border border-neutral-200 bg-neutral-50 p-4">
                  <p className="text-sm text-neutral-500">Laagste actuele prijs</p>
                  <p className="mt-1 text-2xl font-semibold text-neutral-950">
                    {formatEuro(product.lowestPrice)}
                  </p>
                </div>

                <div className="rounded-2xl border border-neutral-200 bg-neutral-50 p-4">
                  <p className="text-sm text-neutral-500">Actieve winkels</p>
                  <p className="mt-1 text-2xl font-semibold text-neutral-950">
                    {product.foundIn}
                  </p>
                </div>
              </div>

              {cheapestOffer ? (
                <div className="mt-6">
                  <a
                    href={cheapestOffer.productUrl}
                    target="_blank"
                    rel="noreferrer"
                    className="inline-flex items-center rounded-full bg-orange-500 px-6 py-3 text-base font-medium text-white transition hover:bg-orange-600"
                  >
                    Ga naar goedkoopste shop
                  </a>
                </div>
              ) : null}
            </div>
          </div>
        </section>

        <section className="mt-8 rounded-3xl border border-neutral-200 bg-white p-6 shadow-sm md:p-8">
          <div className="mb-5 flex items-center justify-between gap-4">
            <div>
              <h2 className="text-2xl font-semibold tracking-tight text-neutral-950">
                Actueel aanbod
              </h2>
              <p className="mt-1 text-sm text-neutral-500">
                Alle gevonden shops voor deze release
              </p>
            </div>
          </div>

          {product.shops.length === 0 ? (
            <div className="rounded-2xl border border-neutral-200 bg-neutral-50 p-5 text-sm text-neutral-500">
              Er zijn momenteel geen actieve aanbiedingen gevonden.
            </div>
          ) : (
            <div className="space-y-3">
              {product.shops.map((shop, index) => (
                <div
                  key={`${shop.name}-${shop.productUrl}`}
                  className="grid gap-3 rounded-2xl border border-neutral-200 p-4 md:grid-cols-[56px_minmax(0,1fr)_auto_auto] md:items-center"
                >
                  <div className="text-sm font-semibold text-neutral-400">#{index + 1}</div>

                  <div className="min-w-0">
                    <a
                      href={shop.productUrl}
                      target="_blank"
                      rel="noreferrer"
                      className="block truncate text-lg font-medium text-neutral-950 transition hover:text-orange-600"
                    >
                      {shop.name}
                    </a>
                    <p className="mt-1 text-sm text-neutral-500">{shop.domain}</p>
                  </div>

                  <a
                    href={shop.productUrl}
                    target="_blank"
                    rel="noreferrer"
                    className="text-lg font-semibold text-neutral-950 transition hover:text-orange-600"
                  >
                    {formatEuro(shop.price)}
                  </a>

                  <a
                    href={shop.productUrl}
                    target="_blank"
                    rel="noreferrer"
                    className="inline-flex items-center rounded-full border border-neutral-300 px-4 py-2 text-sm font-medium text-neutral-900 transition hover:border-orange-500 hover:text-orange-600"
                  >
                    Naar shop
                  </a>
                </div>
              ))}
            </div>
          )}
        </section>

        <section className="mt-8 rounded-3xl border border-dashed border-neutral-300 bg-white p-6 text-sm text-neutral-500 shadow-sm md:p-8">
          Prijshistorie volgt in de volgende stap van de productdetailpagina.
        </section>
      </main>

      <SiteFooter />
    </div>
  );
}