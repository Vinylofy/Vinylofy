import { formatEuro, type ProductDetail } from "@/lib/vinylofy-data";

type ProductSummaryCardProps = {
  product: ProductDetail;
};

export function ProductSummaryCard({ product }: ProductSummaryCardProps) {
  const coverSrc = product.coverUrl ?? "/placeholders/vinylofy-cover-placeholder.png";
  const hasRealCover = Boolean(product.coverUrl);

  return (
    <section className="rounded-[28px] border border-[rgba(230,126,34,0.18)] bg-white p-5 shadow-sm md:p-7">
      <div className="grid gap-6 md:grid-cols-[188px_minmax(0,1fr)] md:items-start">
        <div className="flex items-start justify-center md:justify-start">
          <div className="flex h-[188px] w-[188px] items-center justify-center overflow-hidden rounded-[24px] border border-[rgba(230,126,34,0.10)] bg-[#fffaf6] shadow-inner">
            <img
              src={coverSrc}
              alt={`${product.artist} - ${product.title}`}
              className={
                hasRealCover
                  ? "h-[188px] w-[188px] object-cover"
                  : "h-[150px] w-[150px] object-contain opacity-90"
              }
            />
          </div>
        </div>

        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <span className="rounded-full border border-[rgba(230,126,34,0.22)] bg-[#fff7f0] px-3 py-1 text-xs font-semibold uppercase tracking-[0.14em] text-[#b85c11]">
              Productdetail
            </span>
            {product.freshnessLabel ? (
              <span className="rounded-full border border-[rgba(230,126,34,0.16)] bg-white px-3 py-1 text-xs font-medium text-[#8a5a34]">
                {product.freshnessLabel}
              </span>
            ) : null}
          </div>

          <p className="mt-4 text-sm font-semibold uppercase tracking-[0.12em] text-[#c46817]">
            {product.artist}
          </p>
          <h1 className="mt-2 text-3xl font-semibold leading-tight tracking-tight text-[#3f2616] md:text-4xl">
            {product.title}
          </h1>

          <div className="mt-4 flex flex-wrap gap-2 text-sm text-[#6b5b4f]">
            {product.formatLabel ? (
              <span className="rounded-full border border-neutral-200 bg-neutral-50 px-3 py-1">
                {product.formatLabel}
              </span>
            ) : null}
            {product.ean ? (
              <span className="rounded-full border border-neutral-200 bg-neutral-50 px-3 py-1">
                EAN {product.ean}
              </span>
            ) : null}
          </div>

          <div className="mt-6 grid gap-3 md:grid-cols-3">
            <div className="rounded-3xl border border-[rgba(230,126,34,0.28)] bg-[#fffaf6] px-4 py-4">
              <p className="text-sm text-[#7d6b5d]">Laagste actuele prijs</p>
              <p className="mt-2 text-3xl font-semibold tracking-tight text-[#3f2616]">
                {formatEuro(product.lowestPrice)}
              </p>
            </div>

            <div className="rounded-3xl border border-[rgba(230,126,34,0.28)] bg-[#fffaf6] px-4 py-4">
              <p className="text-sm text-[#7d6b5d]">Nu gevonden bij</p>
              <p className="mt-2 text-3xl font-semibold tracking-tight text-[#3f2616]">
                {product.freshShopCount} winkels
              </p>
            </div>

            <div className="rounded-3xl border border-[rgba(230,126,34,0.28)] bg-[#fffaf6] px-4 py-4">
              <p className="text-sm text-[#7d6b5d]">Totaal bekende shops</p>
              <p className="mt-2 text-3xl font-semibold tracking-tight text-[#3f2616]">
                {product.totalShopCount}
              </p>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
