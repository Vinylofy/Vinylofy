import { CoverImage } from "@/components/cover-image";
import { formatEuro, type ProductDetail } from "@/lib/vinylofy-data";

type ProductSummaryCardProps = {
  product: ProductDetail;
};

export function ProductSummaryCard({ product }: ProductSummaryCardProps) {

  return (
    <section className="rounded-xl border border-[rgba(230,126,34,0.16)] bg-white p-4 shadow-sm md:p-5">
      <div className="grid gap-4 md:grid-cols-[132px_minmax(0,1fr)] md:items-start">
        <div className="flex items-start justify-center md:justify-start">
          <div className="flex h-[132px] w-[132px] items-center justify-center overflow-hidden rounded-xl border border-[rgba(230,126,34,0.10)] bg-[#fffaf6] shadow-inner">
            <CoverImage
              src={product.coverUrl}
              storagePath={product.coverStoragePath}
              alt={`${product.artist} - ${product.title}`}
              className="h-[132px] w-[132px] object-cover data-[cover-fallback=true]:h-[104px] data-[cover-fallback=true]:w-[104px] data-[cover-fallback=true]:object-contain data-[cover-fallback=true]:opacity-90"
            />
          </div>
        </div>

        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <span className="rounded-full border border-[rgba(230,126,34,0.22)] bg-[#fff7f0] px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.12em] text-[#b85c11]">
              Productdetail
            </span>
            {product.freshnessLabel ? (
              <span className="rounded-full border border-[rgba(230,126,34,0.16)] bg-white px-2.5 py-1 text-[11px] font-medium text-[#8a5a34]">
                {product.freshnessLabel}
              </span>
            ) : null}
          </div>

          <p className="mt-3 text-[11px] font-semibold uppercase tracking-[0.12em] text-[#c46817] md:text-xs">
            {product.artist}
          </p>
          <h1 className="mt-1.5 text-2xl font-semibold leading-tight tracking-tight text-[#3f2616] md:text-[2rem]">
            {product.title}
          </h1>

          <div className="mt-3 flex flex-wrap gap-2 text-xs text-[#6b5b4f] md:text-sm">
            {product.formatLabel ? (
              <span className="rounded-full border border-neutral-200 bg-neutral-50 px-2.5 py-1">
                {product.formatLabel}
              </span>
            ) : null}
            {product.ean ? (
              <span className="rounded-full border border-neutral-200 bg-neutral-50 px-2.5 py-1">
                EAN {product.ean}
              </span>
            ) : null}
          </div>

          <div className="mt-4 grid gap-2 md:grid-cols-2">
            <div className="rounded-xl border border-[rgba(230,126,34,0.24)] bg-[#fffaf6] px-3 py-3">
              <p className="text-xs uppercase tracking-[0.08em] text-[#8a7769]">Laagste actuele prijs</p>
              <p className="mt-1.5 text-xl font-semibold tracking-tight text-[#3f2616] md:text-2xl">
                {formatEuro(product.lowestPrice)}
              </p>
            </div>

            <div className="rounded-xl border border-[rgba(230,126,34,0.24)] bg-[#fffaf6] px-3 py-3">
              <p className="text-xs uppercase tracking-[0.08em] text-[#8a7769]">Aanbieders</p>
              <p className="mt-1.5 text-xl font-semibold tracking-tight text-[#3f2616] md:text-2xl">
                {product.freshShopCount} winkels
              </p>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
