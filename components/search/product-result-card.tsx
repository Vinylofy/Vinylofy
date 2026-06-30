import Link from "next/link";

import {
  getOfferDisplayPrice,
  getVisibleOfferSummary,
} from "@/lib/offer-summary";
import { getShopCountryCode } from "@/lib/shop-country";
import { formatEuro, type SearchResultItem } from "@/lib/vinylofy-data";

type ProductResultCardProps = {
  item: SearchResultItem;
};

function formatOfferCount(count: number): string {
  return count === 1 ? "1 aanbieder gevonden" : `${count} aanbieders gevonden`;
}

function formatCtaLabel(count: number): string {
  return count <= 1 ? "Bekijk aanbieding" : `Bekijk alle ${count} aanbiedingen`;
}

export function ProductResultCard({ item }: ProductResultCardProps) {
  const visibleOffers = getVisibleOfferSummary(item.shops, 3);
  const visibleShopCount = item.shops.length;
  const effectiveOfferCount = Math.max(
    item.totalShops ?? 0,
    item.foundIn ?? 0,
    visibleShopCount,
  );
  const offerCountLabel = formatOfferCount(effectiveOfferCount);
  const ctaLabel = formatCtaLabel(effectiveOfferCount);
  const coverSrc =
    item.coverUrl ?? "/placeholders/vinylofy-cover-placeholder-white2.png";
  const hasRealCover = Boolean(item.coverUrl);

  return (
    <article className="rounded-3xl border border-neutral-200 bg-white p-4 shadow-sm md:p-5">
      <div className="grid gap-3 md:grid-cols-[96px_minmax(0,1fr)] md:gap-4">
        <div className="flex items-start">
          <div className="flex h-[96px] w-[96px] items-center justify-center overflow-hidden rounded-2xl bg-neutral-50">
            <img
              src={coverSrc}
              alt={`${item.artist} - ${item.title}`}
              className={
                hasRealCover
                  ? "h-[96px] w-[96px] object-cover"
                  : "h-[82px] w-[82px] object-contain"
              }
            />
          </div>
        </div>

        <div className="min-w-0">
          <p className="text-xs text-neutral-500">{item.artist}</p>

          <h2 className="mt-1 text-lg font-semibold leading-tight tracking-tight text-neutral-950 md:text-[20px]">
            {item.title}
            {item.formatLabel ? ` · ${item.formatLabel}` : ""}
          </h2>

          <div className="mt-3 grid gap-x-4 gap-y-2 md:grid-cols-[minmax(0,1fr)_180px]">
            <div className="md:col-span-2">
              <p className="text-xs font-medium text-neutral-500">Beste opties</p>
            </div>

            {visibleOffers.map((shop) => {
              const countryCode = getShopCountryCode(shop);
              const displayPrice = getOfferDisplayPrice(shop);

              return (
                <div key={`${shop.name}-${shop.productUrl}`} className="contents">
                  <div className="min-w-0 truncate text-sm text-neutral-700">
                    <span>{shop.name}</span>
                    <span className="text-neutral-400"> · {countryCode}</span>
                  </div>

                  <div className="shrink-0 text-right text-sm font-medium text-neutral-950">
                    {formatEuro(displayPrice)}
                  </div>
                </div>
              );
            })}

            <div className="pt-2">
              <Link
                href={`/product/${item.id}`}
                className="inline-flex items-center rounded-full bg-orange-500/80 px-4 py-2 text-sm font-medium text-white transition hover:bg-orange-500"
              >
                {ctaLabel}
              </Link>
            </div>

            <div className="pt-2 text-xs text-neutral-500">
              <div className="space-y-0.5">
                {item.freshnessLabel ? <p>{item.freshnessLabel}</p> : null}
                <p>{offerCountLabel}</p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </article>
  );
}
