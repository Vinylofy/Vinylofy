import Link from "next/link";
import { formatEuro, type SearchResultItem } from "@/lib/vinylofy-data";

type ProductResultCardProps = {
  item: SearchResultItem;
};

export function ProductResultCard({ item }: ProductResultCardProps) {
  const visibleShopCount = item.shops.length;
  const effectiveShopCount = Math.max(item.foundIn ?? 0, visibleShopCount);
  const shouldShowShopCount = effectiveShopCount > 3;
  const hasMeta = Boolean(item.freshnessLabel) || shouldShowShopCount;
  const coverSrc = item.coverUrl ?? "/placeholders/vinylofy-cover-placeholder.png";
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

          <div className="mt-3 grid gap-x-4 gap-y-2 md:grid-cols-[minmax(0,1fr)_140px]">
            {item.shops.map((shop) => (
              <div key={`${shop.name}-${shop.productUrl}`} className="contents">
                <div className="min-w-0">
                  <a
                    href={shop.productUrl}
                    target="_blank"
                    rel="noreferrer"
                    className="block truncate text-sm text-neutral-700 transition hover:text-orange-600 hover:underline"
                  >
                    {shop.name}
                  </a>
                  {shop.availability === "unknown" ? (
                    <p className="mt-0.5 text-[11px] text-neutral-500">beschikbaarheid onbekend</p>
                  ) : null}
                </div>

                <a
                  href={shop.productUrl}
                  target="_blank"
                  rel="noreferrer"
                  className="shrink-0 text-right text-sm font-medium text-neutral-950 transition hover:text-orange-600 hover:underline"
                >
                  {formatEuro(shop.price)}
                </a>
              </div>
            ))}

            <div className="pt-2">
              <Link
                href={`/product/${item.id}`}
                className="inline-flex items-center rounded-full bg-orange-500/80 px-4 py-2 text-sm font-medium text-white transition hover:bg-orange-500"
              >
                Bekijk details
              </Link>
            </div>

            <div className="pt-2 text-xs text-neutral-500">
              {hasMeta ? (
                <div className="space-y-0.5">
                  {item.freshnessLabel ? <p>{item.freshnessLabel}</p> : null}
                  {shouldShowShopCount ? <p>gevonden in {effectiveShopCount} winkels</p> : null}
                </div>
              ) : null}
            </div>
          </div>
        </div>
      </div>
    </article>
  );
}