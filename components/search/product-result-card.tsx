import { formatEuro, type SearchResultItem } from "@/lib/vinylofy-data";

type ProductResultCardProps = {
  item: SearchResultItem;
};

export function ProductResultCard({ item }: ProductResultCardProps) {
  const primaryOffer = item.shops[0];

  return (
    <article className="rounded-2xl border border-neutral-200 bg-white p-4 md:p-6">
      <div className="flex flex-col gap-5 md:flex-row">
        {item.coverUrl ? (
          <img
            src={item.coverUrl}
            alt={`${item.artist} - ${item.title}`}
            className="h-32 w-32 shrink-0 rounded-xl bg-neutral-100 object-cover"
          />
        ) : (
          <div className="h-32 w-32 shrink-0 rounded-xl bg-neutral-100" />
        )}

        <div className="min-w-0 flex-1">
          <p className="text-lg font-semibold">{item.artist}</p>
          <p className="text-neutral-600">
            {item.title}
            {item.formatLabel ? ` · ${item.formatLabel}` : ""}
          </p>

          <div className="mt-4 text-2xl font-semibold tracking-tight text-orange-600">
            Vanaf {formatEuro(item.lowestPrice)}
          </div>

          <div className="mt-4 space-y-2">
            {item.shops.map((shop) => (
              <div
                key={`${item.id}-${shop.domain}`}
                className="flex items-center justify-between rounded-lg bg-neutral-50 px-3 py-2 text-sm"
              >
                <span className="font-medium">{shop.name}</span>
                <span className="font-semibold">{formatEuro(shop.price)}</span>
              </div>
            ))}
          </div>

          <div className="mt-4 flex flex-col gap-3 text-sm text-neutral-500 md:flex-row md:items-center md:justify-between">
            <span>gevonden in {item.foundIn} winkels</span>
            <span>{item.freshnessLabel ?? "geen verse prijsdata"}</span>
          </div>

          {primaryOffer ? (
            <div className="mt-5">
              <a
                href={primaryOffer.productUrl}
                target="_blank"
                rel="noreferrer"
                className="inline-flex rounded-full bg-orange-600 px-5 py-2 text-sm font-medium text-white transition hover:bg-orange-700"
              >
                ga naar shop
              </a>
            </div>
          ) : null}
        </div>
      </div>
    </article>
  );
}