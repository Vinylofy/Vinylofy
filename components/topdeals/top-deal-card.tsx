import Link from "next/link";
import { formatEuro, type TopDealItem } from "@/lib/vinylofy-data";

type TopDealCardProps = {
  deal: TopDealItem;
  rank: number;
};

const COVER_PLACEHOLDER = "/placeholders/vinylofy-cover-placeholder-white2.png";

function formatOfferCount(count: number): string {
  return count === 1 ? "1 aanbieder" : `${count} aanbieders`;
}


export function TopDealCard({ deal, rank }: TopDealCardProps) {
  const coverSrc = deal.coverUrl ?? COVER_PLACEHOLDER;

  return (
    <Link
      href={`/product/${deal.id}`}
      className="group flex h-full flex-col overflow-hidden rounded-3xl border border-neutral-200 bg-white shadow-sm transition hover:-translate-y-0.5 hover:border-neutral-300 hover:shadow-md"
    >
      <div className="flex gap-4 p-4">
        <div className="relative flex h-28 w-28 shrink-0 items-center justify-center overflow-hidden rounded-2xl border border-neutral-100 bg-neutral-50">
          <img
            src={coverSrc}
            alt=""
            className="h-full w-full object-contain"
            loading={rank <= 6 ? "eager" : "lazy"}
            decoding="async"
            fetchPriority={rank <= 6 ? "high" : "low"}
          />
          <span className="absolute left-2 top-2 rounded-full bg-white/95 px-2 py-1 text-xs font-semibold text-neutral-900 shadow-sm">
            #{rank}
          </span>
        </div>

        <div className="min-w-0 flex-1">
          <p className="truncate text-xs font-semibold uppercase tracking-[0.18em] text-neutral-500">
            {deal.artist}
          </p>
          <h2 className="mt-1 line-clamp-2 text-base font-semibold leading-snug text-neutral-950">
            {deal.title}
          </h2>

          <div className="mt-2 flex flex-wrap gap-2 text-xs text-neutral-500">
            {deal.formatLabel ? <span>{deal.formatLabel}</span> : null}
            {deal.ean ? <span>EAN {deal.ean}</span> : null}
          </div>

          <p className="mt-4 text-lg font-semibold text-neutral-950">
            Vanaf {formatEuro(deal.lowestPrice)}
          </p>
          <p className="mt-1 text-sm font-medium text-emerald-700">
            Tot {formatEuro(deal.priceDifference)} voordeliger dan elders
          </p>
        </div>
      </div>

      <div className="mt-auto border-t border-neutral-100 px-4 py-4">
        <dl className="space-y-2 text-sm">
          <div className="flex items-center justify-between gap-3">
            <dt className="text-neutral-500">Laagste prijs</dt>
            <dd className="truncate text-right font-medium text-neutral-900">
              {deal.lowestOffer.name} · {formatEuro(deal.lowestOffer.price)}
            </dd>
          </div>
          <div className="flex items-center justify-between gap-3">
            <dt className="text-neutral-500">Hoogste gevonden</dt>
            <dd className="truncate text-right font-medium text-neutral-900">
              {deal.highestOffer.name} · {formatEuro(deal.highestOffer.price)}
            </dd>
          </div>
        </dl>

        <div className="mt-4 flex items-center justify-between gap-3 text-sm">
          <span className="text-neutral-500">
            Gevonden bij {formatOfferCount(deal.shopCount)}
          </span>
          <span className="font-semibold text-neutral-950 transition group-hover:underline">
            Bekijk prijzen
          </span>
        </div>
      </div>
    </Link>
  );
}
