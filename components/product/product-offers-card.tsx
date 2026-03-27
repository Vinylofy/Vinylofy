import { formatOfferDomain, formatRelativeFreshness } from "@/lib/product-page-format";
import { formatEuro, type SearchShopOffer } from "@/lib/vinylofy-data";

type ProductOffersCardProps = {
  offers: SearchShopOffer[];
};

export function ProductOffersCard({ offers }: ProductOffersCardProps) {
  return (
    <section className="rounded-xl border border-[rgba(230,126,34,0.16)] bg-white p-4 shadow-sm md:p-5">
      <div className="flex flex-col gap-2.5 border-b border-[rgba(63,38,22,0.08)] pb-3 md:flex-row md:items-end md:justify-between">
        <div>
          <h2 className="text-lg font-semibold tracking-tight text-[#3f2616]">Aanbiedingen</h2>
          <p className="mt-1 text-sm text-[#7d6b5d]">Actuele prijzen van winkels die recent zijn gecontroleerd.</p>
        </div>
        <div className="rounded-full border border-[rgba(230,126,34,0.18)] bg-[#fffaf6] px-2.5 py-1 text-xs font-medium text-[#8a5a34]">
          {offers.length} actuele {offers.length === 1 ? "winkel" : "winkels"}
        </div>
      </div>

      {offers.length === 0 ? (
        <div className="mt-4 rounded-xl border border-dashed border-[rgba(230,126,34,0.28)] bg-[#fffaf6] px-4 py-6 text-sm text-[#7d6b5d]">
          Nog geen actuele shopprijzen beschikbaar voor dit product.
        </div>
      ) : (
        <div className="mt-4 space-y-2.5">
          {offers.map((offer, index) => {
            const freshness = formatRelativeFreshness(offer.lastSeenAt);

            return (
              <div
                key={`${offer.name}-${offer.productUrl}-${index}`}
                className="grid gap-3 rounded-xl border border-[rgba(63,38,22,0.08)] bg-[#fffdfb] px-3.5 py-3 transition hover:border-[rgba(230,126,34,0.28)] hover:bg-[#fffaf6] md:grid-cols-[minmax(0,1fr)_96px_128px] md:items-center"
              >
                <div className="min-w-0">
                  <p className="truncate text-sm font-semibold text-[#3f2616] md:text-[15px]">{offer.name}</p>
                  <div className="mt-1 flex flex-wrap items-center gap-1.5 text-xs text-[#7d6b5d] md:text-sm">
                    <span>{formatOfferDomain(offer.domain)}</span>
                    {freshness ? (
                      <>
                        <span aria-hidden="true">•</span>
                        <span>{freshness}</span>
                      </>
                    ) : null}
                  </div>
                </div>

                <p className="text-left text-lg font-semibold tracking-tight text-[#3f2616] md:text-right md:text-xl">
                  {formatEuro(offer.price)}
                </p>

                <div className="md:text-right">
                  <a
                    href={offer.productUrl}
                    target="_blank"
                    rel="noreferrer"
                    className="inline-flex items-center justify-center rounded-full bg-[#e67e22] px-3.5 py-2 text-xs font-semibold text-white transition hover:bg-[#cf6e18] md:text-sm"
                  >
                    Ga naar shop
                  </a>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </section>
  );
}
