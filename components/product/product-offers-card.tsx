import { formatOfferDomain, formatRelativeFreshness } from "@/lib/product-page-format";
import { formatEuro, type SearchShopOffer } from "@/lib/vinylofy-data";

type ProductOffersCardProps = {
  offers: SearchShopOffer[];
};

export function ProductOffersCard({ offers }: ProductOffersCardProps) {
  return (
    <section className="rounded-[28px] border border-[rgba(230,126,34,0.18)] bg-white p-5 shadow-sm md:p-6">
      <div className="flex flex-col gap-3 border-b border-[rgba(63,38,22,0.08)] pb-4 md:flex-row md:items-end md:justify-between">
        <div>
          <h2 className="text-2xl font-semibold tracking-tight text-[#3f2616]">Aanbiedingen</h2>
          <p className="mt-1 text-sm text-[#7d6b5d]">
            Actuele prijzen van winkels die recent zijn gecontroleerd.
          </p>
        </div>
        <div className="rounded-full border border-[rgba(230,126,34,0.18)] bg-[#fffaf6] px-3 py-1 text-sm font-medium text-[#8a5a34]">
          {offers.length} actuele {offers.length === 1 ? "winkel" : "winkels"}
        </div>
      </div>

      {offers.length === 0 ? (
        <div className="mt-5 rounded-3xl border border-dashed border-[rgba(230,126,34,0.28)] bg-[#fffaf6] px-5 py-8 text-sm text-[#7d6b5d]">
          Nog geen actuele shopprijzen beschikbaar voor dit product.
        </div>
      ) : (
        <div className="mt-5 space-y-3">
          {offers.map((offer, index) => {
            const freshness = formatRelativeFreshness(offer.lastSeenAt);

            return (
              <div
                key={`${offer.name}-${offer.productUrl}-${index}`}
                className="grid gap-4 rounded-[24px] border border-[rgba(63,38,22,0.08)] bg-[#fffdfb] px-4 py-4 transition hover:border-[rgba(230,126,34,0.28)] hover:bg-[#fffaf6] md:grid-cols-[minmax(0,1fr)_120px_170px] md:items-center"
              >
                <div className="min-w-0">
                  <p className="truncate text-base font-semibold text-[#3f2616]">{offer.name}</p>
                  <div className="mt-1 flex flex-wrap items-center gap-2 text-sm text-[#7d6b5d]">
                    <span>{formatOfferDomain(offer.domain)}</span>
                    {freshness ? (
                      <>
                        <span aria-hidden="true">•</span>
                        <span>{freshness}</span>
                      </>
                    ) : null}
                  </div>
                </div>

                <p className="text-left text-2xl font-semibold tracking-tight text-[#3f2616] md:text-right">
                  {formatEuro(offer.price)}
                </p>

                <div className="md:text-right">
                  <a
                    href={offer.productUrl}
                    target="_blank"
                    rel="noreferrer"
                    className="inline-flex items-center justify-center rounded-full bg-[#e67e22] px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-[#cf6e18]"
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
