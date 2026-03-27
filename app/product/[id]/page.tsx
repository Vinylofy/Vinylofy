import type { ReactNode } from "react";
import { notFound } from "next/navigation";
import { PriceHistoryCard } from "@/components/product/price-history-card";
import { ProductOffersCard } from "@/components/product/product-offers-card";
import { ProductSummaryCard } from "@/components/product/product-summary-card";
import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import { getProductDetail, getProductPriceHistory } from "@/lib/vinylofy-data";

type ProductPageProps = {
  params: Promise<{ id?: string }> | { id?: string };
};

function InsightCard({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="rounded-xl border border-[rgba(230,126,34,0.16)] bg-white p-4 shadow-sm md:p-5">
      <h2 className="text-base font-semibold tracking-tight text-[#3f2616]">{title}</h2>
      <div className="mt-3 space-y-2 text-sm leading-6 text-[#7d6b5d]">{children}</div>
    </div>
  );
}

export default async function ProductDetailPage({ params }: ProductPageProps) {
  const resolvedParams = await Promise.resolve(params);
  const routeId = typeof resolvedParams?.id === "string" ? resolvedParams.id : "";

  const product = await getProductDetail(routeId);
  const priceHistory = product ? await getProductPriceHistory(product.id, 10) : [];

  if (!product) {
    notFound();
  }

  return (
    <div className="min-h-screen bg-[#f8f7f4] text-neutral-900">
      <SiteHeader />

      <main className="mx-auto max-w-6xl px-4 py-6 md:px-6 md:py-8">
        <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_300px] xl:gap-5">
          <section className="space-y-4 md:space-y-5">
            <ProductSummaryCard product={product} />
            <ProductOffersCard offers={product.shops} />
            <PriceHistoryCard currentPrice={product.lowestPrice} points={priceHistory} />
          </section>

          <aside className="space-y-4 xl:sticky xl:top-24 xl:self-start">
            <InsightCard title="Waarom deze grafiek?">
              <p>
                Vinylofy toont hier tijdelijk alleen de laagste waargenomen dagprijs van de laatste 10 dagen.
                Daardoor blijft de pagina rustig en kunnen we de prijsgrafiek eerst stabiel valideren.
              </p>
              <p>
                De actuele aanbiedingen blijven leidend. De grafiek is ondersteunende context, niet het
                hoofdonderdeel van de pagina.
              </p>
            </InsightCard>

            <InsightCard title="Hoe verzamelen we dit?">
              <p>
                De importers schrijven prijswaarnemingen al historisch weg. Voor deze kaart gebruiken we nu
                bewust alleen de laatste 10 dagen en worden die metingen per kalenderdag samengevat tot één
                laagste dagprijs per product.
              </p>
              <p>
                Zo blijft de historie uitlegbaar, snel en consistent met de simpele uitstraling van Vinylofy.
              </p>
            </InsightCard>
          </aside>
        </div>
      </main>

      <SiteFooter />
    </div>
  );
}
