import type { ReactNode } from "react";
import { notFound } from "next/navigation";
import { PriceHistoryCard } from "@/components/product/price-history-card";
import { ProductOffersCard } from "@/components/product/product-offers-card";
import { ProductSummaryCard } from "@/components/product/product-summary-card";
import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import {
  getProductDetail,
  getProductPriceHistory,
} from "@/lib/vinylofy-data";

type ProductPageProps = {
  params: {
    id: string;
  };
};

function InsightCard({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="rounded-[28px] border border-[rgba(230,126,34,0.18)] bg-white p-5 shadow-sm md:p-6">
      <h2 className="text-xl font-semibold tracking-tight text-[#3f2616]">{title}</h2>
      <div className="mt-4 space-y-3 text-sm leading-6 text-[#7d6b5d]">{children}</div>
    </div>
  );
}

export default async function ProductDetailPage({ params }: ProductPageProps) {
  const { id } = params;

  const product = await getProductDetail(id);
  const priceHistory = product ? await getProductPriceHistory(product.id) : [];

  if (!product) {
    notFound();
  }

  return (
    <div className="min-h-screen bg-[#f8f7f4] text-neutral-900">
      <SiteHeader />

      <main className="mx-auto max-w-7xl px-6 py-8 md:py-10">
        <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_360px]">
          <section className="space-y-6">
            <ProductSummaryCard product={product} />
            <ProductOffersCard offers={product.shops} />
            <PriceHistoryCard currentPrice={product.lowestPrice} points={priceHistory} />
          </section>

          <aside className="space-y-6">
            <InsightCard title="Waarom deze grafiek?">
              <p>
                Vinylofy toont hier bewust alleen de laagste waargenomen dagprijs. Daardoor blijft
                de pagina rustig en zie je sneller of dit een logisch koopmoment is.
              </p>
              <p>
                De actuele aanbiedingen blijven leidend. De grafiek is ondersteunende context, niet
                het hoofdonderdeel van de pagina.
              </p>
            </InsightCard>

            <InsightCard title="Hoe verzamelen we dit?">
              <p>
                De importers schrijven prijswaarnemingen al historisch weg. Voor deze kaart worden
                die metingen per kalenderdag samengevat tot één laagste dagprijs per product.
              </p>
              <p>
                Zo blijft de historie uitlegbaar, snel en consistent met de simpele uitstraling van
                Vinylofy.
              </p>
            </InsightCard>
          </aside>
        </div>
      </main>

      <SiteFooter />
    </div>
  );
}
