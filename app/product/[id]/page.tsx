import { notFound } from "next/navigation";
import { PriceHistoryCard } from "@/components/product/price-history-card";
import { ProductOffersCard } from "@/components/product/product-offers-card";
import { ProductSummaryCard } from "@/components/product/product-summary-card";
import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import { SearchControls } from "@/components/search/search-controls";
import { CoverQueueBeacon } from "@/components/cover-queue-beacon";
import { getProductDetail, getProductPriceHistory, type ProductPriceHistoryPoint } from "@/lib/vinylofy-data";

type ProductPageProps = {
  params: Promise<{ id?: string }> | { id?: string };
};


function getTodayIsoDay(): string {
  return new Date().toISOString().slice(0, 10);
}

function mergeCurrentPriceIntoHistory(
  points: ProductPriceHistoryPoint[],
  product: {
    lowestPrice: number | null;
    freshShopCount: number;
    lastSeenAt: string | null;
  },
): ProductPriceHistoryPoint[] {
  if (product.lowestPrice === null) return points;

  const today = getTodayIsoDay();
  const todayPoint: ProductPriceHistoryPoint = {
    day: today,
    price: product.lowestPrice,
    shopCount: product.freshShopCount,
    lastCapturedAt: product.lastSeenAt,
  };

  const withoutToday = points.filter((point) => point.day !== today);

  return [...withoutToday, todayPoint].sort((a, b) => a.day.localeCompare(b.day));
}

export default async function ProductDetailPage({ params }: ProductPageProps) {
  const resolvedParams = await Promise.resolve(params);
  const routeId = typeof resolvedParams?.id === "string" ? resolvedParams.id : "";

  const product = await getProductDetail(routeId);

  if (!product) {
    notFound();
  }

  const priceHistory = await getProductPriceHistory(product.id, 30);
  const todayIsoDay = new Date().toISOString().slice(0, 10);
  const chartPoints = mergeCurrentPriceIntoHistory(priceHistory, product);

  return (
    <div className="min-h-screen bg-[#f8f7f4] text-neutral-900">
      <SiteHeader searchSlot={<SearchControls initialQuery="" />} />

      <main className="mx-auto max-w-6xl px-4 py-6 md:px-6 md:py-8">
        <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_300px] xl:gap-5">
          <section className="space-y-4 md:space-y-5">
            <ProductSummaryCard product={product} />
            <ProductOffersCard offers={product.shops} />
            <PriceHistoryCard currentPrice={product.lowestPrice} points={chartPoints} asOfDay={todayIsoDay} />
          </section>
        </div>
      </main>

      <CoverQueueBeacon productIds={[product.id]} source="detail" priorityBump={1000} />

      <SiteFooter />
    </div>
  );
}
