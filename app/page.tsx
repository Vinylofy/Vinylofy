import { HeroSearch } from "@/components/home/hero-search";
import { SiteFooter } from "@/components/site-footer";
import { getActiveMarkets, redirectInactiveMarket, resolveMarket } from "@/lib/markets";

export default async function HomePage({ searchParams }: { searchParams: Promise<{ market?: string }> }) {
  const requestedMarket = (await searchParams).market;
  const market = await resolveMarket(requestedMarket);
  redirectInactiveMarket(requestedMarket, market, "/");
  const markets = await getActiveMarkets();
  return (
    <>
      <main>
        <HeroSearch marketCode={market.country_code} markets={markets} />
      </main>
      <SiteFooter marketCode={market.country_code} />
    </>
  );
}
