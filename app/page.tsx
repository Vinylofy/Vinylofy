import { HeroSearch } from "@/components/home/hero-search";
import { NewReleasesGrid } from "@/components/home/new-releases-grid";
import { TopVinylList } from "@/components/home/top-vinyl-list";
import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import { getHomePageData } from "@/lib/vinylofy-data";

export default async function HomePage() {
  const { top25, newReleases } = await getHomePageData();

  return (
    <div className="min-h-screen bg-white text-neutral-900">
      <SiteHeader />

      <main>
        <HeroSearch />
        <TopVinylList items={top25} />
        <NewReleasesGrid items={newReleases} />
      </main>

      <SiteFooter />
    </div>
  );
}