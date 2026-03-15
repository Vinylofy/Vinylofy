import { HeroSearch } from "@/components/home/hero-search";
import { NewReleasesGrid } from "@/components/home/new-releases-grid";
import { TopVinylList } from "@/components/home/top-vinyl-list";
import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";

export default function HomePage() {
  return (
    <div className="min-h-screen bg-white text-neutral-900">
      <SiteHeader />

      <main>
        <HeroSearch />
        <TopVinylList />
        <NewReleasesGrid />
      </main>

      <SiteFooter />
    </div>
  );
}