import { GlobalSearchBar } from "@/components/global-search-bar";
import { Logo } from "@/components/logo";

export function HeroSearch() {
  return (
    <section className="mx-auto flex max-w-5xl flex-col items-center px-6 py-16 text-center md:py-24">
      <div className="mb-8">
        <Logo size="lg" withTagline />
      </div>

      <h1 className="max-w-3xl text-3xl font-semibold tracking-tight md:text-5xl">
        Vind de beste prijs voor vinyl
      </h1>

      <p className="mt-4 max-w-2xl text-sm text-neutral-500 md:text-base">
        Vergelijk prijzen tussen winkels en vind snel de beste deal voor je
        vinyl.
      </p>

      <div className="mt-8 w-full max-w-2xl">
        <GlobalSearchBar />
      </div>

      <p className="mt-4 text-sm text-neutral-400">
        Bijvoorbeeld: Fleetwood Mac, Rumours of Kind of Blue
      </p>
    </section>
  );
}