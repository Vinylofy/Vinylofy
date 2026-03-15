import { GlobalSearchBar } from "@/components/global-search-bar";
import { Logo } from "@/components/logo";
import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";

const top25 = [
  { artist: "Fleetwood Mac", title: "Rumours", price: "€19,95" },
  { artist: "Nirvana", title: "Nevermind", price: "€18,99" },
  { artist: "Pink Floyd", title: "The Wall", price: "€21,50" },
  { artist: "Daft Punk", title: "Random Access Memories", price: "€24,95" },
  { artist: "Radiohead", title: "OK Computer", price: "€22,99" },
];

const newReleases = [
  { artist: "The Smile", title: "Cutouts" },
  { artist: "Fontaines D.C.", title: "Romance" },
  { artist: "Nick Cave", title: "Wild God" },
  { artist: "The Cure", title: "Songs of a Lost World" },
];

export default function HomePage() {
  return (
    <div className="min-h-screen bg-white text-neutral-900">
      <SiteHeader />

      <main>
        <section className="mx-auto flex max-w-5xl flex-col items-center px-6 py-16 text-center md:py-24">
          <div className="mb-8">
            <Logo size="lg" />
          </div>

          <h1 className="max-w-3xl text-3xl font-semibold tracking-tight md:text-5xl">
            Vind de beste prijs voor vinyl
          </h1>

          <p className="mt-4 text-sm text-neutral-500 md:text-base">
            Zoek op artiest, albumtitel of EAN
          </p>

          <div className="mt-8 w-full max-w-2xl">
            <GlobalSearchBar />
          </div>
        </section>

        <section className="mx-auto max-w-5xl px-6 py-10">
          <div className="mb-6">
            <h2 className="text-2xl font-semibold tracking-tight">
              Top 25 vinyl van dit moment
            </h2>
          </div>

          <div className="rounded-2xl border border-neutral-200 bg-white">
            <ol className="divide-y divide-neutral-200">
              {top25.map((item, index) => (
                <li
                  key={`${item.artist}-${item.title}`}
                  className="flex items-center justify-between gap-4 px-4 py-4 md:px-6"
                >
                  <div className="flex min-w-0 items-start gap-4">
                    <span className="w-6 shrink-0 text-sm font-medium text-neutral-400">
                      {index + 1}
                    </span>
                    <div className="min-w-0">
                      <p className="truncate font-medium">{item.artist}</p>
                      <p className="truncate text-sm text-neutral-500">
                        {item.title}
                      </p>
                    </div>
                  </div>

                  <div className="shrink-0 text-sm font-semibold text-orange-600">
                    vanaf {item.price}
                  </div>
                </li>
              ))}
            </ol>
          </div>
        </section>

        <section className="mx-auto max-w-5xl px-6 py-10">
          <div className="mb-6">
            <h2 className="text-2xl font-semibold tracking-tight">
              Nieuwe releases
            </h2>
          </div>

          <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
            {newReleases.map((item) => (
              <article
                key={`${item.artist}-${item.title}`}
                className="rounded-2xl border border-neutral-200 bg-white p-4"
              >
                <div className="aspect-square rounded-xl bg-neutral-100" />
                <div className="mt-3">
                  <p className="truncate font-medium">{item.artist}</p>
                  <p className="line-clamp-2 text-sm text-neutral-500">
                    {item.title}
                  </p>
                </div>
              </article>
            ))}
          </div>
        </section>
      </main>

      <SiteFooter />
    </div>
  );
}