import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";

export default function OverPage() {
  return (
    <div className="min-h-screen bg-white text-neutral-900">
      <SiteHeader />

      <main className="mx-auto max-w-4xl px-6 py-12">
        <div className="rounded-3xl border border-neutral-200 bg-white p-8 md:p-10">
          <p className="text-sm font-medium uppercase tracking-[0.14em] text-orange-600">
            Over Vinylofy
          </p>

          <h1 className="mt-4 text-3xl font-semibold tracking-tight md:text-4xl">
            Vinyl offers for you
          </h1>

          <div className="mt-8 space-y-5 text-neutral-600">
            <p>
              Vinylofy is een prijsvergelijker voor vinylplaten. Het platform
              helpt je om snel te zien waar een release het scherpst geprijsd
              is.
            </p>

            <p>
              De focus ligt op prijs, dealfinding en winkelvergelijking. Dus
              niet op communityfuncties, reviews of een uitgebreide
              muziekdatabase als kernproduct.
            </p>

            <p>
              In volgende fases groeit Vinylofy door naar prijshistorie,
              prijsbewegingen en signalen zoals prijsdalingen en laagste prijs
              in 30 dagen.
            </p>
          </div>
        </div>
      </main>

      <SiteFooter />
    </div>
  );
}