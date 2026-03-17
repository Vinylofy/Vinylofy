import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";

export default function PrivacyPage() {
  return (
    <div className="min-h-screen bg-white text-neutral-900">
      <SiteHeader />

      <main className="mx-auto max-w-4xl px-6 py-12">
        <div className="rounded-3xl border border-neutral-200 bg-white p-8 md:p-10">
          <p className="text-sm font-medium uppercase tracking-[0.14em] text-orange-600">
            Privacy
          </p>

          <h1 className="mt-4 text-3xl font-semibold tracking-tight md:text-4xl">
            Privacyverklaring
          </h1>

          <div className="mt-8 space-y-5 text-neutral-600">
            <p>
              Vinylofy verwerkt op dit moment alleen beperkte technische data
              die nodig is om de website goed te laten werken, zoals
              serververzoeken en basisanalyse van gebruik.
            </p>

            <p>
              Vinylofy verkoopt geen persoonlijke gegevens. Externe winkels waar
              je naartoe klikt, hanteren hun eigen privacy- en cookiebeleid.
            </p>

            <p>
              Deze pagina is een eerste basisversie en wordt later uitgebreid
              met een formele privacytekst zodra tracking, alerts of accounts
              worden toegevoegd.
            </p>
          </div>
        </div>
      </main>

      <SiteFooter />
    </div>
  );
}