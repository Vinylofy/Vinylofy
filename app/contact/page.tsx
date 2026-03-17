import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";

export default function ContactPage() {
  return (
    <div className="min-h-screen bg-white text-neutral-900">
      <SiteHeader />

      <main className="mx-auto max-w-4xl px-6 py-12">
        <div className="rounded-3xl border border-neutral-200 bg-white p-8 md:p-10">
          <p className="text-sm font-medium uppercase tracking-[0.14em] text-orange-600">
            Contact
          </p>

          <h1 className="mt-4 text-3xl font-semibold tracking-tight md:text-4xl">
            Neem contact op
          </h1>

          <div className="mt-8 space-y-5 text-neutral-600">
            <p>
              Heb je een vraag, mis je een shop, of klopt een prijs volgens jou
              niet? Dan horen we dat graag.
            </p>

            <p>
              Voor nu kun je contact opnemen via het centrale contactadres van
              Vinylofy. Deze pagina kan later worden uitgebreid met een formulier
              of supportflow.
            </p>

            <p className="rounded-xl bg-neutral-50 px-4 py-3 text-sm text-neutral-700">
              contact@vinylofy.com
            </p>
          </div>
        </div>
      </main>

      <SiteFooter />
    </div>
  );
}