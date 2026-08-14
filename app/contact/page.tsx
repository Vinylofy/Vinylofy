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
              Heb je een algemene vraag over Vinylofy of mis je een winkel in de
              prijsvergelijking? Laat het gerust weten.
            </p>

            <p>
              Koop je regelmatig bij een platenzaak die nog niet in Vinylofy staat?
              Of wil je jouw eigen webshop onder de aandacht brengen? Suggesties
              voor nieuwe winkels zijn welkom.
            </p>

            <p>
              Neem contact op via{" "}
              <a href="mailto:info@vinylofy.com">info@vinylofy.com</a>.
            </p>
          </div>
        </div>
      </main>

      <SiteFooter />
    </div>
  );
}