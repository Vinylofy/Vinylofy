import Link from "next/link";
import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";

export default function ProductNotFoundPage() {
  return (
    <div className="min-h-screen bg-[#f8f7f4] text-neutral-900">
      <SiteHeader />

      <main className="mx-auto flex max-w-3xl px-6 py-16 md:py-24">
        <section className="w-full rounded-[32px] border border-[rgba(230,126,34,0.18)] bg-white p-8 text-center shadow-sm md:p-12">
          <div className="mx-auto w-fit rounded-full border border-[rgba(230,126,34,0.22)] bg-[#fff7f0] px-4 py-2 text-xs font-semibold uppercase tracking-[0.14em] text-[#c46817]">
            Product niet gevonden
          </div>

          <h1 className="mt-5 text-3xl font-semibold tracking-tight text-[#3f2616] md:text-4xl">
            Dit product is niet beschikbaar op Vinylofy
          </h1>

          <p className="mx-auto mt-4 max-w-2xl text-base leading-7 text-[#7d6b5d]">
            Mogelijk bestaat dit product nog niet in de database, is het nog niet gekoppeld aan een geldig productrecord of is het tijdelijk niet publiek beschikbaar.
          </p>

          <div className="mt-8 flex flex-col items-center justify-center gap-3 sm:flex-row">
            <Link
              href="/"
              className="inline-flex items-center justify-center rounded-full bg-[#e67e22] px-5 py-3 text-sm font-semibold text-white transition hover:bg-[#cf6e18]"
            >
              Terug naar home
            </Link>
            <Link
              href="/search"
              className="inline-flex items-center justify-center rounded-full border border-[rgba(230,126,34,0.22)] bg-white px-5 py-3 text-sm font-semibold text-[#3f2616] transition hover:bg-[#fffaf6]"
            >
              Naar zoeken
            </Link>
          </div>
        </section>
      </main>

      <SiteFooter />
    </div>
  );
}
