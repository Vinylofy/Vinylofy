import type { Metadata } from "next";
import Link from "next/link";

import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";

export const metadata: Metadata = {
  title: "Topdeals",
};

const demoDeals = [
  {
    title: "Weekend deal",
    shop: "Demo shop",
    text: "Een tijdelijke dealmodule voor opvallende aanbiedingen en scherpe price-drops.",
    badge: "-18%",
  },
  {
    title: "Prijsfavoriet",
    shop: "Demo shop",
    text: "Hier kun je straks de beste deal-tips tonen op basis van prijsverschil of opvallende daling.",
    badge: "Hot",
  },
  {
    title: "Editor’s tip",
    shop: "Demo shop",
    text: "Gebruik deze dummy landingspagina om later echte dealregels, filters of een curatorselectie aan te hangen.",
    badge: "Tip",
  },
];

export default function TopDealsPage() {
  return (
    <main className="min-h-screen bg-neutral-50 text-neutral-950">
      <SiteHeader />

      <section className="mx-auto max-w-6xl px-6 pb-10 pt-8 md:pt-10">
        <div className="max-w-3xl">
          <div className="mb-3 text-xs font-semibold uppercase tracking-[0.18em] text-orange-600">
            Dummy landingpagina
          </div>
          <h1 className="text-4xl font-semibold tracking-tight text-neutral-950">
            TOPDEALS
          </h1>
          <p className="mt-3 text-base leading-7 text-neutral-600">
            Bekijk hier onze top-tips voor aanbiedingen. Deze pagina is bewust dummy opgezet zodat je nu al een actiematige homepage hebt, terwijl de echte deal-logica later kan volgen.
          </p>
        </div>

        <div className="mt-8 grid gap-4 md:grid-cols-3">
          {demoDeals.map((deal) => (
            <article key={deal.title} className="rounded-3xl border border-neutral-200 bg-white p-5 shadow-sm">
              <div className="mb-4 inline-flex rounded-full bg-orange-100 px-3 py-1 text-xs font-semibold uppercase tracking-[0.16em] text-orange-700">
                {deal.badge}
              </div>
              <h2 className="text-xl font-semibold tracking-tight text-neutral-950">{deal.title}</h2>
              <p className="mt-1 text-sm font-medium text-neutral-500">{deal.shop}</p>
              <p className="mt-4 text-sm leading-6 text-neutral-600">{deal.text}</p>
            </article>
          ))}
        </div>

        <div className="mt-10 rounded-3xl border border-dashed border-neutral-300 bg-white px-6 py-6 text-sm text-neutral-600">
          Straks kun je hier ook filters, deal-labels en directe links naar zoekresultaten opnemen. Voor nu is dit een nette dummy bestemming vanaf de homepage.
          <div className="mt-4">
            <Link href="/search" className="font-medium text-orange-600 hover:text-orange-700">
              Naar zoeken →
            </Link>
          </div>
        </div>
      </section>

      <SiteFooter />
    </main>
  );
}
