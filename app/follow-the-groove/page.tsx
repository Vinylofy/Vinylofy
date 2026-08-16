import Image from "next/image";
import { SearchAutocompleteForm } from "@/components/search/search-autocomplete-form";
import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";

export default function FollowTheGrooveStartPage() {
  return (
    <div className="min-h-screen bg-[#f8f7f4] text-neutral-900">
      <SiteHeader searchSlot={null} />
      <main className="mx-auto max-w-6xl px-6 py-8 md:py-12">
        <div className="overflow-hidden rounded-3xl border border-neutral-200 bg-white shadow-sm">
          <Image
            src="/follow-the-groove/FTG.png"
            alt="Follow the Groove visualisatie van een vinylplaat met muzikale routes naar verwante artiesten"
            width={1672}
            height={941}
            priority
            className="h-auto w-full"
          />
        </div>

        <section className="mx-auto mt-8 max-w-3xl rounded-3xl border border-neutral-200 bg-white p-6 shadow-sm md:mt-10 md:p-10">
          <h1 className="text-2xl font-semibold tracking-tight text-neutral-950 md:text-3xl">Kies je startartiest</h1>
          <p className="mt-3 text-sm leading-6 text-neutral-600">
            Volg de connecties. Ontdek nieuwe muziek. Follow the Groove laat je artiesten ontdekken die verbonden zijn door bandleden, samenwerkingen en muzikale connecties.
          </p>
          <div className="mt-8">
            <label htmlFor="follow-the-groove-artist" className="mb-2 block text-sm font-medium text-neutral-900">
              Zoek een artiest
            </label>
            <SearchAutocompleteForm
              initialValue=""
              placeholder="Zoek een artiest..."
              variant="global"
              suggestionMode="follow-the-groove"
              selectionOnly
              inputId="follow-the-groove-artist"
              noResultsLabel="Geen artiest gevonden"
            />
            <p className="mt-3 text-xs text-neutral-500">Bijv. Foo Fighters, Miles Davis, Radiohead, Aretha Franklin…</p>
          </div>
        </section>
      </main>
      <SiteFooter />
    </div>
  );
}
