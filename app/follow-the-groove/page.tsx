import { SearchAutocompleteForm } from "@/components/search/search-autocomplete-form";
import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";

export default function FollowTheGrooveStartPage() {
  return (
    <div className="min-h-screen bg-[#f8f7f4] text-neutral-900">
      <SiteHeader searchSlot={null} />
      <main className="mx-auto flex max-w-3xl flex-col px-6 py-16 md:py-24">
        <section className="rounded-3xl border border-neutral-200 bg-white p-6 shadow-sm md:p-10">
          <p className="text-xs font-medium uppercase tracking-[0.16em] text-orange-600">Follow the Groove</p>
          <h1 className="mt-3 text-3xl font-semibold tracking-tight text-neutral-950 md:text-4xl">Ontdek je volgende groove</h1>
          <p className="mt-3 max-w-xl text-sm leading-6 text-neutral-600">
            Ontdek muziek via echte connecties tussen artiesten. Kies een artiest en bouw je eigen muzikale route.
          </p>
          <div className="mt-8">
            <label htmlFor="follow-the-groove-artist" className="mb-2 block text-sm font-medium text-neutral-900">
              Kies een artiest
            </label>
            <SearchAutocompleteForm
              initialValue=""
              placeholder="Zoek op artiest"
              variant="global"
              suggestionMode="follow-the-groove"
              selectionOnly
              inputId="follow-the-groove-artist"
              noResultsLabel="Geen artiest gevonden"
            />
          </div>
        </section>
      </main>
      <SiteFooter />
    </div>
  );
}
