import { notFound } from "next/navigation";
import { GrooveArtistHero } from "@/components/follow-the-groove/groove-artist-hero";
import { GrooveCandidateCard } from "@/components/follow-the-groove/groove-candidate-card";
import { GrooveEmptyState } from "@/components/follow-the-groove/groove-empty-state";
import { GrooveTrail } from "@/components/follow-the-groove/groove-trail";
import { SearchControls } from "@/components/search/search-controls";
import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import { getFollowTheGroovePage } from "@/lib/follow-the-groove/data";
import { isValidTrail } from "@/lib/follow-the-groove/presentation";

type FollowTheGroovePageProps = {
  params: Promise<{ trail?: string[] }>;
};

export default async function FollowTheGroovePage({ params }: FollowTheGroovePageProps) {
  const { trail = [] } = await params;
  if (!isValidTrail(trail)) notFound();

  const data = await getFollowTheGroovePage({
    trailMbids: trail,
    mode: "trail",
    limit: 5,
  });
  if (!data) notFound();

  return (
    <div className="min-h-screen bg-[#f8f7f4] text-neutral-900">
      <SiteHeader searchSlot={<SearchControls initialQuery="" />} />
      <main className="mx-auto max-w-7xl px-4 py-6 md:px-6 md:py-10">
        <div className="grid gap-8 lg:grid-cols-[220px_minmax(0,1fr)] lg:gap-10">
          <aside className="space-y-5 lg:sticky lg:top-28 lg:self-start">
            <div>
              <p className="text-xs font-medium uppercase tracking-[0.16em] text-orange-600">Follow the Groove</p>
              <p className="mt-2 text-sm leading-6 text-neutral-600">Ontdek muziek via echte connecties.</p>
            </div>
            <div className="space-y-3">
              <h2 className="text-sm font-semibold text-neutral-950">Jouw groove</h2>
              <GrooveTrail trail={data.trail} />
            </div>
          </aside>

          <div className="min-w-0 space-y-6 md:space-y-8">
            <GrooveArtistHero artist={data.artist} />

          {data.candidates.length === 0 ? (
            <GrooveEmptyState />
          ) : (
            <section aria-labelledby="groove-next-heading" className="space-y-4">
              <div>
                <h2
                  id="groove-next-heading"
                  className="mt-1 text-2xl font-semibold tracking-tight text-neutral-950"
                >
                  Waar ga je verder?
                </h2>
                <p className="mt-2 text-sm text-neutral-600">Connecties geselecteerd op feitelijke en muzikale relaties.</p>
              </div>
              <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-5">
                {data.candidates.map((candidate) => (
                  <GrooveCandidateCard
                    key={candidate.id}
                    candidate={candidate}
                    trailMbids={trail}
                  />
                ))}
              </div>
            </section>
          )}
          </div>
        </div>
      </main>
      <SiteFooter />
    </div>
  );
}
