import Link from "next/link";
import { CoverImage } from "@/components/cover-image";
import { buildGrooveHref, formatEntityType } from "@/lib/follow-the-groove/presentation";
import type { FtgCandidateView } from "@/lib/follow-the-groove/types";

export function GrooveSearchBlock({
  activeArtistMbid,
  candidates,
}: {
  activeArtistMbid: string;
  candidates: FtgCandidateView[];
}) {
  if (candidates.length === 0) return null;

  return (
    <section aria-labelledby="search-groove-heading" className="rounded-3xl border border-orange-100 bg-white p-4 shadow-sm md:p-5">
      <div className="space-y-1">
        <p className="text-xs font-medium uppercase tracking-[0.16em] text-orange-600">FOLLOW THE GROOVE</p>
        <h2 id="search-groove-heading" className="text-xl font-semibold tracking-tight text-neutral-950">
          Ontdek jouw volgende muzikale bestemming
        </h2>
        <p className="text-sm text-neutral-600">Verken artiesten die muzikaal verbonden zijn met wat je zoekt.</p>
      </div>
      <div className="mt-4 grid gap-3 md:grid-cols-2 lg:grid-cols-3">
        {candidates.map((candidate) => (
          <article key={candidate.id} className="flex min-w-0 gap-3 rounded-2xl border border-neutral-200 bg-[#f8f7f4] p-3">
            <div className="flex h-16 w-16 shrink-0 items-center justify-center overflow-hidden rounded-xl bg-neutral-50">
              <CoverImage
                src={candidate.representativeCover.src}
                alt={candidate.representativeCover.alt}
                className="h-full w-full object-cover data-[cover-fallback=true]:h-[82%] data-[cover-fallback=true]:w-[82%] data-[cover-fallback=true]:object-contain"
              />
            </div>
            <div className="flex min-w-0 flex-1 flex-col">
              <p className="text-[0.68rem] font-medium uppercase tracking-[0.12em] text-neutral-500">{formatEntityType(candidate.entityType)}</p>
              <h3 className="mt-0.5 truncate text-sm font-semibold text-neutral-950">{candidate.name}</h3>
              <p className="mt-1 min-h-5 text-xs text-neutral-600">{candidate.reasonLabel}</p>
              <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1">
                <Link
                  href={buildGrooveHref([activeArtistMbid], candidate.mbid)}
                  prefetch={false}
                  className="text-xs font-medium text-neutral-800 underline-offset-4 hover:text-orange-700 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-orange-300"
                >
                  Volg de groove →
                </Link>
                {candidate.productCount > 0 && candidate.searchHref ? (
                  <Link href={candidate.searchHref} prefetch={false} className="text-xs font-medium text-orange-700 underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-orange-300">
                    {candidate.productCount === 1 ? "1 titel op Vinylofy →" : `${candidate.productCount} titels op Vinylofy →`}
                  </Link>
                ) : null}
              </div>
            </div>
          </article>
        ))}
      </div>
    </section>
  );
}
