import Link from "next/link";
import { CoverImage } from "@/components/cover-image";
import { buildGrooveHref, formatEntityType } from "@/lib/follow-the-groove/presentation";
import type { FtgCandidateView } from "@/lib/follow-the-groove/types";

export function GrooveCandidateCard({
  candidate,
  trailMbids,
}: {
  candidate: FtgCandidateView;
  trailMbids: string[];
}) {
  return (
    <article className="flex h-full flex-col rounded-3xl border border-neutral-200 bg-white p-4 shadow-sm md:p-5">
      <div className="flex gap-4 md:flex-col">
        <div className="flex h-24 w-24 shrink-0 items-center justify-center overflow-hidden rounded-2xl bg-neutral-50 md:aspect-square md:h-auto md:w-full">
          <CoverImage
            src={candidate.representativeCover.src}
            alt={candidate.representativeCover.alt}
            className="h-full w-full object-cover data-[cover-fallback=true]:h-[82%] data-[cover-fallback=true]:w-[82%] data-[cover-fallback=true]:object-contain"
          />
        </div>
        <div className="min-w-0 flex-1">
          <p className="text-xs font-medium uppercase tracking-[0.12em] text-neutral-500">
            {formatEntityType(candidate.entityType)}
          </p>
          <h3 className="mt-1 text-lg font-semibold leading-tight tracking-tight text-neutral-950">
            {candidate.name}
          </h3>
          <p className="mt-2 text-sm leading-6 text-neutral-600">{candidate.reasonLabel}</p>
          {candidate.productCount > 0 ? (
            <p className="mt-2 text-xs text-neutral-500">
              {candidate.productCount === 1
                ? "1 titel bij Vinylofy"
                : `${candidate.productCount} titels bij Vinylofy`}
            </p>
          ) : null}
        </div>
      </div>

      <Link
        href={buildGrooveHref(trailMbids, candidate.mbid)}
        prefetch={false}
        className="mt-5 inline-flex min-h-11 items-center justify-center rounded-full border border-neutral-300 px-4 py-2 text-sm font-medium text-neutral-800 transition hover:border-orange-300 hover:bg-orange-50 hover:text-orange-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-orange-300 focus-visible:ring-offset-2"
      >
        Volg de groove →
      </Link>
    </article>
  );
}
