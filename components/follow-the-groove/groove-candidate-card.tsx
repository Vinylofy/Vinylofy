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
        <div className="min-w-0 flex-1 md:flex md:min-h-[154px] md:flex-col">
          <p className="min-h-4 text-xs font-medium uppercase tracking-[0.12em] text-neutral-500">
            {formatEntityType(candidate.entityType)}
          </p>
          <h3 className="mt-1 min-h-[2.75rem] text-lg font-semibold leading-tight tracking-tight text-neutral-950">
            {candidate.name}
          </h3>
          <p className="mt-2 min-h-[3rem] text-sm leading-6 text-neutral-600">{candidate.reasonLabel}</p>
          <div className="mt-2 min-h-5">
          {candidate.productCount > 0 ? (
            <p className="text-xs text-neutral-500">
              {candidate.productCount === 1
                ? "1 titel op Vinylofy"
                : `${candidate.productCount} titels op Vinylofy`}
            </p>
          ) : null}
          </div>
        </div>
      </div>

      <div className="mt-5 flex min-h-[76px] flex-col items-stretch justify-end gap-2">
        <Link
          href={buildGrooveHref(trailMbids, candidate.mbid)}
          prefetch={false}
          className="inline-flex min-h-11 items-center justify-center rounded-full border border-neutral-300 px-4 py-2 text-sm font-medium text-neutral-800 transition hover:border-orange-300 hover:bg-orange-50 hover:text-orange-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-orange-300 focus-visible:ring-offset-2"
        >
          Volg de groove →
        </Link>
        {candidate.productCount > 0 && candidate.searchHref ? (
          <Link href={candidate.searchHref} prefetch={false} className="text-center text-xs font-medium text-orange-700 underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-orange-300">
            Bekijk titels →
          </Link>
        ) : null}
      </div>
    </article>
  );
}
