import Link from "next/link";
import { buildGrooveHref } from "@/lib/follow-the-groove/presentation";
import { trailPrefix } from "@/lib/follow-the-groove/trail";
import type { FtgTrailItem } from "@/lib/follow-the-groove/types";

function TrailItems({ trail, vertical = false }: { trail: FtgTrailItem[]; vertical?: boolean }) {
  return (
    <ol className={vertical ? "relative space-y-1" : "flex min-w-max items-center gap-2 text-sm text-neutral-500"}>
      {trail.map((item, index) => {
        const active = index === trail.length - 1;
        return (
          <li key={`${item.mbid}-${index}`} className={vertical ? "relative flex items-stretch gap-3" : "flex items-center gap-2"}>
            {vertical ? (
              <span aria-hidden="true" className="relative flex w-3 shrink-0 justify-center">
                <span className={`absolute top-0 h-full w-px ${index === trail.length - 1 ? "bg-neutral-200" : "bg-orange-200"}`} />
                <span className={`relative mt-2 h-2.5 w-2.5 rounded-full border-2 ${active ? "border-orange-600 bg-orange-500" : "border-orange-300 bg-white"}`} />
              </span>
            ) : index > 0 ? <span aria-hidden="true">›</span> : null}
            <span className={vertical ? "flex min-w-0 flex-1 flex-col" : "flex min-w-0 flex-1 items-center gap-2"}>
              {active ? (
                <span aria-current="page" className={`min-w-0 font-semibold ${vertical ? "flex-1 rounded-xl bg-orange-50 px-3 py-2 text-orange-700" : "text-neutral-900"}`}>
                  {item.name}
                </span>
              ) : (
                <Link
                  href={buildGrooveHref(trailPrefix(trail, index).map((entry) => entry.mbid))}
                  prefetch={false}
                  className={vertical ? "min-w-0 flex-1 rounded-xl px-3 py-2 text-neutral-600 transition hover:bg-neutral-50 hover:text-neutral-950 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-orange-300" : "rounded-sm transition hover:text-neutral-900 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-orange-300"}
                >
                  {item.name}
                </Link>
              )}
              {item.explanation ? (
                <span className={vertical ? "self-end px-3 text-right text-xs leading-5 text-neutral-500" : "text-xs text-neutral-500"}>
                  {item.explanation}
                </span>
              ) : null}
            </span>
          </li>
        );
      })}
    </ol>
  );
}

export function GrooveTrail({ trail }: { trail: FtgTrailItem[] }) {
  return (
    <>
      <nav aria-label="Jouw groove" className="hidden lg:block">
        <TrailItems trail={trail} vertical />
      </nav>
      <nav aria-label="Gevolgde groove" className="overflow-x-auto pb-1 lg:hidden">
        <TrailItems trail={trail} />
      </nav>
    </>
  );
}
