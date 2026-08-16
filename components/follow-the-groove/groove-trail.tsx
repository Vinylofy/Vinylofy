import Link from "next/link";
import { buildGrooveHref } from "@/lib/follow-the-groove/presentation";
import type { FtgTrailItem } from "@/lib/follow-the-groove/types";

export function GrooveTrail({ trail }: { trail: FtgTrailItem[] }) {
  return (
    <nav aria-label="Gevolgde groove" className="overflow-x-auto pb-1">
      <ol className="flex min-w-max items-center gap-2 text-sm text-neutral-500">
        {trail.map((item, index) => {
          const active = index === trail.length - 1;
          return (
            <li key={`${item.mbid}-${index}`} className="flex items-center gap-2">
              {index > 0 ? <span aria-hidden="true">›</span> : null}
              {active ? (
                <span aria-current="page" className="font-semibold text-neutral-900">
                  {item.name}
                </span>
              ) : (
                <Link
                  href={buildGrooveHref(trail.slice(0, index + 1).map((entry) => entry.mbid))}
                  prefetch={false}
                  className="rounded-sm transition hover:text-neutral-900 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-orange-300"
                >
                  {item.name}
                </Link>
              )}
            </li>
          );
        })}
      </ol>
    </nav>
  );
}
