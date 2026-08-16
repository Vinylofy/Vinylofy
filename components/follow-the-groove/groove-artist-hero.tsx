import Link from "next/link";
import { CoverImage } from "@/components/cover-image";
import { formatEntityType, formatProductCount } from "@/lib/follow-the-groove/presentation";
import type { FtgArtistView } from "@/lib/follow-the-groove/types";

export function GrooveArtistHero({ artist }: { artist: FtgArtistView }) {
  return (
    <section
      aria-labelledby="active-groove-artist"
      className="rounded-3xl border border-neutral-200 bg-white p-5 shadow-sm md:p-8"
    >
      <div className="grid items-center gap-6 md:grid-cols-[220px_minmax(0,1fr)] md:gap-8">
        <div className="mx-auto flex aspect-square w-full max-w-[220px] items-center justify-center overflow-hidden rounded-2xl bg-neutral-50">
          <CoverImage
            src={artist.representativeCover.src}
            alt={artist.representativeCover.alt}
            className="h-full w-full object-cover data-[cover-fallback=true]:h-[82%] data-[cover-fallback=true]:w-[82%] data-[cover-fallback=true]:object-contain"
          />
        </div>

        <div className="min-w-0 text-center md:text-left">
          <p className="text-xs font-medium uppercase tracking-[0.16em] text-neutral-500">
            Nu in de groove · {formatEntityType(artist.entityType)}
          </p>
          <h1
            id="active-groove-artist"
            className="mt-2 text-3xl font-semibold tracking-tight text-neutral-950 md:text-4xl"
          >
            {artist.name}
          </h1>
          <p className="mt-4 text-sm text-neutral-600">
            {formatProductCount(artist.productCount)}
          </p>
          {artist.productCount > 0 && artist.searchHref ? (
            <Link
              href={artist.searchHref}
              prefetch={false}
              className="mt-4 inline-flex min-h-11 items-center justify-center rounded-full bg-orange-500 px-5 py-2 text-sm font-medium text-white transition hover:bg-orange-600 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-orange-300 focus-visible:ring-offset-2"
            >
              Bekijk titels →
            </Link>
          ) : null}
        </div>
      </div>
    </section>
  );
}
