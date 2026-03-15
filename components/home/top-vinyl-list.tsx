import Link from "next/link";
import { formatEuro, type HomeProduct } from "@/lib/vinylofy-data";

type TopVinylListProps = {
  items: HomeProduct[];
};

export function TopVinylList({ items }: TopVinylListProps) {
  return (
    <section className="mx-auto max-w-5xl px-6 py-10">
      <div className="mb-6">
        <h2 className="text-2xl font-semibold tracking-tight">
          Top 25 vinyl van dit moment
        </h2>
      </div>

      <div className="rounded-2xl border border-neutral-200 bg-white">
        {items.length === 0 ? (
          <div className="px-6 py-8 text-sm text-neutral-500">
            Nog geen resultaten beschikbaar.
          </div>
        ) : (
          <ol className="divide-y divide-neutral-200">
            {items.map((item, index) => (
              <li
                key={item.id}
                className="flex items-center justify-between gap-4 px-4 py-4 md:px-6"
              >
                <div className="flex min-w-0 items-start gap-4">
                  <span className="w-6 shrink-0 text-sm font-medium text-neutral-400">
                    {index + 1}
                  </span>

                  <div className="min-w-0">
                    <Link
                      href={`/search?q=${encodeURIComponent(`${item.artist} ${item.title}`)}`}
                      className="block"
                    >
                      <p className="truncate font-medium hover:text-orange-600">
                        {item.artist}
                      </p>
                      <p className="truncate text-sm text-neutral-500">
                        {item.title}
                      </p>
                    </Link>
                  </div>
                </div>

                <div className="shrink-0 text-sm font-semibold text-orange-600">
                  vanaf {formatEuro(item.lowestPrice)}
                </div>
              </li>
            ))}
          </ol>
        )}
      </div>
    </section>
  );
}