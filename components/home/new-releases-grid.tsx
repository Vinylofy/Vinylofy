import Link from "next/link";
import { formatEuro, type HomeProduct } from "@/lib/vinylofy-data";

type NewReleasesGridProps = {
  items: HomeProduct[];
};

export function NewReleasesGrid({ items }: NewReleasesGridProps) {
  return (
    <section className="mx-auto max-w-5xl px-6 py-10">
      <div className="mb-6">
        <h2 className="text-2xl font-semibold tracking-tight">
          Nieuwe releases
        </h2>
      </div>

      {items.length === 0 ? (
        <div className="rounded-2xl border border-neutral-200 bg-white p-6 text-sm text-neutral-500">
          Nog geen nieuwe releases beschikbaar.
        </div>
      ) : (
        <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
          {items.map((item) => (
            <Link
              key={item.id}
              href={`/search?q=${encodeURIComponent(`${item.artist} ${item.title}`)}`}
              className="rounded-2xl border border-neutral-200 bg-white p-4 transition hover:border-orange-300"
            >
              {item.coverUrl ? (
                <img
                  src={item.coverUrl}
                  alt={`${item.artist} - ${item.title}`}
                  className="aspect-square w-full rounded-xl bg-neutral-100 object-cover"
                />
              ) : (
                <div className="aspect-square rounded-xl bg-neutral-100" />
              )}

              <div className="mt-3">
                <p className="truncate font-medium">{item.artist}</p>
                <p className="line-clamp-2 text-sm text-neutral-500">
                  {item.title}
                </p>
                <p className="mt-2 text-sm font-semibold text-orange-600">
                  vanaf {formatEuro(item.lowestPrice)}
                </p>
              </div>
            </Link>
          ))}
        </div>
      )}
    </section>
  );
}