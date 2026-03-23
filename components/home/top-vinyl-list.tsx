import { formatEuro, type HomeProduct } from "@/lib/vinylofy-data";

type TopVinylListProps = {
  items: HomeProduct[];
};

export function TopVinylList({ items }: TopVinylListProps) {
  return (
    <section className="mx-auto w-full max-w-6xl px-6 py-8">
      <h2 className="mb-4 text-2xl font-semibold tracking-tight text-neutral-950">
        Top 25 vinyl van dit moment
      </h2>

      {items.length === 0 ? (
        <div className="rounded-2xl border border-neutral-200 bg-white px-5 py-6 text-sm text-neutral-500">
          Nog geen resultaten beschikbaar.
        </div>
      ) : (
        <div className="overflow-hidden rounded-2xl border border-neutral-200 bg-white">
          {items.map((item, index) => (
            <div
              key={`${item.artist}-${item.title}-${index}`}
              className="grid grid-cols-[22px_minmax(0,1fr)_auto] items-center gap-3 border-b border-neutral-200 px-4 py-3 last:border-b-0 md:grid-cols-[26px_minmax(0,1fr)_auto] md:px-5 md:py-3"
            >
              <div className="text-sm font-medium tabular-nums text-neutral-400">
                {index + 1}
              </div>

              <div className="min-w-0">
                <div className="truncate text-sm leading-snug md:text-[15px]">
                  <span className="font-semibold text-neutral-950">
                    {item.artist}
                  </span>
                  <span className="mx-1.5 text-neutral-300">—</span>
                  <span className="text-neutral-700">{item.title}</span>
                </div>
              </div>

              <div className="whitespace-nowrap text-sm font-semibold text-orange-600 md:text-[15px]">
                vanaf {formatEuro(item.lowestPrice)}
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}