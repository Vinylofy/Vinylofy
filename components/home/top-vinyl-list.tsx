const top25 = [
  { artist: "Fleetwood Mac", title: "Rumours", price: "€19,95" },
  { artist: "Nirvana", title: "Nevermind", price: "€18,99" },
  { artist: "Pink Floyd", title: "The Wall", price: "€21,50" },
  { artist: "Daft Punk", title: "Random Access Memories", price: "€24,95" },
  { artist: "Radiohead", title: "OK Computer", price: "€22,99" },
];

export function TopVinylList() {
  return (
    <section className="mx-auto max-w-5xl px-6 py-10">
      <div className="mb-6">
        <h2 className="text-2xl font-semibold tracking-tight">
          Top 25 vinyl van dit moment
        </h2>
      </div>

      <div className="rounded-2xl border border-neutral-200 bg-white">
        <ol className="divide-y divide-neutral-200">
          {top25.map((item, index) => (
            <li
              key={`${item.artist}-${item.title}`}
              className="flex items-center justify-between gap-4 px-4 py-4 md:px-6"
            >
              <div className="flex min-w-0 items-start gap-4">
                <span className="w-6 shrink-0 text-sm font-medium text-neutral-400">
                  {index + 1}
                </span>
                <div className="min-w-0">
                  <p className="truncate font-medium">{item.artist}</p>
                  <p className="truncate text-sm text-neutral-500">
                    {item.title}
                  </p>
                </div>
              </div>

              <div className="shrink-0 text-sm font-semibold text-orange-600">
                vanaf {item.price}
              </div>
            </li>
          ))}
        </ol>
      </div>
    </section>
  );
}