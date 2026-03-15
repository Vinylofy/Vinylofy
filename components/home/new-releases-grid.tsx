const newReleases = [
  { artist: "The Smile", title: "Cutouts" },
  { artist: "Fontaines D.C.", title: "Romance" },
  { artist: "Nick Cave", title: "Wild God" },
  { artist: "The Cure", title: "Songs of a Lost World" },
];

export function NewReleasesGrid() {
  return (
    <section className="mx-auto max-w-5xl px-6 py-10">
      <div className="mb-6">
        <h2 className="text-2xl font-semibold tracking-tight">
          Nieuwe releases
        </h2>
      </div>

      <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
        {newReleases.map((item) => (
          <article
            key={`${item.artist}-${item.title}`}
            className="rounded-2xl border border-neutral-200 bg-white p-4"
          >
            <div className="aspect-square rounded-xl bg-neutral-100" />
            <div className="mt-3">
              <p className="truncate font-medium">{item.artist}</p>
              <p className="line-clamp-2 text-sm text-neutral-500">
                {item.title}
              </p>
              <p className="mt-2 text-sm font-semibold text-orange-600">
                vanaf €--,--
              </p>
            </div>
          </article>
        ))}
      </div>
    </section>
  );
}