export function GrooveEmptyState() {
  return (
    <div className="rounded-3xl border border-neutral-200 bg-white p-8 text-center shadow-sm">
      <h2 className="text-lg font-semibold text-neutral-900">Hier eindigt deze groove</h2>
      <p className="mx-auto mt-2 max-w-lg text-sm leading-6 text-neutral-600">
        Voor deze artiest zijn nu geen volgende verbindingen beschikbaar. Ga terug in je
        gevolgde route om een andere afslag te kiezen.
      </p>
    </div>
  );
}
