export default function Loading() {
  return (
    <div className="min-h-screen bg-white text-neutral-900">
      <div className="border-b border-neutral-200 bg-white">
        <div className="mx-auto max-w-6xl px-6 py-4">
          <div className="h-6 w-28 animate-pulse rounded bg-neutral-200" />
        </div>
      </div>

      <main className="mx-auto max-w-6xl px-6 py-10">
        <div className="space-y-4">
          <div className="h-10 w-72 animate-pulse rounded bg-neutral-200" />
          <div className="h-4 w-96 animate-pulse rounded bg-neutral-100" />
          <div className="h-12 w-full max-w-2xl animate-pulse rounded-full bg-neutral-100" />
        </div>

        <div className="mt-10 grid grid-cols-1 gap-4 md:grid-cols-2">
          {Array.from({ length: 6 }).map((_, index) => (
            <div
              key={index}
              className="rounded-2xl border border-neutral-200 bg-white p-4"
            >
              <div className="flex gap-4">
                <div className="h-24 w-24 animate-pulse rounded-xl bg-neutral-100" />
                <div className="flex-1 space-y-3">
                  <div className="h-5 w-3/4 animate-pulse rounded bg-neutral-200" />
                  <div className="h-4 w-1/2 animate-pulse rounded bg-neutral-100" />
                  <div className="h-6 w-24 animate-pulse rounded bg-neutral-100" />
                </div>
              </div>
            </div>
          ))}
        </div>
      </main>
    </div>
  );
}