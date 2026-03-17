"use client";

export default function GlobalError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <html lang="nl">
      <body>
        <div className="min-h-screen bg-white px-6 py-16 text-neutral-900">
          <div className="mx-auto max-w-2xl rounded-3xl border border-neutral-200 bg-white p-8">
            <p className="text-sm font-medium uppercase tracking-[0.14em] text-orange-600">
              Vinylofy
            </p>

            <h1 className="mt-4 text-3xl font-semibold tracking-tight">
              Er ging iets mis
            </h1>

            <p className="mt-4 text-neutral-600">
              Er trad een onverwachte fout op tijdens het laden van de pagina.
              Probeer het opnieuw.
            </p>

            {error?.message ? (
              <p className="mt-4 rounded-xl bg-neutral-50 px-4 py-3 text-sm text-neutral-500">
                {error.message}
              </p>
            ) : null}

            <div className="mt-8 flex flex-wrap gap-3">
              <button
                onClick={reset}
                className="inline-flex rounded-full bg-orange-600 px-5 py-2 text-sm font-medium text-white transition hover:bg-orange-700"
              >
                Opnieuw proberen
              </button>

              <a
                href="/"
                className="inline-flex rounded-full border border-neutral-300 px-5 py-2 text-sm font-medium text-neutral-700 transition hover:border-neutral-400 hover:text-neutral-900"
              >
                Naar homepage
              </a>
            </div>
          </div>
        </div>
      </body>
    </html>
  );
}