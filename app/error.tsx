'use client';

import { useEffect } from 'react';

export default function ErrorPage({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error(error);
  }, [error]);

  return (
    <div className="min-h-screen bg-white px-6 py-16 text-neutral-900">
      <div className="mx-auto max-w-2xl rounded-3xl border border-neutral-200 bg-white p-8 shadow-sm">
        <p className="mb-2 text-sm uppercase tracking-[0.2em] text-neutral-500">
          Er ging iets mis
        </p>

        <h1 className="text-2xl font-semibold text-neutral-900">
          Deze pagina kon niet goed geladen worden
        </h1>

        <p className="mt-3 text-sm leading-6 text-neutral-600">
          Probeer het opnieuw. Blijft het probleem terugkomen, ga dan een stap
          terug en voer de zoekopdracht opnieuw uit.
        </p>

        {error?.message ? (
          <p className="mt-4 rounded-2xl bg-neutral-50 px-4 py-3 text-xs text-neutral-500">
            {error.message}
          </p>
        ) : null}

        <div className="mt-6 flex gap-3">
          <button
            onClick={() => reset()}
            className="rounded-full bg-orange-500 px-5 py-2 text-sm font-medium text-white hover:bg-orange-600"
          >
            Opnieuw proberen
          </button>

          <a
            href="/"
            className="rounded-full border border-neutral-300 px-5 py-2 text-sm font-medium text-neutral-700 hover:bg-neutral-50"
          >
            Naar home
          </a>
        </div>
      </div>
    </div>
  );
}