"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";

type SearchControlsProps = {
  initialQuery: string;
};

export function SearchControls({ initialQuery }: SearchControlsProps) {
  const router = useRouter();
  const [query, setQuery] = useState(initialQuery);

  function submitSearch(event?: React.FormEvent) {
    event?.preventDefault();
    const trimmed = query.trim();
    const params = new URLSearchParams();
    if (trimmed) params.set("q", trimmed);
    router.push(`/search${params.toString() ? `?${params.toString()}` : ""}`);
  }

  return (
    <form onSubmit={submitSearch} className="w-full">
      <div className="relative w-full">
        <input
          type="search"
          name="q"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Zoek op artiest of titel"
          className="h-11 w-full rounded-full border border-neutral-300 bg-white pl-5 pr-28 text-sm text-neutral-900 shadow-sm outline-none transition focus:border-neutral-400"
        />
        <button
          type="submit"
          className="absolute right-1 top-1 inline-flex h-9 items-center rounded-full bg-neutral-900 px-5 text-sm font-medium text-white hover:bg-neutral-800"
        >
          Zoek
        </button>
      </div>
    </form>
  );
}
