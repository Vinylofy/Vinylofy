"use client";

import type { ChangeEvent } from "react";
import { useRouter } from "next/navigation";

import {
  SEARCH_SORT_OPTIONS,
  type SearchSort,
} from "@/lib/search-sort";

type SearchSortSelectProps = {
  value: SearchSort;
  query: string;
  artistFilter: string;
};

export function SearchSortSelect({
  value,
  query,
  artistFilter,
}: SearchSortSelectProps) {
  const router = useRouter();

  function handleChange(nextSort: SearchSort) {
    const params = new URLSearchParams();

    if (query) {
      params.set("q", query);
    }

    if (artistFilter) {
      params.set(
        "artist_filter",
        artistFilter,
      );
    }

    params.set("sort", nextSort);

    router.replace(
      `/search?${params.toString()}`,
      {
        scroll: false,
      },
    );
  }

  function handleSelectChange(
    event: ChangeEvent<HTMLSelectElement>,
  ) {
    handleChange(
      event.currentTarget.value as SearchSort,
    );
  }

  return (
    <label className="flex w-full items-center gap-2 sm:w-auto">
      <span className="shrink-0 text-xs font-medium text-neutral-600">
        Sorteren op
      </span>

      <select
        aria-label="Sorteer zoekresultaten"
        value={value}
        onChange={handleSelectChange}
        className="min-w-0 flex-1 rounded-full border border-neutral-300 bg-white px-4 py-2 text-sm text-neutral-900 shadow-sm outline-none transition focus:border-orange-500 focus:ring-2 focus:ring-orange-200 sm:w-auto sm:flex-none"
      >
        {SEARCH_SORT_OPTIONS.map((option) => (
          <option
            key={option.value}
            value={option.value}
          >
            {option.label}
          </option>
        ))}
      </select>
    </label>
  );
}
