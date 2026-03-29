"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";

type SearchSuggestion = {
  id: string;
  kind: "artist" | "album";
  label: string;
  sublabel?: string;
  href: string;
  searchValue: string;
};

type SearchAutocompleteFormProps = {
  initialValue?: string;
  placeholder: string;
  variant: "global" | "search";
  compact?: boolean;
};

function buildSearchHref(query: string) {
  const trimmed = query.trim();
  const params = new URLSearchParams();

  if (trimmed) {
    params.set("q", trimmed);
  }

  return `/search${params.toString() ? `?${params.toString()}` : ""}`;
}

export function SearchAutocompleteForm({
  initialValue = "",
  placeholder,
  variant,
  compact = false,
}: SearchAutocompleteFormProps) {
  const router = useRouter();
  const [query, setQuery] = useState(initialValue);
  const [suggestions, setSuggestions] = useState<SearchSuggestion[]>([]);
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(-1);
  const [isLoading, setIsLoading] = useState(false);
  const requestRef = useRef<AbortController | null>(null);

  useEffect(() => {
    setQuery(initialValue);
  }, [initialValue]);

  useEffect(() => {
    const trimmed = query.trim();

    if (trimmed.length < 2) {
      requestRef.current?.abort();
      setSuggestions([]);
      setOpen(false);
      setActiveIndex(-1);
      setIsLoading(false);
      return;
    }

    const controller = new AbortController();
    requestRef.current?.abort();
    requestRef.current = controller;

    const timeoutId = window.setTimeout(async () => {
      try {
        setIsLoading(true);

        const response = await fetch(
          `/api/search-suggest?q=${encodeURIComponent(trimmed)}`,
          {
            method: "GET",
            signal: controller.signal,
            cache: "no-store",
          },
        );

        if (!response.ok) {
          throw new Error(`Suggest request failed with status ${response.status}`);
        }

        const json = (await response.json()) as {
          suggestions?: SearchSuggestion[];
        };

        const nextSuggestions = json.suggestions ?? [];
        setSuggestions(nextSuggestions);
        setOpen(nextSuggestions.length > 0);
        setActiveIndex(-1);
      } catch (error) {
        if ((error as Error).name === "AbortError") {
          return;
        }

        console.error("Autocomplete ophalen mislukt", error);
        setSuggestions([]);
        setOpen(false);
        setActiveIndex(-1);
      } finally {
        if (!controller.signal.aborted) {
          setIsLoading(false);
        }
      }
    }, 180);

    return () => {
      window.clearTimeout(timeoutId);
      controller.abort();
    };
  }, [query]);

  const activeSuggestion = useMemo(() => {
    if (activeIndex < 0 || activeIndex >= suggestions.length) {
      return null;
    }

    return suggestions[activeIndex] ?? null;
  }, [activeIndex, suggestions]);

  function goToSearch(nextQuery: string) {
    router.push(buildSearchHref(nextQuery));
  }

  function chooseSuggestion(suggestion: SearchSuggestion) {
    setQuery(suggestion.searchValue);
    setOpen(false);
    setActiveIndex(-1);
    router.push(suggestion.href);
  }

  function submitSearch(event?: React.FormEvent) {
    event?.preventDefault();

    if (activeSuggestion) {
      chooseSuggestion(activeSuggestion);
      return;
    }

    goToSearch(query);
  }

  function handleKeyDown(event: React.KeyboardEvent<HTMLInputElement>) {
    if (event.key === "ArrowDown") {
      if (suggestions.length === 0) return;
      event.preventDefault();
      setOpen(true);
      setActiveIndex((current) =>
        current < suggestions.length - 1 ? current + 1 : 0,
      );
      return;
    }

    if (event.key === "ArrowUp") {
      if (suggestions.length === 0) return;
      event.preventDefault();
      setOpen(true);
      setActiveIndex((current) =>
        current > 0 ? current - 1 : suggestions.length - 1,
      );
      return;
    }

    if (event.key === "Enter" && activeSuggestion) {
      event.preventDefault();
      chooseSuggestion(activeSuggestion);
      return;
    }

    if (event.key === "Escape") {
      setOpen(false);
      setActiveIndex(-1);
    }
  }

  const isGlobal = variant === "global";

  return (
    <form onSubmit={submitSearch} className="w-full" role="search" autoComplete="off">
      <div className="relative w-full">
        {isGlobal ? (
          <div
            className={[
              "flex items-center rounded-full border border-neutral-300 bg-white shadow-sm",
              compact ? "px-2 py-2" : "px-3 py-2",
            ].join(" ")}
          >
            <input
              type="search"
              name="q"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              onFocus={() => {
                if (suggestions.length > 0) {
                  setOpen(true);
                }
              }}
              onBlur={() => {
                window.setTimeout(() => {
                  setOpen(false);
                  setActiveIndex(-1);
                }, 120);
              }}
              onKeyDown={handleKeyDown}
              placeholder={placeholder}
              className={[
                "w-full bg-transparent outline-none placeholder:text-neutral-400",
                compact ? "px-3 py-1 text-sm" : "px-3 py-2 text-sm md:text-base",
              ].join(" ")}
              aria-autocomplete="list"
              aria-expanded={open}
              aria-controls="vinylofy-search-suggestions"
            />

            <button
              type="submit"
              className={[
                "rounded-full bg-orange-600 font-medium text-white transition hover:bg-orange-700",
                compact ? "px-4 py-2 text-sm" : "px-5 py-2 text-sm",
              ].join(" ")}
            >
              Zoek
            </button>
          </div>
        ) : (
          <>
            <input
              type="search"
              name="q"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              onFocus={() => {
                if (suggestions.length > 0) {
                  setOpen(true);
                }
              }}
              onBlur={() => {
                window.setTimeout(() => {
                  setOpen(false);
                  setActiveIndex(-1);
                }, 120);
              }}
              onKeyDown={handleKeyDown}
              placeholder={placeholder}
              className="h-11 w-full rounded-full border border-neutral-300 bg-white pl-5 pr-28 text-sm text-neutral-900 shadow-sm outline-none transition focus:border-neutral-400"
              aria-autocomplete="list"
              aria-expanded={open}
              aria-controls="vinylofy-search-suggestions"
            />

            <button
              type="submit"
              className="absolute right-1 top-1 inline-flex h-9 items-center rounded-full bg-neutral-900 px-5 text-sm font-medium text-white hover:bg-neutral-800"
            >
              Zoek
            </button>
          </>
        )}

        {open ? (
          <div
            id="vinylofy-search-suggestions"
            role="listbox"
            className="absolute left-0 right-0 top-[calc(100%+8px)] z-50 max-h-[156px] overflow-y-auto rounded-2xl border border-neutral-200 bg-white p-1 shadow-lg"
          >
            {suggestions.map((suggestion, index) => {
              const active = index === activeIndex;

              return (
                <button
                  key={suggestion.id}
                  type="button"
                  role="option"
                  aria-selected={active}
                  onMouseDown={(event) => event.preventDefault()}
                  onClick={() => chooseSuggestion(suggestion)}
                  onMouseEnter={() => setActiveIndex(index)}
                  className={[
                    "flex w-full items-center justify-between gap-3 rounded-xl px-3 py-2 text-left transition",
                    active ? "bg-orange-50" : "bg-white hover:bg-neutral-50",
                  ].join(" ")}
                >
                  <span className="min-w-0">
                    <span className="block truncate text-sm font-medium text-neutral-950">
                      {suggestion.label}
                    </span>
                    {suggestion.sublabel ? (
                      <span className="mt-0.5 block truncate text-[11px] text-neutral-500">
                        {suggestion.sublabel}
                      </span>
                    ) : null}
                  </span>

                  <span className="shrink-0 rounded-full border border-neutral-200 px-2 py-0.5 text-[10px] font-medium uppercase tracking-[0.08em] text-neutral-500">
                    {suggestion.kind === "artist" ? "Artiest" : "Album"}
                  </span>
                </button>
              );
            })}
          </div>
        ) : null}

        {isLoading && query.trim().length >= 2 ? (
          <div className="pointer-events-none absolute right-24 top-1/2 -translate-y-1/2 text-[11px] text-neutral-400">
            zoeken…
          </div>
        ) : null}
      </div>
    </form>
  );
}
