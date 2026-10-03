"use client";

import { SearchAutocompleteForm } from "@/components/search/search-autocomplete-form";

type SearchControlsProps = {
  initialQuery: string;
  marketCode?: string;
};

export function SearchControls({ initialQuery, marketCode = "NL" }: SearchControlsProps) {
  return (
    <SearchAutocompleteForm
      initialValue={initialQuery}
      marketCode={marketCode}
      placeholder="Zoek op artiest of albumtitel"
      variant="search"
      openOnFocus={false}
      analyticsSource="header"
    />
  );
}
