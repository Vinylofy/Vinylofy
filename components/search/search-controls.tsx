"use client";

import { SearchAutocompleteForm } from "@/components/search/search-autocomplete-form";

type SearchControlsProps = {
  initialQuery: string;
};

export function SearchControls({ initialQuery }: SearchControlsProps) {
  return (
    <SearchAutocompleteForm
      initialValue={initialQuery}
      placeholder="Zoek op artiest of titel"
      variant="search"
    />
  );
}
