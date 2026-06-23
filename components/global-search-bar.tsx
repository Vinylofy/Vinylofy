import { SearchAutocompleteForm } from "@/components/search/search-autocomplete-form";

type GlobalSearchBarProps = {
  defaultValue?: string;
  compact?: boolean;
};

export function GlobalSearchBar({
  defaultValue = "",
  compact = false,
}: GlobalSearchBarProps) {
  return (
    <SearchAutocompleteForm
      initialValue={defaultValue}
      placeholder="Zoek op artiest of albumtitel"
      variant="global"
      compact={compact}
    />
  );
}
