import { SearchAutocompleteForm } from "@/components/search/search-autocomplete-form";

type GlobalSearchBarProps = {
  defaultValue?: string;
  compact?: boolean;
  marketCode?: string;
};

export function GlobalSearchBar({
  defaultValue = "",
  compact = false,
  marketCode = "NL",
}: GlobalSearchBarProps) {
  return (
    <SearchAutocompleteForm
      initialValue={defaultValue}
      marketCode={marketCode}
      placeholder="Zoek op artiest of albumtitel"
      variant="global"
      compact={compact}
    />
  );
}
