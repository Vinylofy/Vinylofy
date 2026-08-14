import { getVisibleBasePriceOfferSummary } from "@/lib/offer-summary";
import type { SearchResultItem } from "@/lib/vinylofy-data";

export const SEARCH_SORT_OPTIONS = [
  {
    value: "relevance",
    label: "Relevantie",
  },
  {
    value: "shops_desc",
    label: "Aantal aanbieders: hoog naar laag",
  },
  {
    value: "price_asc",
    label: "Prijs: laag naar hoog",
  },
  {
    value: "price_desc",
    label: "Prijs: hoog naar laag",
  },
  {
    value: "artist_asc",
    label: "Artiest: A–Z",
  },
  {
    value: "artist_desc",
    label: "Artiest: Z–A",
  },
  {
    value: "title_asc",
    label: "Titel: A–Z",
  },
  {
    value: "title_desc",
    label: "Titel: Z–A",
  },
] as const;

export type SearchSort =
  (typeof SEARCH_SORT_OPTIONS)[number]["value"];

export const DEFAULT_SEARCH_SORT: SearchSort =
  "relevance";

const VALID_SEARCH_SORTS = new Set<string>(
  SEARCH_SORT_OPTIONS.map((option) => option.value),
);

const DUTCH_COLLATOR = new Intl.Collator("nl-NL", {
  sensitivity: "base",
  numeric: true,
  ignorePunctuation: true,
});

export function parseSearchSort(
  value: string | undefined,
): SearchSort {
  if (value && VALID_SEARCH_SORTS.has(value)) {
    return value as SearchSort;
  }

  return DEFAULT_SEARCH_SORT;
}

function compareText(
  left: string,
  right: string,
): number {
  return DUTCH_COLLATOR.compare(
    left.trim(),
    right.trim(),
  );
}

function getResultPrice(
  item: SearchResultItem,
): number | null {
  const bestOffer = getVisibleBasePriceOfferSummary(item.shops, 1)[0];

  if (bestOffer && Number.isFinite(bestOffer.price)) {
    return bestOffer.price;
  }

  if (
    item.lowestPrice !== null &&
    Number.isFinite(item.lowestPrice)
  ) {
    return item.lowestPrice;
  }

  return null;
}

function comparePrices(
  left: SearchResultItem,
  right: SearchResultItem,
  direction: "asc" | "desc",
): number {
  const leftPrice = getResultPrice(left);
  const rightPrice = getResultPrice(right);

  // Resultaten zonder prijs staan altijd onderaan.
  if (leftPrice === null && rightPrice === null) {
    return 0;
  }

  if (leftPrice === null) {
    return 1;
  }

  if (rightPrice === null) {
    return -1;
  }

  return direction === "asc"
    ? leftPrice - rightPrice
    : rightPrice - leftPrice;
}

function compareStableFallback(
  left: SearchResultItem,
  right: SearchResultItem,
): number {
  return (
    compareText(left.artist, right.artist) ||
    compareText(left.title, right.title) ||
    left.id.localeCompare(right.id)
  );
}

export function sortSearchResults(
  results: SearchResultItem[],
  sort: SearchSort,
): SearchResultItem[] {
  if (sort === "relevance") {
    // searchProducts levert resultaten al in relevantievolgorde.
    return results.slice();
  }

  return results.slice().sort((left, right) => {
    if (sort === "shops_desc") {
      const shopOrder =
        right.shops.length - left.shops.length;

      if (shopOrder !== 0) {
        return shopOrder;
      }

      return (
        comparePrices(left, right, "asc") ||
        compareStableFallback(left, right)
      );
    }

    if (
      sort === "price_asc" ||
      sort === "price_desc"
    ) {
      return (
        comparePrices(
          left,
          right,
          sort === "price_asc" ? "asc" : "desc",
        ) ||
        compareStableFallback(left, right)
      );
    }

    if (
      sort === "artist_asc" ||
      sort === "artist_desc"
    ) {
      const artistOrder = compareText(
        left.artist,
        right.artist,
      );

      if (artistOrder !== 0) {
        return sort === "artist_asc"
          ? artistOrder
          : -artistOrder;
      }

      return (
        compareText(left.title, right.title) ||
        comparePrices(left, right, "asc") ||
        left.id.localeCompare(right.id)
      );
    }

    const titleOrder = compareText(
      left.title,
      right.title,
    );

    if (titleOrder !== 0) {
      return sort === "title_asc"
        ? titleOrder
        : -titleOrder;
    }

    return (
      compareText(left.artist, right.artist) ||
      comparePrices(left, right, "asc") ||
      left.id.localeCompare(right.id)
    );
  });
}
