import posthog from "posthog-js";

export const ANALYTICS_EVENTS = {
  searchPerformed: "search_performed",
  searchResultClicked: "search_result_clicked",
  autocompleteSelected: "autocomplete_selected",
  releaseViewed: "release_viewed",
  offersExpanded: "offers_expanded",
  offerClicked: "offer_clicked",
  shopClicked: "shop_clicked",
  priceHistoryViewed: "price_history_viewed",
  ftgStarted: "ftg_started",
  ftgRelationClicked: "ftg_relation_clicked",
  ftgArtistViewed: "ftg_artist_viewed",
  ftgReleaseClicked: "ftg_release_clicked",
  countryChanged: "country_changed",
  sortChanged: "sort_changed",
} as const;

export type AnalyticsSource =
  | "homepage"
  | "header"
  | "release_page"
  | "search"
  | "release_detail"
  | "top45"
  | "new_releases"
  | "ftg"
  | "unknown";

export type FtgRelationType =
  | "membership"
  | "recording_collaboration"
  | "factual_and_similarity"
  | "similar_artist";

type AnalyticsValue = string | number | boolean | null;
type AnalyticsProperties = Record<string, AnalyticsValue>;

const SEARCH_SOURCE_KEY = "vinylofy:analytics:search-source";
const RELEASE_SOURCE_KEY = "vinylofy:analytics:release-source";
const EMAIL_PATTERN = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const MAX_QUERY_LENGTH = 120;

const environment =
  process.env.NEXT_PUBLIC_VERCEL_ENV ?? process.env.NODE_ENV ?? "unknown";
const analyticsConfigured = Boolean(
  process.env.NEXT_PUBLIC_POSTHOG_PROJECT_TOKEN &&
    process.env.NEXT_PUBLIC_POSTHOG_HOST,
);

function capture(event: string, properties: AnalyticsProperties): void {
  if (!analyticsConfigured) return;

  try {
    posthog.capture(event, {
      ...properties,
      environment,
    });
  } catch {
    // Analytics must never affect rendering or navigation.
  }
}

export function sanitizeSearchQuery(value: string): string | null {
  const normalized = value.trim().replace(/\s+/g, " ").slice(0, MAX_QUERY_LENGTH);
  if (!normalized) return null;
  return EMAIL_PATTERN.test(normalized) ? "[redacted]" : normalized;
}

export function normalizeSearchQuery(value: string): string | null {
  const sanitized = sanitizeSearchQuery(value);
  return sanitized ? sanitized.toLowerCase() : null;
}

function browserStorageSet(key: string, value: AnalyticsSource): void {
  if (typeof window === "undefined") return;
  try {
    window.sessionStorage.setItem(key, value);
  } catch {
    // Storage may be unavailable in privacy mode or tests.
  }
}

function browserStorageConsume(key: string): AnalyticsSource | null {
  if (typeof window === "undefined") return null;
  try {
    const value = window.sessionStorage.getItem(key) as AnalyticsSource | null;
    window.sessionStorage.removeItem(key);
    return value;
  } catch {
    return null;
  }
}

export function rememberSearchSource(source: AnalyticsSource): void {
  browserStorageSet(SEARCH_SOURCE_KEY, source);
}

export function consumeSearchSource(): AnalyticsSource | null {
  return browserStorageConsume(SEARCH_SOURCE_KEY);
}

export function rememberReleaseSource(source: AnalyticsSource): void {
  browserStorageSet(RELEASE_SOURCE_KEY, source);
}

export function consumeReleaseSource(): AnalyticsSource | null {
  return browserStorageConsume(RELEASE_SOURCE_KEY);
}

export function trackSearch(input: {
  query: string;
  resultCount: number;
  source?: AnalyticsSource;
  searchType?: string;
}): void {
  const query = sanitizeSearchQuery(input.query);
  const normalizedQuery = normalizeSearchQuery(input.query);
  if (!query || !normalizedQuery) return;

  capture(ANALYTICS_EVENTS.searchPerformed, {
    query,
    normalized_query: normalizedQuery,
    result_count: input.resultCount,
    source: input.source ?? "unknown",
    search_type: input.searchType ?? null,
  });
}

export function trackSearchResultClick(input: {
  query: string;
  resultPosition: number;
  resultCount: number;
  releaseId: string;
  artistId?: string | null;
  artistName: string;
  releaseTitle: string;
  ean?: string | null;
  source?: AnalyticsSource;
}): void {
  const query = sanitizeSearchQuery(input.query);
  const normalizedQuery = normalizeSearchQuery(input.query);
  if (!query || !normalizedQuery) return;

  capture(ANALYTICS_EVENTS.searchResultClicked, {
    query,
    normalized_query: normalizedQuery,
    result_position: input.resultPosition,
    result_count: input.resultCount,
    release_id: input.releaseId,
    artist_id: input.artistId ?? null,
    artist_name: input.artistName,
    release_title: input.releaseTitle,
    ean: input.ean ?? null,
    source: input.source ?? "search",
  });
}

export function trackAutocompleteSelected(input: {
  query: string;
  selectedValue: string;
  resultPosition?: number;
  entityType: string;
  source?: AnalyticsSource;
}): void {
  const query = sanitizeSearchQuery(input.query);
  const selectedValue = sanitizeSearchQuery(input.selectedValue);
  if (!query || !selectedValue) return;

  capture(ANALYTICS_EVENTS.autocompleteSelected, {
    query,
    selected_value: selectedValue,
    result_position: input.resultPosition ?? null,
    entity_type: input.entityType,
    source: input.source ?? "unknown",
  });
}

export function trackReleaseView(input: {
  releaseId: string;
  ean?: string | null;
  artistId?: string | null;
  artistName: string;
  releaseTitle: string;
  offerCount: number;
  lowestPrice: number | null;
  source?: AnalyticsSource;
}): void {
  capture(ANALYTICS_EVENTS.releaseViewed, {
    release_id: input.releaseId,
    ean: input.ean ?? null,
    artist_id: input.artistId ?? null,
    artist_name: input.artistName,
    release_title: input.releaseTitle,
    offer_count: input.offerCount,
    lowest_price: input.lowestPrice,
    source: input.source ?? "unknown",
  });
}

export function trackOffersExpanded(input: {
  releaseId: string;
  ean?: string | null;
  offerCount: number;
  source?: AnalyticsSource;
}): void {
  capture(ANALYTICS_EVENTS.offersExpanded, {
    release_id: input.releaseId,
    ean: input.ean ?? null,
    offer_count: input.offerCount,
    source: input.source ?? "release_detail",
  });
}

type OfferClickInput = {
  releaseId: string;
  ean?: string | null;
  artistName: string;
  releaseTitle: string;
  shopId: string;
  shopName: string;
  price: number;
  shippingCost?: number | null;
  totalPrice?: number | null;
  offerPosition: number;
  isCheapest: boolean;
  source?: AnalyticsSource;
};

export function trackOfferClick(input: OfferClickInput): void {
  capture(ANALYTICS_EVENTS.offerClicked, {
    release_id: input.releaseId,
    ean: input.ean ?? null,
    shop_id: input.shopId,
    shop_name: input.shopName,
    price: input.price,
    shipping_cost: input.shippingCost ?? null,
    total_price: input.totalPrice ?? null,
    offer_position: input.offerPosition,
    is_cheapest: input.isCheapest,
    source: input.source ?? "release_detail",
  });
}

export function trackShopClick(input: OfferClickInput): void {
  capture(ANALYTICS_EVENTS.shopClicked, {
    release_id: input.releaseId,
    ean: input.ean ?? null,
    artist_name: input.artistName,
    release_title: input.releaseTitle,
    shop_id: input.shopId,
    shop_name: input.shopName,
    price: input.price,
    shipping_cost: input.shippingCost ?? null,
    total_price: input.totalPrice ?? null,
    offer_position: input.offerPosition,
    is_cheapest: input.isCheapest,
    source: input.source ?? "release_detail",
  });
}

export function trackPriceHistoryView(input: {
  releaseId: string;
  ean?: string | null;
  artistName: string;
  releaseTitle: string;
  source?: AnalyticsSource;
}): void {
  capture(ANALYTICS_EVENTS.priceHistoryViewed, {
    release_id: input.releaseId,
    ean: input.ean ?? null,
    artist_name: input.artistName,
    release_title: input.releaseTitle,
    source: input.source ?? "release_detail",
  });
}

export function trackFtgStart(input: {
  startArtistId: string;
  startArtistName: string;
  source?: AnalyticsSource;
}): void {
  capture(ANALYTICS_EVENTS.ftgStarted, {
    start_artist_id: input.startArtistId,
    start_artist_name: input.startArtistName,
    source: input.source ?? "ftg",
  });
}

export function trackFtgRelationClick(input: {
  startArtistId: string;
  startArtistName: string;
  fromArtistId: string;
  fromArtistName: string;
  toArtistId: string;
  toArtistName: string;
  relationType: FtgRelationType;
  depth: number;
}): void {
  capture(ANALYTICS_EVENTS.ftgRelationClicked, {
    start_artist_id: input.startArtistId,
    start_artist_name: input.startArtistName,
    from_artist_id: input.fromArtistId,
    from_artist_name: input.fromArtistName,
    to_artist_id: input.toArtistId,
    to_artist_name: input.toArtistName,
    relation_type: input.relationType,
    depth: input.depth,
  });
}

export function trackFtgArtistView(input: {
  startArtistId: string;
  startArtistName: string;
  artistId: string;
  artistName: string;
  depth: number;
}): void {
  capture(ANALYTICS_EVENTS.ftgArtistViewed, {
    start_artist_id: input.startArtistId,
    start_artist_name: input.startArtistName,
    artist_id: input.artistId,
    artist_name: input.artistName,
    depth: input.depth,
  });
}

export function trackFtgReleaseClick(input: {
  startArtistId: string;
  startArtistName: string;
  artistId: string;
  artistName: string;
  releaseId: string;
  releaseTitle: string;
  ean?: string | null;
  depth: number;
}): void {
  capture(ANALYTICS_EVENTS.ftgReleaseClicked, {
    start_artist_id: input.startArtistId,
    start_artist_name: input.startArtistName,
    artist_id: input.artistId,
    artist_name: input.artistName,
    release_id: input.releaseId,
    release_title: input.releaseTitle,
    ean: input.ean ?? null,
    depth: input.depth,
  });
}

export function trackCountryChange(fromCountry: string, toCountry: string): void {
  capture(ANALYTICS_EVENTS.countryChanged, {
    from_country: fromCountry,
    to_country: toCountry,
  });
}

export function trackSortChange(input: {
  pageType: string;
  sortFrom?: string;
  sortTo: string;
}): void {
  capture(ANALYTICS_EVENTS.sortChanged, {
    page_type: input.pageType,
    sort_from: input.sortFrom ?? null,
    sort_to: input.sortTo,
  });
}
