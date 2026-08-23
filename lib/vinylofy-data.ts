import { createSupabaseServerClient } from "@/lib/supabase/server";
import { enrichOffersWithShipping } from "@/lib/shipping";
import { getShippingRulesMap } from "@/lib/shipping-repository";
import { resolveCoverUrl } from "@/lib/cover-url";

type ProductRow = {
  id: string;
  ean: string | null;
  gtin_normalized: string | null;
  artist: string;
  title: string;
  format_label: string | null;
  cover_url: string | null;
  cover_storage_path: string | null;
  created_at: string;
};

type BestPriceRow = {
  product_id: string;
  lowest_fresh_price: number | string | null;
  fresh_instock_shop_count: number | null;
  total_active_shop_count: number | null;
  best_price_last_seen_at: string | null;
};

const DUTCH_VAT_RATE = 0.21;
const DUTCH_VAT_MULTIPLIER = 1 + DUTCH_VAT_RATE;
const VAT_INCLUSIVE_SHOP_DOMAIN = "atthemoviesshop.com";

type ShopRelation =
  | {
      name: string;
      domain: string;
    }
  | {
      name: string;
      domain: string;
    }[]
  | null;

type PriceRow = {
  product_id: string;
  shop_id: string;
  price: number | string;
  product_url: string;
  last_seen_at: string;
  availability: string | null;
  shops: ShopRelation;
};

export type HomeProduct = {
  id: string;
  ean: string | null;
  artist: string;
  title: string;
  formatLabel: string | null;
  coverUrl: string | null;
  coverStoragePath: string | null;
  lowestPrice: number | null;
  freshShopCount: number;
  totalShopCount: number;
  lastSeenAt: string | null;
};

export type SearchShopOffer = {
  name: string;
  domain: string;
  shopId: string;
  price: number;
  productUrl: string;
  lastSeenAt: string;
  availability: "in_stock" | "unknown";

  estimatedShippingPrice: number | null;
  estimatedTotalPrice: number | null;
  freeShippingApplied: boolean;
  shippingNote: string | null;
  shippingConfidence: string | null;
  freeShippingThresholdPrice: number | null;
};

export type SearchResultItem = {
  id: string;
  ean: string | null;
  artist: string;
  title: string;
  formatLabel: string | null;
  coverUrl: string | null;
  coverStoragePath: string | null;
  lowestPrice: number | null;
  foundIn: number;
  totalShops: number;
  lastSeenAt: string | null;
  freshnessLabel: string | null;
  shops: SearchShopOffer[];
};

export type ProductDetail = {
  id: string;
  ean: string | null;
  artist: string;
  title: string;
  formatLabel: string | null;
  coverUrl: string | null;
  coverStoragePath: string | null;
  lowestPrice: number | null;
  freshShopCount: number;
  totalShopCount: number;
  lastSeenAt: string | null;
  freshnessLabel: string | null;
  shops: SearchShopOffer[];
};


export type PriceHistoryWindow = "30d" | "90d" | "1y";

export type ProductPriceHistoryPoint = {
  day: string;
  price: number;
  shopCount: number;
  lastCapturedAt: string | null;
};

type PriceHistoryRow = {
  shop_id: string;
  price: number | string | null;
  availability: string | null;
  captured_at: string;
  shops: ShopRelation;
};

type RankedSearchResult = SearchResultItem & {
  _score: number;
};


const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function isUuidLike(value: string): boolean {
  return UUID_RE.test(value.trim());
}

function normalizeProductRouteKey(value: unknown): string | null {
  if (typeof value !== "string") return null;

  const trimmed = value.trim();
  if (!trimmed) return null;

  return trimmed;
}

function extractDigits(value: string): string {
  return value.replace(/\D/g, "");
}

function normalizeGtinLookup(value: string): string | null {
  const digits = extractDigits(value);

  if (![8, 12, 13, 14].includes(digits.length)) {
    return null;
  }

  return digits.padStart(14, "0");
}

const BLACKLISTED_FORMAT_LABELS = new Set([
  "CD",
  "POSTER",
  "ACCESSORIES",
  "PHOTOBOOK",
  "BLUERAY",
  "BLURAY"
]);

function normalizeFormatLabel(formatLabel: string | null | undefined): string {
  return (formatLabel ?? "").trim().toUpperCase();
}

function isBlacklistedFormat(formatLabel: string | null | undefined): boolean {
  return BLACKLISTED_FORMAT_LABELS.has(normalizeFormatLabel(formatLabel));
}

function isAllowedProduct(product: Pick<ProductRow, "format_label">): boolean {
  return !isBlacklistedFormat(product.format_label);
}

function preferProductIdentity(a: ProductRow, b: ProductRow): ProductRow {
  const aHasCanonicalGtin = Boolean(a.gtin_normalized);
  const bHasCanonicalGtin = Boolean(b.gtin_normalized);
  if (aHasCanonicalGtin !== bHasCanonicalGtin) {
    return aHasCanonicalGtin ? a : b;
  }

  const aHasFormat = Boolean(a.format_label?.trim());
  const bHasFormat = Boolean(b.format_label?.trim());
  if (aHasFormat !== bHasFormat) return aHasFormat ? a : b;

  // Keep the older identity stable when two rows are otherwise equivalent.
  return a.created_at <= b.created_at ? a : b;
}

function deduplicateProductIdentities(products: ProductRow[]): ProductRow[] {
  const byIdentity = new Map<string, ProductRow>();
  const withoutIdentity: ProductRow[] = [];

  for (const product of products) {
    const identity = normalizeGtinLookup(product.ean ?? "") ?? product.gtin_normalized;
    if (!identity) {
      withoutIdentity.push(product);
      continue;
    }

    const existing = byIdentity.get(identity);
    byIdentity.set(identity, existing ? preferProductIdentity(existing, product) : product);
  }

  return [...byIdentity.values(), ...withoutIdentity];
}

function toPublicCoverUrl(
  product: Pick<ProductRow, "cover_url" | "cover_storage_path">,
): string | null {
  return resolveCoverUrl({
    coverUrl: product.cover_url,
    storagePath: product.cover_storage_path,
  });
}

function toNumber(value: number | string | null | undefined): number | null {
  if (value === null || value === undefined) return null;
  const n = typeof value === "string" ? Number(value) : value;
  return Number.isFinite(n) ? n : null;
}

/** At The Movies stores its public listing price net; other shops are already gross. */
export function priceForDisplay(
  value: number | string | null | undefined,
  shopDomain?: string | null,
): number | null {
  const price = toNumber(value);
  if (price === null) return null;
  if (shopDomain !== VAT_INCLUSIVE_SHOP_DOMAIN) return price;

  return Math.round((price * DUTCH_VAT_MULTIPLIER + Number.EPSILON) * 100) / 100;
}

export function formatEuro(value: number | null | undefined): string {
  if (value === null || value === undefined) return "€--,--";

  return new Intl.NumberFormat("nl-NL", {
    style: "currency",
    currency: "EUR",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
}

function normalizeQuery(value: string): string {
  return value.toLowerCase().replace(/\s+/g, " ").trim();
}

function tokenize(value: string): string[] {
  return normalizeQuery(value)
    .split(" ")
    .map((token) => token.trim())
    .filter(Boolean);
}

export function getFreshnessLabel(iso: string | null | undefined): string | null {
  if (!iso) return null;

  const lastSeen = new Date(iso).getTime();
  if (Number.isNaN(lastSeen)) return null;

  const diffHours = (Date.now() - lastSeen) / (1000 * 60 * 60);
  if (diffHours < 24) return "vandaag gecontroleerd";
  if (diffHours < 48) return "1 dag oud";
  if (diffHours < 72) return "2 dagen oud";
  return "mogelijk niet actueel";
}

function amsterdamDay(iso: string): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Amsterdam",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date(iso));
}

function normalizeShopRelation(shops: ShopRelation): { name: string; domain: string } | null {
  if (!shops) return null;
  if (Array.isArray(shops)) return shops[0] ?? null;
  return shops;
}

function normalizeOfferAvailability(value: string | null | undefined): "in_stock" | "unknown" {
  return value === "in_stock" ? "in_stock" : "unknown";
}

function compareOffers(a: SearchShopOffer, b: SearchShopOffer): number {
  const aAvailabilityRank = a.availability === "in_stock" ? 0 : 1;
  const bAvailabilityRank = b.availability === "in_stock" ? 0 : 1;

  if (aAvailabilityRank !== bAvailabilityRank) {
    return aAvailabilityRank - bAvailabilityRank;
  }

  if (a.price !== b.price) return a.price - b.price;
  return b.lastSeenAt.localeCompare(a.lastSeenAt);
}

async function getProductsByIds(ids: string[]): Promise<ProductRow[]> {
  if (ids.length === 0) return [];

  const supabase = createSupabaseServerClient();
  const { data, error } = await supabase
    .from("products")
    .select("id, ean, gtin_normalized, artist, title, format_label, cover_url, cover_storage_path, created_at")
    .in("id", ids);

  if (error) throw error;
  return (data ?? []) as ProductRow[];
}

async function getBestPriceMap(productIds?: string[]) {
  const supabase = createSupabaseServerClient();

  let query = supabase
    .from("product_best_prices_v1")
    .select(
      "product_id, lowest_fresh_price, fresh_instock_shop_count, total_active_shop_count, best_price_last_seen_at",
    );

  if (productIds && productIds.length > 0) {
    query = query.in("product_id", productIds);
  }

  const { data, error } = await query;
  if (error) throw error;

  const map = new Map<string, BestPriceRow>();
  for (const row of (data ?? []) as BestPriceRow[]) {
    map.set(row.product_id, row);
  }

  return map;
}

async function getOffersMap(productIds: string[]) {
  if (productIds.length === 0) return new Map<string, SearchShopOffer[]>();

  const supabase = createSupabaseServerClient();
  const cutoff = new Date(Date.now() - 48 * 60 * 60 * 1000).toISOString();

  const { data, error } = await supabase
    .from("prices")
    .select("product_id, price, product_url, last_seen_at, availability, shop_id, shops(name, domain)")
    .in("product_id", productIds)
    .eq("is_active", true)
    .in("availability", ["in_stock", "unknown"])
    .gte("last_seen_at", cutoff)
    .order("price", { ascending: true })
    .order("last_seen_at", { ascending: false });

  if (error) throw error;

  const grouped = new Map<string, SearchShopOffer[]>();

  for (const row of (data ?? []) as PriceRow[]) {
    const shop = normalizeShopRelation(row.shops);
    if (!shop) continue;

    const offer: SearchShopOffer = {
      name: shop.name,
      domain: shop.domain,
      shopId: row.shop_id,
      price: priceForDisplay(row.price, shop.domain) ?? 0,
      productUrl: row.product_url,
      lastSeenAt: row.last_seen_at,
      availability: normalizeOfferAvailability(row.availability),
      estimatedShippingPrice: null,
      estimatedTotalPrice: null,
      freeShippingApplied: false,
      shippingNote: null,
      shippingConfidence: null,
      freeShippingThresholdPrice: null,
    };

    const existing = grouped.get(row.product_id) ?? [];
    existing.push(offer);
    grouped.set(row.product_id, existing);
  }

  const shippingRules = await getShippingRulesMap();

  for (const [productId, offers] of grouped.entries()) {
    grouped.set(
      productId,
      enrichOffersWithShipping(
        offers.sort(compareOffers),
        shippingRules,
      ),
    );
  }

  return grouped;
}

function scoreProductMatch(product: ProductRow, query: string, best: BestPriceRow | undefined): number {
  const normalizedQuery = normalizeQuery(query);
  const tokens = tokenize(query);
  const normalizedGtin = normalizeGtinLookup(query);

  const artist = normalizeQuery(product.artist);
  const title = normalizeQuery(product.title);
  const combined = `${artist} ${title}`.trim();

  let score = 0;

  if (normalizedGtin && product.gtin_normalized === normalizedGtin) score += 5000;
  if (combined === normalizedQuery) score += 1200;
  if (title === normalizedQuery) score += 1000;
  if (artist === normalizedQuery) score += 900;
  if (combined.startsWith(normalizedQuery)) score += 600;
  if (title.startsWith(normalizedQuery)) score += 500;
  if (artist.startsWith(normalizedQuery)) score += 450;
  if (combined.includes(normalizedQuery)) score += 250;
  if (title.includes(normalizedQuery)) score += 220;
  if (artist.includes(normalizedQuery)) score += 200;

  const allTokensMatch = tokens.length > 0 && tokens.every((token) => combined.includes(token));
  if (allTokensMatch) score += 180;

  const tokenHits = tokens.reduce((sum, token) => {
    if (title.includes(token)) return sum + 30;
    if (artist.includes(token)) return sum + 25;
    if (combined.includes(token)) return sum + 15;
    return sum;
  }, 0);
  score += tokenHits;

  const freshShops = best?.fresh_instock_shop_count ?? 0;
  const totalShops = best?.total_active_shop_count ?? 0;
  score += Math.min(freshShops, 5) * 20;
  score += Math.min(totalShops, 5) * 8;

  const lowestPrice = toNumber(best?.lowest_fresh_price);
  if (lowestPrice !== null) {
    score += 40;
    if (lowestPrice < 20) score += 15;
  }

  return score;
}

export async function getHomePageData(): Promise<{
  top25: HomeProduct[];
  newReleases: HomeProduct[];
}> {
  const supabase = createSupabaseServerClient();

  const { data: topRows, error: topError } = await supabase
    .from("product_best_prices_v1")
    .select(
      "product_id, lowest_fresh_price, fresh_instock_shop_count, total_active_shop_count, best_price_last_seen_at",
    )
    .gt("fresh_instock_shop_count", 0)
    .order("fresh_instock_shop_count", { ascending: false })
    .order("lowest_fresh_price", { ascending: true })
    .limit(100);

  if (topError) throw topError;

  const topBestRows = (topRows ?? []) as BestPriceRow[];
  const topIds = topBestRows.map((row) => row.product_id);
  const topProducts = (await getProductsByIds(topIds)).filter(isAllowedProduct);
  const topProductsMap = new Map(topProducts.map((row) => [row.id, row]));
  const topOffersMap = await getOffersMap(topIds);

  const top25: HomeProduct[] = topBestRows
    .map((row) => {
      const product = topProductsMap.get(row.product_id);
      if (!product) return null;

      return {
        id: product.id,
        ean: product.ean,
        artist: product.artist,
        title: product.title,
        formatLabel: product.format_label,
        coverUrl: toPublicCoverUrl(product),
  coverStoragePath: product.cover_storage_path,
        lowestPrice: topOffersMap.get(row.product_id)?.[0]?.price ?? toNumber(row.lowest_fresh_price),
        freshShopCount: row.fresh_instock_shop_count ?? 0,
        totalShopCount: row.total_active_shop_count ?? 0,
        lastSeenAt: row.best_price_last_seen_at,
      };
    })
    .filter((item): item is HomeProduct => Boolean(item && item.lowestPrice !== null))
    .slice(0, 25);

  const { data: latestProductsData, error: latestProductsError } = await supabase
    .from("products")
    .select("id, ean, gtin_normalized, artist, title, format_label, cover_url, cover_storage_path, created_at")
    .order("created_at", { ascending: false })
    .limit(50);

  if (latestProductsError) throw latestProductsError;

  const latestProducts = ((latestProductsData ?? []) as ProductRow[]).filter(isAllowedProduct);
  const latestIds = latestProducts.map((row) => row.id);
  const latestBestMap = await getBestPriceMap(latestIds);
  const latestOffersMap = await getOffersMap(latestIds);

  const newReleases: HomeProduct[] = latestProducts
    .map((product) => {
      const best = latestBestMap.get(product.id);

      return {
        id: product.id,
        ean: product.ean,
        artist: product.artist,
        title: product.title,
        formatLabel: product.format_label,
        coverUrl: toPublicCoverUrl(product),
  coverStoragePath: product.cover_storage_path,
        lowestPrice: latestOffersMap.get(product.id)?.[0]?.price ?? toNumber(best?.lowest_fresh_price),
        freshShopCount: best?.fresh_instock_shop_count ?? 0,
        totalShopCount: best?.total_active_shop_count ?? 0,
        lastSeenAt: best?.best_price_last_seen_at ?? null,
      };
    })
    .filter((item) => item.lowestPrice !== null && item.freshShopCount > 0)
    .slice(0, 8);

  return { top25, newReleases };
}


async function resolveProductRowByRouteKey(routeKey: unknown): Promise<ProductRow | null> {
  const key = normalizeProductRouteKey(routeKey);
  if (!key) return null;

  const supabase = createSupabaseServerClient();
  const normalizedGtin = normalizeGtinLookup(key);

  if (isUuidLike(key)) {
    const { data, error } = await supabase
      .from("products")
      .select("id, ean, gtin_normalized, artist, title, format_label, cover_url, cover_storage_path, created_at")
      .eq("id", key)
      .maybeSingle();

    if (error) throw error;
    return (data as ProductRow | null) ?? null;
  }

  if (normalizedGtin) {
    const { data, error } = await supabase
      .from("products")
      .select("id, ean, gtin_normalized, artist, title, format_label, cover_url, cover_storage_path, created_at")
      .eq("gtin_normalized", normalizedGtin)
      .maybeSingle();

    if (error) throw error;
    return (data as ProductRow | null) ?? null;
  }

  return null;
}

export async function getProductDetail(id: unknown): Promise<ProductDetail | null> {
  const product = await resolveProductRowByRouteKey(id);
  if (!product) return null;
  if (!isAllowedProduct(product)) return null;

  const bestMap = await getBestPriceMap([product.id]);
  const offersMap = await getOffersMap([product.id]);
  const best = bestMap.get(product.id);
  const offers = offersMap.get(product.id) ?? [];
  const lowestPrice = offers[0]?.price ?? toNumber(best?.lowest_fresh_price) ?? null;
  const freshShopCount = offers.length > 0 ? offers.length : (best?.fresh_instock_shop_count ?? 0);
  const totalShopCount = Math.max(best?.total_active_shop_count ?? 0, offers.length);
  const lastSeenAt = offers[0]?.lastSeenAt ?? best?.best_price_last_seen_at ?? null;

  return {
    id: product.id,
    ean: product.ean,
    artist: product.artist,
    title: product.title,
    formatLabel: product.format_label,
    coverUrl: toPublicCoverUrl(product),
    coverStoragePath: product.cover_storage_path,
    lowestPrice,
    freshShopCount,
    totalShopCount,
    lastSeenAt,
    freshnessLabel: getFreshnessLabel(lastSeenAt),
    shops: offers,
  };
}

export async function searchProducts(
  query: string,
  options: { limit?: number | null } = {},
): Promise<SearchResultItem[]> {
  const normalizedQuery = query.trim();
  if (!normalizedQuery) return [];

  const supabase = createSupabaseServerClient();
  const normalizedDigits = normalizeGtinLookup(normalizedQuery);
  const candidates = new Map<string, ProductRow>();

  const baseSelect = "id, ean, gtin_normalized, artist, title, format_label, cover_url, cover_storage_path, created_at";

  async function collect(promise: PromiseLike<{ data: unknown; error: unknown }>) {
    const result = (await promise) as { data: unknown; error: unknown };
    if (result.error) throw result.error;

    for (const row of (result.data ?? []) as ProductRow[]) {
      candidates.set(row.id, row);
    }
  }

  if (normalizedDigits) {
    await collect(
      supabase
        .from("products")
        .select(baseSelect)
        .eq("gtin_normalized", normalizedDigits)
        .limit(10),
    );
  }

  await collect(supabase.from("products").select(baseSelect).ilike("artist", `%${normalizedQuery}%`));
  await collect(supabase.from("products").select(baseSelect).ilike("title", `%${normalizedQuery}%`));
  await collect(
    supabase
      .from("products")
      .select(baseSelect)
      .ilike("search_text", `%${normalizeQuery(normalizedQuery)}%`),
  );

  const productList = deduplicateProductIdentities(
    Array.from(candidates.values()).filter(isAllowedProduct),
  );
  if (productList.length === 0) return [];

  const ids = productList.map((row) => row.id);
  const bestMap = await getBestPriceMap(ids);
  const offersMap = await getOffersMap(ids);

  const ranked: RankedSearchResult[] = productList
    .map((product) => {
      const best = bestMap.get(product.id);
      const offers = offersMap.get(product.id) ?? [];
      const lowestPrice = offers[0]?.price ?? toNumber(best?.lowest_fresh_price) ?? null;
      const freshShopCount = offers.length > 0 ? offers.length : (best?.fresh_instock_shop_count ?? 0);
      const totalShopCount = Math.max(best?.total_active_shop_count ?? 0, offers.length);
      const lastSeenAt = offers[0]?.lastSeenAt ?? best?.best_price_last_seen_at ?? null;

      return {
        id: product.id,
        ean: product.ean,
        artist: product.artist,
        title: product.title,
        formatLabel: product.format_label,
        coverUrl: toPublicCoverUrl(product),
  coverStoragePath: product.cover_storage_path,
        lowestPrice,
        foundIn: freshShopCount,
        totalShops: totalShopCount,
        lastSeenAt,
        freshnessLabel: getFreshnessLabel(lastSeenAt),
        shops: offers,
        _score: scoreProductMatch(product, normalizedQuery, best),
      };
    })
    .filter((item) => item.lowestPrice !== null || item.shops.length > 0)
    .filter((item) => item._score > 0)
    .sort((a, b) => {
      if (b._score !== a._score) return b._score - a._score;

      const aPrice = a.lowestPrice ?? Number.MAX_SAFE_INTEGER;
      const bPrice = b.lowestPrice ?? Number.MAX_SAFE_INTEGER;
      if (aPrice !== bPrice) return aPrice - bPrice;

      if (b.foundIn !== a.foundIn) return b.foundIn - a.foundIn;

      return a.artist.localeCompare(b.artist);
    });

  const results = ranked.map((item) => {
    const { _score, ...rest } = item;
    void _score;
    return rest;
  });

  if (options.limit === null) {
    return results;
  }

  const limit = Math.max(0, options.limit ?? 24);
  return results.slice(0, limit);
}

export async function getProductPriceHistory(
  productId: string,
  maxDays = 30,
): Promise<ProductPriceHistoryPoint[]> {
  const supabase = createSupabaseServerClient();

  const cutoff = new Date();
  cutoff.setUTCHours(0, 0, 0, 0);
  cutoff.setUTCDate(cutoff.getUTCDate() - Math.max(1, maxDays - 1));
  const cutoffDay = cutoff.toISOString().slice(0, 10);

  const { data, error } = await supabase
    .from("price_history")
    .select("shop_id, price, availability, captured_at, shops(domain)")
    .eq("product_id", productId)
    .gte("captured_at", cutoff.toISOString())
    .order("captured_at", { ascending: false });

  if (error) {
    console.warn("[vinylofy] product price history unavailable", {
      productId,
      code: (error as { code?: string }).code,
      message: (error as { message?: string }).message,
      hint: (error as { hint?: string | null }).hint ?? null,
    });

    return [];
  }

  const daily = new Map<string, {
    price: number;
    shopIds: Set<string>;
    lastCapturedAt: string;
  }>();

  for (const row of (data ?? []) as PriceHistoryRow[]) {
    if (row.availability !== "in_stock") continue;

    const shop = normalizeShopRelation(row.shops);
    const price = priceForDisplay(row.price, shop?.domain);
    if (price === null) continue;

    const day = amsterdamDay(row.captured_at);
    const existing = daily.get(day);
    if (!existing) {
      daily.set(day, {
        price,
        shopIds: new Set([row.shop_id]),
        lastCapturedAt: row.captured_at,
      });
      continue;
    }

    existing.price = Math.min(existing.price, price);
    existing.shopIds.add(row.shop_id);
    if (row.captured_at > existing.lastCapturedAt) {
      existing.lastCapturedAt = row.captured_at;
    }
  }

  return Array.from(daily.entries())
    .filter(([day]) => day >= cutoffDay)
    .map(([day, value]) => ({
      day,
      price: value.price,
      shopCount: value.shopIds.size,
      lastCapturedAt: value.lastCapturedAt,
    }))
    .sort((a, b) => a.day.localeCompare(b.day));
}


export type TopDealItem = {
  id: string;
  ean: string | null;
  artist: string;
  title: string;
  formatLabel: string | null;
  coverUrl: string | null;
  coverStoragePath: string | null;
  lowestPrice: number;
  highestPrice: number;
  priceDifference: number;
  shopCount: number;
  lowestOffer: SearchShopOffer;
  highestOffer: SearchShopOffer;
  offers: SearchShopOffer[];
  lastSeenAt: string | null;
};

type TopDealsSnapshotRow = {
  rank: number;
  product_id: string;
  ean: string | null;
  artist: string;
  title: string;
  format_label: string | null;
  cover_url: string | null;
  lowest_price: number | string;
  highest_price: number | string;
  price_difference: number | string;
  shop_count: number;
  lowest_offer: unknown;
  highest_offer: unknown;
  offers: unknown;
  last_seen_at: string | null;
};

function isSnapshotOffer(value: unknown): value is SearchShopOffer {
  if (!value || typeof value !== "object") return false;

  const offer = value as Partial<SearchShopOffer>;

  return (
    typeof offer.name === "string" &&
    typeof offer.domain === "string" &&
    typeof offer.shopId === "string" &&
    typeof offer.price === "number" &&
    typeof offer.productUrl === "string" &&
    typeof offer.lastSeenAt === "string" &&
    (offer.availability === "in_stock" || offer.availability === "unknown")
  );
}

function normalizeSnapshotOffer(value: unknown): SearchShopOffer | null {
  if (!value || typeof value !== "object") return null;

  const raw = value as Record<string, unknown>;
  const shopId = typeof raw.shopId === "string" ? raw.shopId : "";
  const shopDomain = typeof raw.domain === "string" ? raw.domain : null;
  const price = priceForDisplay(raw.price as number | string | null | undefined, shopDomain);
  const availability = normalizeOfferAvailability(
    typeof raw.availability === "string" ? raw.availability : null,
  );

  const offer: SearchShopOffer = {
    name: typeof raw.name === "string" ? raw.name : "",
    domain: typeof raw.domain === "string" ? raw.domain : "",
    shopId,
    price: price ?? 0,
    productUrl: typeof raw.productUrl === "string" ? raw.productUrl : "",
    lastSeenAt: typeof raw.lastSeenAt === "string" ? raw.lastSeenAt : "",
    availability,
    estimatedShippingPrice: null,
    estimatedTotalPrice: null,
    freeShippingApplied: false,
    shippingNote: null,
    shippingConfidence: null,
    freeShippingThresholdPrice: null,
  };

  return isSnapshotOffer(offer) ? offer : null;
}

export async function getTopDeals(limit = 45): Promise<TopDealItem[]> {
  const supabase = createSupabaseServerClient();
  const safeLimit = Math.max(1, Math.min(limit, 45));

  const { data, error } = await supabase
    .from("top_deals_snapshot")
    .select(
      "rank, product_id, ean, artist, title, format_label, cover_url, lowest_price, highest_price, price_difference, shop_count, lowest_offer, highest_offer, offers, last_seen_at",
    )
    .eq("snapshot_key", "current")
    .order("rank", { ascending: true })
    .limit(safeLimit);

  if (error) throw error;

  const rows = (data ?? []) as TopDealsSnapshotRow[];
  const productIds = Array.from(
    new Set(rows.map((row) => row.product_id)),
  );
  const products = await getProductsByIds(productIds);
  const coverStoragePathMap = new Map(
    products.map((product) => [
      product.id,
      product.cover_storage_path,
    ]),
  );

  return rows.flatMap((row) => {
    const offers = Array.isArray(row.offers)
      ? row.offers.flatMap((offer) => {
          const normalized = normalizeSnapshotOffer(offer);
          return normalized ? [normalized] : [];
        })
      : [];
    const lowestOffer = offers.reduce<SearchShopOffer | null>(
      (current, offer) => (current === null || offer.price < current.price ? offer : current),
      null,
    );
    const highestOffer = offers.reduce<SearchShopOffer | null>(
      (current, offer) => (current === null || offer.price > current.price ? offer : current),
      null,
    );
    const lowestPrice = lowestOffer?.price ?? null;
    const highestPrice = highestOffer?.price ?? null;
    const priceDifference =
      lowestPrice !== null && highestPrice !== null
        ? Math.round((highestPrice - lowestPrice + Number.EPSILON) * 100) / 100
        : null;

    if (
      lowestPrice === null ||
      highestPrice === null ||
      priceDifference === null ||
      !lowestOffer ||
      !highestOffer ||
      offers.length < 2
    ) {
      return [];
    }

    return [
      {
        id: row.product_id,
        ean: row.ean,
        artist: row.artist,
        title: row.title,
        formatLabel: row.format_label,
        coverUrl: row.cover_url,
        coverStoragePath: coverStoragePathMap.get(row.product_id) ?? null,
        lowestPrice,
        highestPrice,
        priceDifference,
        shopCount: row.shop_count,
        lowestOffer,
        highestOffer,
        offers,
        lastSeenAt: row.last_seen_at,
      },
    ];
  });
}

export type ReleaseCalendarItem = {
  id: string;
  ean: string | null;
  artist: string;
  title: string;
  releaseDate: string;
  sourceShop: string;
  sourceUrl: string;
  imageUrl: string | null;
  imageStoragePath: string | null;
  format: string | null;
  label: string | null;
  productId: string | null;
  lowestPrice: number | null;
};

type ReleaseCalendarRow = {
  id: string;
  ean: string | null;
  artist: string;
  title: string;
  release_date: string;
  source_shop: string;
  source_url: string;
  format: string | null;
  label: string | null;
  product_id: string | null;
};

export async function getReleaseCalendarItems(limit = 120): Promise<ReleaseCalendarItem[]> {
  const supabase = createSupabaseServerClient();

  const minDateValue = new Date();
  minDateValue.setUTCDate(minDateValue.getUTCDate() - 14);
  const minDate = minDateValue.toISOString().slice(0, 10);

  const maxDateValue = new Date();
  maxDateValue.setUTCDate(maxDateValue.getUTCDate() + 14);
  const maxDate = maxDateValue.toISOString().slice(0, 10);

  /*
   * Haal eerst alle actieve releasevermeldingen binnen het datumvenster op.
   * De gebruikerslimiet wordt pas toegepast nadat het aantal actuele shops
   * is gecontroleerd.
   */
  const pageSize = 1000;
  const rows: ReleaseCalendarRow[] = [];

  for (let offset = 0; ; offset += pageSize) {
    const { data, error } = await supabase
      .from("release_calendar")
      .select(
        "id, ean, artist, title, release_date, source_shop, source_url, format, label, product_id",
      )
      .eq("status", "active")
      .gte("release_date", minDate)
      .lte("release_date", maxDate)
      .order("release_date", { ascending: true })
      .order("artist", { ascending: true })
      .range(offset, offset + pageSize - 1);

    if (error) throw error;

    const batch = (data ?? []) as ReleaseCalendarRow[];
    rows.push(...batch);

    if (batch.length < pageSize) break;
  }

  const productIds = Array.from(
    new Set(
      rows
        .map((row) => row.product_id)
        .filter((id): id is string => Boolean(id)),
    ),
  );

  /*
   * Product-ID’s worden in beperkte batches opgehaald om een te grote
   * Supabase IN-query te voorkomen.
   */
  const bestPriceMap = new Map<string, BestPriceRow>();
  const productMap = new Map<string, ProductRow>();
  const productBatchSize = 200;

  for (let offset = 0; offset < productIds.length; offset += productBatchSize) {
    const batchProductIds = productIds.slice(
      offset,
      offset + productBatchSize,
    );
    const batchMap = await getBestPriceMap(batchProductIds);
    const batchProducts = await getProductsByIds(batchProductIds);

    for (const [productId, bestPrice] of batchMap) {
      bestPriceMap.set(productId, bestPrice);
    }
    for (const product of batchProducts) {
      productMap.set(product.id, product);
    }
  }

  const releaseOffersMap = await getOffersMap(Array.from(productIds));

  const seenProductIds = new Set<string>();
  const safeLimit = Math.max(0, Math.floor(limit));

  return rows
    .filter((row) => {
      if (!row.product_id) return false;
      if (seenProductIds.has(row.product_id)) return false;

      const bestPrice = bestPriceMap.get(row.product_id);
      const freshShopCount = bestPrice?.fresh_instock_shop_count ?? 0;

      if (freshShopCount < 2) return false;

      seenProductIds.add(row.product_id);
      return true;
    })
    .slice(0, safeLimit)
    .map((row) => {
      const bestPrice = row.product_id
        ? bestPriceMap.get(row.product_id)
        : undefined;
      const product = row.product_id
        ? productMap.get(row.product_id)
        : undefined;

      return {
        id: row.id,
        ean: row.ean,
        artist: row.artist,
        title: row.title,
        releaseDate: row.release_date,
        sourceShop: row.source_shop,
        sourceUrl: row.source_url,
        imageUrl: product?.cover_url ?? null,
        imageStoragePath: product?.cover_storage_path ?? null,
        format: row.format,
        label: row.label,
        productId: row.product_id,
        lowestPrice:
          releaseOffersMap.get(row.product_id ?? "")?.[0]?.price ??
          toNumber(bestPrice?.lowest_fresh_price),
      };
    });
}
