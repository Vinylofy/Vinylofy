import { createSupabaseServerClient } from "@/lib/supabase/server";

type ProductRow = {
  id: string;
  ean: string | null;
  artist: string;
  title: string;
  format_label: string | null;
  cover_url: string | null;
  created_at: string;
};

type BestPriceRow = {
  product_id: string;
  lowest_fresh_price: number | string | null;
  fresh_instock_shop_count: number | null;
  total_active_shop_count: number | null;
  best_price_last_seen_at: string | null;
};

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
  price: number | string;
  product_url: string;
  last_seen_at: string;
  shops: ShopRelation;
};

export type HomeProduct = {
  id: string;
  ean: string | null;
  artist: string;
  title: string;
  formatLabel: string | null;
  coverUrl: string | null;
  lowestPrice: number | null;
  freshShopCount: number;
  totalShopCount: number;
  lastSeenAt: string | null;
};

export type SearchShopOffer = {
  name: string;
  domain: string;
  price: number;
  productUrl: string;
  lastSeenAt: string;
};

export type SearchResultItem = {
  id: string;
  ean: string | null;
  artist: string;
  title: string;
  formatLabel: string | null;
  coverUrl: string | null;
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
  lowestPrice: number | null;
  freshShopCount: number;
  totalShopCount: number;
  lastSeenAt: string | null;
  freshnessLabel: string | null;
  shops: SearchShopOffer[];
};

type RankedSearchResult = SearchResultItem & {
  _score: number;
};

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

function toNumber(value: number | string | null | undefined): number | null {
  if (value === null || value === undefined) return null;
  const n = typeof value === "string" ? Number(value) : value;
  return Number.isFinite(n) ? n : null;
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

function normalizeShopRelation(shops: ShopRelation): { name: string; domain: string } | null {
  if (!shops) return null;
  if (Array.isArray(shops)) return shops[0] ?? null;
  return shops;
}

async function getProductsByIds(ids: string[]): Promise<ProductRow[]> {
  if (ids.length === 0) return [];

  const supabase = createSupabaseServerClient();
  const { data, error } = await supabase
    .from("products")
    .select("id, ean, artist, title, format_label, cover_url, created_at")
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
    .select("product_id, price, product_url, last_seen_at, shops(name, domain)")
    .in("product_id", productIds)
    .eq("is_active", true)
    .eq("availability", "in_stock")
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
      price: toNumber(row.price) ?? 0,
      productUrl: row.product_url,
      lastSeenAt: row.last_seen_at,
    };

    const existing = grouped.get(row.product_id) ?? [];
    existing.push(offer);
    grouped.set(row.product_id, existing);
  }

  for (const [productId, offers] of grouped.entries()) {
    const deduped = offers.sort((a, b) => {
      if (a.price !== b.price) return a.price - b.price;
      return b.lastSeenAt.localeCompare(a.lastSeenAt);
    });

    grouped.set(productId, deduped);
  }

  return grouped;
}

function scoreProductMatch(product: ProductRow, query: string, best: BestPriceRow | undefined): number {
  const normalizedQuery = normalizeQuery(query);
  const tokens = tokenize(query);
  const digits = query.replace(/\D/g, "");

  const artist = normalizeQuery(product.artist);
  const title = normalizeQuery(product.title);
  const combined = `${artist} ${title}`.trim();

  let score = 0;

  if (digits && product.ean === digits) score += 5000;
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
        coverUrl: product.cover_url,
        lowestPrice: toNumber(row.lowest_fresh_price),
        freshShopCount: row.fresh_instock_shop_count ?? 0,
        totalShopCount: row.total_active_shop_count ?? 0,
        lastSeenAt: row.best_price_last_seen_at,
      };
    })
    .filter((item): item is HomeProduct => Boolean(item && item.lowestPrice !== null))
    .slice(0, 25);

  const { data: latestProductsData, error: latestProductsError } = await supabase
    .from("products")
    .select("id, ean, artist, title, format_label, cover_url, created_at")
    .order("created_at", { ascending: false })
    .limit(50);

  if (latestProductsError) throw latestProductsError;

  const latestProducts = ((latestProductsData ?? []) as ProductRow[]).filter(isAllowedProduct);
  const latestIds = latestProducts.map((row) => row.id);
  const latestBestMap = await getBestPriceMap(latestIds);

  const newReleases: HomeProduct[] = latestProducts
    .map((product) => {
      const best = latestBestMap.get(product.id);

      return {
        id: product.id,
        ean: product.ean,
        artist: product.artist,
        title: product.title,
        formatLabel: product.format_label,
        coverUrl: product.cover_url,
        lowestPrice: toNumber(best?.lowest_fresh_price),
        freshShopCount: best?.fresh_instock_shop_count ?? 0,
        totalShopCount: best?.total_active_shop_count ?? 0,
        lastSeenAt: best?.best_price_last_seen_at ?? null,
      };
    })
    .filter((item) => item.lowestPrice !== null && item.freshShopCount > 0)
    .slice(0, 8);

  return { top25, newReleases };
}

export async function getProductDetail(id: string): Promise<ProductDetail | null> {
  const supabase = createSupabaseServerClient();

  const { data, error } = await supabase
    .from("products")
    .select("id, ean, artist, title, format_label, cover_url, created_at")
    .eq("id", id)
    .maybeSingle();

  if (error) throw error;
  if (!data) return null;

  const product = data as ProductRow;
  if (!isAllowedProduct(product)) return null;

  const bestMap = await getBestPriceMap([product.id]);
  const offersMap = await getOffersMap([product.id]);
  const best = bestMap.get(product.id);
  const offers = offersMap.get(product.id) ?? [];
  const lowestPrice = toNumber(best?.lowest_fresh_price) ?? (offers[0]?.price ?? null);
  const freshShopCount = best?.fresh_instock_shop_count ?? offers.length;
  const totalShopCount = best?.total_active_shop_count ?? offers.length;
  const lastSeenAt = best?.best_price_last_seen_at ?? offers[0]?.lastSeenAt ?? null;

  return {
    id: product.id,
    ean: product.ean,
    artist: product.artist,
    title: product.title,
    formatLabel: product.format_label,
    coverUrl: product.cover_url,
    lowestPrice,
    freshShopCount,
    totalShopCount,
    lastSeenAt,
    freshnessLabel: getFreshnessLabel(lastSeenAt),
    shops: offers,
  };
}

export async function searchProducts(query: string): Promise<SearchResultItem[]> {
  const normalizedQuery = query.trim();
  if (!normalizedQuery) return [];

  const supabase = createSupabaseServerClient();
  const digits = normalizedQuery.replace(/\D/g, "");
  const candidates = new Map<string, ProductRow>();

  const baseSelect = "id, ean, artist, title, format_label, cover_url, created_at";

  async function collect(promise: PromiseLike<{ data: unknown; error: unknown }>) {
    const result = (await promise) as { data: unknown; error: unknown };
    if (result.error) throw result.error;

    for (const row of (result.data ?? []) as ProductRow[]) {
      candidates.set(row.id, row);
    }
  }

  if (/^\d+$/.test(digits) && [8, 12, 13, 14].includes(digits.length)) {
    await collect(supabase.from("products").select(baseSelect).eq("ean", digits).limit(10));
  }

  await collect(supabase.from("products").select(baseSelect).ilike("artist", `%${normalizedQuery}%`).limit(20));
  await collect(supabase.from("products").select(baseSelect).ilike("title", `%${normalizedQuery}%`).limit(20));
  await collect(
    supabase
      .from("products")
      .select(baseSelect)
      .ilike("search_text", `%${normalizeQuery(normalizedQuery)}%`)
      .limit(30),
  );

  const productList = Array.from(candidates.values()).filter(isAllowedProduct);
  if (productList.length === 0) return [];

  const ids = productList.map((row) => row.id);
  const bestMap = await getBestPriceMap(ids);
  const offersMap = await getOffersMap(ids);

  const ranked: RankedSearchResult[] = productList
    .map((product) => {
      const best = bestMap.get(product.id);
      const offers = offersMap.get(product.id) ?? [];
      const lowestPrice = toNumber(best?.lowest_fresh_price) ?? (offers[0]?.price ?? null);
      const freshShopCount = best?.fresh_instock_shop_count ?? offers.length;
      const totalShopCount = best?.total_active_shop_count ?? offers.length;
      const lastSeenAt = best?.best_price_last_seen_at ?? offers[0]?.lastSeenAt ?? null;

      return {
        id: product.id,
        ean: product.ean,
        artist: product.artist,
        title: product.title,
        formatLabel: product.format_label,
        coverUrl: product.cover_url,
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

  return ranked.slice(0, 24).map(({ _score, ...rest }) => rest);
}
