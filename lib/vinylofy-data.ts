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

type PriceRow = {
  product_id: string;
  price: number | string;
  product_url: string;
  last_seen_at: string;
  shops:
    | {
        name: string;
        domain: string;
      }
    | {
        name: string;
        domain: string;
      }[]
    | null;
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
  lastSeenAt: string | null;
  freshnessLabel: string | null;
  shops: SearchShopOffer[];
};

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
  }).format(value);
}

export function getFreshnessLabel(iso: string | null | undefined): string | null {
  if (!iso) return null;

  const lastSeen = new Date(iso).getTime();
  const diffHours = (Date.now() - lastSeen) / (1000 * 60 * 60);

  if (diffHours < 24) return "vandaag gecontroleerd";
  if (diffHours < 48) return "1 dag oud";
  if (diffHours < 72) return "2 dagen oud";
  return "mogelijk niet actueel";
}

function normalizeShopRelation(
  shops: PriceRow["shops"]
): { name: string; domain: string } | null {
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
      "product_id, lowest_fresh_price, fresh_instock_shop_count, total_active_shop_count, best_price_last_seen_at"
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
    grouped.set(productId, offers.slice(0, 5));
  }

  return grouped;
}

export async function getHomePageData(): Promise<{
  top25: HomeProduct[];
  newReleases: HomeProduct[];
}> {
  const supabase = createSupabaseServerClient();

  const { data: topRows, error: topError } = await supabase
    .from("product_best_prices_v1")
    .select(
      "product_id, lowest_fresh_price, fresh_instock_shop_count, total_active_shop_count, best_price_last_seen_at"
    )
    .gt("fresh_instock_shop_count", 0)
    .order("fresh_instock_shop_count", { ascending: false })
    .order("lowest_fresh_price", { ascending: true })
    .limit(25);

  if (topError) throw topError;

  const topBestRows = (topRows ?? []) as BestPriceRow[];
  const topIds = topBestRows.map((row) => row.product_id);
  const topProducts = await getProductsByIds(topIds);

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
        lastSeenAt: row.best_price_last_seen_at,
      };
    })
    .filter(Boolean) as HomeProduct[];

  const { data: latestProductsData, error: latestProductsError } = await supabase
    .from("products")
    .select("id, ean, artist, title, format_label, cover_url, created_at")
    .order("created_at", { ascending: false })
    .limit(12);

  if (latestProductsError) throw latestProductsError;

  const latestProducts = (latestProductsData ?? []) as ProductRow[];
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
        lastSeenAt: best?.best_price_last_seen_at ?? null,
      };
    })
    .filter((item) => item.lowestPrice !== null)
    .slice(0, 8);

  return { top25, newReleases };
}

export async function searchProducts(query: string): Promise<SearchResultItem[]> {
  const normalizedQuery = query.trim();
  if (!normalizedQuery) return [];

  const supabase = createSupabaseServerClient();
  const lowerQuery = normalizedQuery.toLowerCase();
  const digits = normalizedQuery.replace(/\D/g, "");

  const candidates = new Map<string, ProductRow>();

  async function collect(
    builder: ReturnType<typeof supabase.from<"products">>
  ) {
    const result = await builder;
    if (result.error) throw result.error;
    for (const row of (result.data ?? []) as ProductRow[]) {
      candidates.set(row.id, row);
    }
  }

  const baseSelect = "id, ean, artist, title, format_label, cover_url, created_at";

  if (/^\d+$/.test(digits) && [8, 12, 13, 14].includes(digits.length)) {
    await collect(
      supabase.from("products").select(baseSelect).eq("ean", digits).limit(10)
    );
  }

  await collect(
    supabase
      .from("products")
      .select(baseSelect)
      .ilike("artist", `%${normalizedQuery}%`)
      .limit(20)
  );

  await collect(
    supabase
      .from("products")
      .select(baseSelect)
      .ilike("title", `%${normalizedQuery}%`)
      .limit(20)
  );

  await collect(
    supabase
      .from("products")
      .select(baseSelect)
      .ilike("search_text", `%${lowerQuery}%`)
      .limit(20)
  );

  const productList = Array.from(candidates.values());
  if (productList.length === 0) return [];

  const ids = productList.map((row) => row.id);
  const bestMap = await getBestPriceMap(ids);
  const offersMap = await getOffersMap(ids);

  const results: SearchResultItem[] = productList
    .map((product) => {
      const best = bestMap.get(product.id);
      const offers = offersMap.get(product.id) ?? [];
      const lowestPrice = toNumber(best?.lowest_fresh_price);
      const haystack = `${product.artist} ${product.title}`.toLowerCase();

      let score = 0;
      if (digits && product.ean === digits) score += 1000;
      if (haystack === lowerQuery) score += 500;
      if (haystack.includes(lowerQuery)) score += 300;
      if (product.artist.toLowerCase().includes(lowerQuery)) score += 120;
      if (product.title.toLowerCase().includes(lowerQuery)) score += 120;
      if (best?.fresh_instock_shop_count) score += Math.min(best.fresh_instock_shop_count, 5) * 10;

      return {
        id: product.id,
        ean: product.ean,
        artist: product.artist,
        title: product.title,
        formatLabel: product.format_label,
        coverUrl: product.cover_url,
        lowestPrice,
        foundIn: best?.fresh_instock_shop_count ?? offers.length,
        lastSeenAt: best?.best_price_last_seen_at ?? offers[0]?.lastSeenAt ?? null,
        freshnessLabel: getFreshnessLabel(
          best?.best_price_last_seen_at ?? offers[0]?.lastSeenAt ?? null
        ),
        shops: offers,
        _score: score,
      };
    })
    .filter((item) => item.lowestPrice !== null || item.shops.length > 0)
    .sort((a, b) => {
      if (b._score !== a._score) return b._score - a._score;

      const aPrice = a.lowestPrice ?? Number.MAX_SAFE_INTEGER;
      const bPrice = b.lowestPrice ?? Number.MAX_SAFE_INTEGER;
      if (aPrice !== bPrice) return aPrice - bPrice;

      return a.artist.localeCompare(b.artist);
    })
    .slice(0, 24)
    .map(({ _score, ...rest }) => rest);

  return results;
}