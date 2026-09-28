import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import { ShopTypeBadge } from "@/components/shop-type-badge";
import { getShopHighlights, type ShopHighlights } from "@/lib/shop-highlights";
import { createSupabaseServerClient } from "@/lib/supabase/server";
import { isMissingShopTypeColumn, type ShopType } from "@/lib/shop-type";

type ShopRow = {
  id: string;
  name: string;
  domain: string;
  country: string;
  is_active: boolean;
  shop_type?: ShopType | null;
};

const EXCLUDED_PUBLIC_SHOP_DOMAINS = new Set([
  "bol.com",
  "groovespin.nl",
  "hhv.de",
  "junorecords.com",
  "soundsdelft.nl",
]);

export const dynamic = "force-dynamic";

export default async function ShopsPage() {
  const supabase = createSupabaseServerClient();

  const fetchShops = (columns: string) =>
    supabase.from("shops").select(columns).order("name", { ascending: true });

  let result = await fetchShops("id, name, domain, country, is_active, shop_type");
  if (isMissingShopTypeColumn(result.error)) {
    result = await fetchShops("id, name, domain, country, is_active");
  }

  const { data, error } = result;

  if (error) {
    throw error;
  }

  const shops = ((data ?? []) as unknown as ShopRow[]).filter(
    (shop) =>
      shop.is_active &&
      !EXCLUDED_PUBLIC_SHOP_DOMAINS.has(shop.domain.trim().toLowerCase()),
  );

  const highlights = shops.length > 0
    ? await getShopHighlights()
    : new Map<string, ShopHighlights>();

  return (
    <div className="min-h-screen bg-white text-neutral-900">
      <SiteHeader />

      <main className="mx-auto max-w-5xl px-6 py-12">
        <div className="mb-8">
          <p className="text-sm font-medium uppercase tracking-[0.14em] text-orange-600">
            Shops
          </p>

          <h1 className="mt-4 text-3xl font-semibold tracking-tight md:text-4xl">
            Winkels in de prijsvergelijking
          </h1>

          <p className="mt-4 max-w-2xl text-neutral-600">
            Vinylofy vergelijkt het actuele aanbod van onderstaande winkels.
            Vermelding betekent niet dat er sprake is van een commerciële
            samenwerking of partnerschap. Het winkelaanbod binnen Vinylofy kan
            in de loop van de tijd veranderen en wordt regelmatig bijgewerkt.
          </p>
        </div>

        {shops.length === 0 ? (
          <div className="rounded-2xl border border-neutral-200 bg-white p-6 text-sm text-neutral-500">
            Er zijn nog geen shops beschikbaar.
          </div>
        ) : (
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
            {shops.map((shop) => {
              const shopHighlights = highlights.get(shop.id);

              return (
                <article
                  key={shop.id}
                  className="rounded-2xl border border-neutral-200 bg-white p-5"
                >
                  <div className="flex items-start justify-between gap-4">
                    <div className="min-w-0">
                      <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1">
                        <h2 className="min-w-0 break-words text-lg font-semibold">{shop.name}</h2>
                        <ShopTypeBadge type={shop.shop_type} />
                      </div>
                      <p className="mt-1 text-sm text-neutral-500">{shop.domain}</p>
                    </div>

                    <span className="inline-flex shrink-0 rounded-full bg-orange-50 px-3 py-1 text-xs font-medium text-orange-700">
                      actief
                    </span>
                  </div>

                  {(shopHighlights?.lowestPriceCount ?? 0) > 0 ||
                  (shopHighlights?.onlyHereCount ?? 0) > 0 ? (
                    <div className="mt-3 space-y-1 text-sm text-neutral-600">
                      {(shopHighlights?.lowestPriceCount ?? 0) > 0 && (
                        <p>🏷️ <span className="font-medium tabular-nums">{shopHighlights?.lowestPriceCount}×</span> laagste prijs</p>
                      )}
                      {(shopHighlights?.onlyHereCount ?? 0) > 0 && (
                        <p>💎 <span className="font-medium tabular-nums">{shopHighlights?.onlyHereCount}×</span> alleen hier op Vinylofy</p>
                      )}
                    </div>
                  ) : null}

                  <p className="mt-3 text-sm text-neutral-500">
                    Land: {shop.country}
                  </p>
                </article>
              );
            })}
          </div>
        )}
      </main>

      <SiteFooter />
    </div>
  );
}
