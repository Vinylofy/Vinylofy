import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import { createSupabaseServerClient } from "@/lib/supabase/server";

type ShopRow = {
  id: string;
  name: string;
  domain: string;
  country: string;
  is_active: boolean;
};

const EXCLUDED_PUBLIC_SHOP_DOMAINS = new Set([
  "bol.com",
  "groovespin.nl",
  "hhv.de",
  "junorecords.com",
  "soundsdelft.nl",
]);

export default async function ShopsPage() {
  const supabase = createSupabaseServerClient();

  const { data, error } = await supabase
    .from("shops")
    .select("id, name, domain, country, is_active")
    .order("name", { ascending: true });

  if (error) {
    throw error;
  }

  const shops = ((data ?? []) as ShopRow[]).filter(
    (shop) =>
      shop.is_active &&
      !EXCLUDED_PUBLIC_SHOP_DOMAINS.has(shop.domain.trim().toLowerCase()),
  );

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
            {shops.map((shop) => (
              <article
                key={shop.id}
                className="rounded-2xl border border-neutral-200 bg-white p-5"
              >
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <h2 className="text-lg font-semibold">{shop.name}</h2>
                    <p className="mt-1 text-sm text-neutral-500">{shop.domain}</p>
                  </div>

                  <span className="inline-flex rounded-full bg-orange-50 px-3 py-1 text-xs font-medium text-orange-700">
                    actief
                  </span>
                </div>

                <p className="mt-4 text-sm text-neutral-500">
                  Land: {shop.country}
                </p>
              </article>
            ))}
          </div>
        )}
      </main>

      <SiteFooter />
    </div>
  );
}
