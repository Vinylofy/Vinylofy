import { createSupabaseServerClient } from "@/lib/supabase/server";
import { redirect } from "next/navigation";
import { connection } from "next/server";
import { marketHref } from "@/lib/market-url";

export type PublicMarket = {
  country_code: string;
  name: string;
  currency: string;
};

export const DEFAULT_MARKET: PublicMarket = {
  country_code: "NL",
  name: "Nederland",
  currency: "EUR",
};

export async function getActiveMarkets(): Promise<PublicMarket[]> {
  await connection();
  const supabase = createSupabaseServerClient();
  const { data, error } = await supabase
    .from("markets")
    .select("country_code,name,currency")
    .eq("status", "active")
    .order("name");
  if (error) throw error;
  const active = (data ?? []) as PublicMarket[];
  const visible = await Promise.all(active.map(async (market) => {
    if (market.country_code === "NL") return market;
    const { data: offers, error: offersError } = await supabase
      .from("market_product_best_prices_v1")
      .select("product_id")
      .eq("market_code", market.country_code)
      .limit(1);
    if (offersError) throw offersError;
    return offers?.length ? market : null;
  }));
  return visible.filter((market): market is PublicMarket => market !== null);
}

export async function resolveMarket(value: unknown): Promise<PublicMarket> {
  const code = typeof value === "string" ? value.trim().toUpperCase() : "NL";
  const markets = await getActiveMarkets();
  return markets.find((market) => market.country_code === code)
    ?? markets.find((market) => market.country_code === "NL")
    ?? DEFAULT_MARKET;
}

export function redirectInactiveMarket(value: unknown, market: PublicMarket, href: string): void {
  if (typeof value === "string" && value.trim().toUpperCase() !== market.country_code) {
    redirect(marketHref(href, market.country_code));
  }
}

export async function getEligibleShopIds(market: PublicMarket): Promise<string[]> {
  const { data, error } = await createSupabaseServerClient()
    .from("shop_shipping_destinations")
    .select("shop_id,shops!inner(is_active)")
    .eq("country_code", market.country_code)
    .eq("status", "confirmed")
    .eq("shops.is_active", true);
  if (error) throw error;
  return (data ?? []).map((row) => row.shop_id);
}
