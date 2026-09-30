import { createSupabaseServerClient } from "@/lib/supabase/server";
import { DEFAULT_MARKET, type PublicMarket } from "@/lib/markets";

export type ShopHighlights = {
  lowestPriceCount: number;
  onlyHereCount: number;
};

type ShopHighlightsRow = {
  shop_id: string;
  lowest_price_count: number | string;
  only_here_count: number | string;
};

export async function getShopHighlights(market: PublicMarket = DEFAULT_MARKET): Promise<Map<string, ShopHighlights>> {
  const supabase = createSupabaseServerClient();
  const { data, error } = await supabase.rpc("shop_highlights_market_v1", {
    p_country_code: market.country_code,
  });
  if (error) {
    console.warn("[shops] highlights unavailable", {
      code: error.code,
      message: error.message,
    });
    return new Map();
  }

  return new Map(
    ((data ?? []) as ShopHighlightsRow[]).map((row) => [
      row.shop_id,
      {
        lowestPriceCount: Number(row.lowest_price_count),
        onlyHereCount: Number(row.only_here_count),
      },
    ]),
  );
}
