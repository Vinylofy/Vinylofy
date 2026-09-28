import { createSupabaseServerClient } from "@/lib/supabase/server";

export type ShopHighlights = {
  lowestPriceCount: number;
  onlyHereCount: number;
};

type ShopHighlightsRow = {
  shop_id: string;
  lowest_price_count: number | string;
  only_here_count: number | string;
};

export async function getShopHighlights(): Promise<Map<string, ShopHighlights>> {
  const supabase = createSupabaseServerClient();
  const { data, error } = await supabase.rpc("shop_highlights_v1");
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
