import { createSupabaseServerClient } from "@/lib/supabase/server";
import { ShippingRule } from "./shipping";

export async function getShippingRulesMap(): Promise<Map<string, ShippingRule>> {
  const supabase = createSupabaseServerClient();

  const { data, error } = await supabase
    .from("shop_shipping_rules")
    .select(`
      shop_id,
      shipping_cost_cents,
      free_shipping_threshold_cents,
      shipping_note,
      confidence
    `)
    .eq("active", true)
    .eq("country_code", "NL");

  if (error) {
    console.error("[shipping] failed to load shipping rules", error);

    return new Map();
  }

  const map = new Map<string, ShippingRule>();

  for (const row of data ?? []) {
    map.set(row.shop_id, {
      shopId: row.shop_id,
      shippingCostCents: row.shipping_cost_cents,
      freeShippingThresholdCents: row.free_shipping_threshold_cents,
      shippingNote: row.shipping_note,
      confidence: row.confidence,
    });
  }

  return map;
}
