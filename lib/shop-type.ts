export type ShopType =
  | "physical_record_store"
  | "online_record_store"
  | "online_retailer";

export function isMissingShopTypeColumn(error: { code?: string; message?: string } | null): boolean {
  return error?.code === "42703" && Boolean(error.message?.includes("shop_type"));
}
