import type { ShopType } from "@/lib/shop-type";

const SHOP_TYPE_BADGES: Record<ShopType, { label: string; className: string }> = {
  physical_record_store: {
    label: "Fysieke platenwinkel",
    className: "border-green-200 bg-green-50 text-green-800",
  },
  online_record_store: {
    label: "Online platenwinkel",
    className: "border-blue-200 bg-blue-50 text-blue-800",
  },
  online_retailer: {
    label: "Online retailer",
    className: "border-orange-200 bg-orange-50 text-orange-800",
  },
};

export function ShopTypeBadge({ type }: { type: ShopType | null | undefined }) {
  const badge = type ? SHOP_TYPE_BADGES[type] : null;
  if (!badge) return null;

  return (
    <span
      className={`inline-flex h-5 max-w-full shrink-0 items-center rounded-full border px-2 text-[10px] font-medium leading-none whitespace-nowrap ${badge.className}`}
    >
      {badge.label}
    </span>
  );
}
