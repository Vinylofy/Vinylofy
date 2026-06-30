import type { SearchShopOffer } from "@/lib/vinylofy-data";

type OfferWithFutureShippingFields = SearchShopOffer & {
  estimatedTotalPrice?: number | string | null;
  estimated_total_price?: number | string | null;
  totalPrice?: number | string | null;
  total_price?: number | string | null;
  shippingPrice?: number | string | null;
  shipping_price?: number | string | null;
  estimatedShippingPrice?: number | string | null;
  estimated_shipping_price?: number | string | null;
  shippingCost?: number | string | null;
  shipping_cost?: number | string | null;
  shippingSource?: string | null;
  shipping_source?: string | null;
  shippingType?: string | null;
  shipping_type?: string | null;
  shippingConfidence?: string | null;
  shipping_confidence?: string | null;
};

function toNumber(value: number | string | null | undefined): number | null {
  if (value === null || value === undefined || value === "") return null;
  const parsed = typeof value === "string" ? Number(value) : value;
  return Number.isFinite(parsed) ? parsed : null;
}

function firstNumber(values: Array<number | string | null | undefined>): number | null {
  for (const value of values) {
    const parsed = toNumber(value);
    if (parsed !== null) return parsed;
  }

  return null;
}

function getShippingPrice(offer: OfferWithFutureShippingFields): number | null {
  return firstNumber([
    offer.shippingPrice,
    offer.shipping_price,
    offer.estimatedShippingPrice,
    offer.estimated_shipping_price,
    offer.shippingCost,
    offer.shipping_cost,
  ]);
}

function getEstimatedTotalPrice(offer: OfferWithFutureShippingFields): number | null {
  const explicitTotal = firstNumber([
    offer.estimatedTotalPrice,
    offer.estimated_total_price,
    offer.totalPrice,
    offer.total_price,
  ]);

  if (explicitTotal !== null) return explicitTotal;

  const shippingPrice = getShippingPrice(offer);
  if (shippingPrice !== null) return offer.price + shippingPrice;

  return null;
}

function getShippingRank(offer: OfferWithFutureShippingFields): number {
  const signal = [
    offer.shippingSource,
    offer.shipping_source,
    offer.shippingType,
    offer.shipping_type,
    offer.shippingConfidence,
    offer.shipping_confidence,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  if (signal.includes("exact")) return 0;
  if (
    signal.includes("estimate") ||
    signal.includes("estimated") ||
    signal.includes("geschat")
  ) {
    return 1;
  }

  return getShippingPrice(offer) !== null ? 1 : 2;
}

export function getOfferDisplayPrice(offer: SearchShopOffer): number | null {
  const enrichedOffer = offer as OfferWithFutureShippingFields;
  return getEstimatedTotalPrice(enrichedOffer) ?? offer.price;
}

export function compareOffersForSummary(
  a: SearchShopOffer,
  b: SearchShopOffer,
): number {
  const aAvailabilityRank = a.availability === "in_stock" ? 0 : 1;
  const bAvailabilityRank = b.availability === "in_stock" ? 0 : 1;

  if (aAvailabilityRank !== bAvailabilityRank) {
    return aAvailabilityRank - bAvailabilityRank;
  }

  const aDisplayPrice = getOfferDisplayPrice(a) ?? Number.MAX_SAFE_INTEGER;
  const bDisplayPrice = getOfferDisplayPrice(b) ?? Number.MAX_SAFE_INTEGER;

  if (aDisplayPrice !== bDisplayPrice) return aDisplayPrice - bDisplayPrice;

  const aShippingRank = getShippingRank(a as OfferWithFutureShippingFields);
  const bShippingRank = getShippingRank(b as OfferWithFutureShippingFields);

  if (aShippingRank !== bShippingRank) return aShippingRank - bShippingRank;

  return b.lastSeenAt.localeCompare(a.lastSeenAt);
}

export function getVisibleOfferSummary(
  offers: SearchShopOffer[],
  limit = 3,
): SearchShopOffer[] {
  return offers.slice().sort(compareOffersForSummary).slice(0, limit);
}
