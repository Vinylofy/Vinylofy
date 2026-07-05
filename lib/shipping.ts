export type ShippingRule = {
  shopId: string;
  shippingCostCents: number | null;
  freeShippingThresholdCents: number | null;
  shippingNote: string | null;
  confidence: string | null;
};

export type ShippingInfo = {
  estimatedShippingPrice: number | null;
  estimatedTotalPrice: number | null;
  freeShippingApplied: boolean;
  shippingNote: string | null;
  shippingConfidence: string | null;
};

export function enrichOffersWithShipping<
  T extends {
    shopId?: string | null;
    price: number;
  },
>(
  offers: T[],
  rules: Map<string, ShippingRule>,
): (T & ShippingInfo)[] {
  return offers.map((offer) => {
    const rule =
      offer.shopId && rules.has(offer.shopId)
        ? rules.get(offer.shopId)!
        : null;

    if (!rule) {
      return {
        ...offer,
        estimatedShippingPrice: null,
        estimatedTotalPrice: null,
        freeShippingApplied: false,
        shippingNote: null,
        shippingConfidence: null,
      };
    }

    const priceCents = Math.round(offer.price * 100);

    const free =
      rule.freeShippingThresholdCents !== null &&
      priceCents >= rule.freeShippingThresholdCents;

    const shippingCents = free
      ? 0
      : (rule.shippingCostCents ?? 0);

    return {
      ...offer,
      estimatedShippingPrice: shippingCents / 100,
      estimatedTotalPrice:
        (priceCents + shippingCents) / 100,
      freeShippingApplied: free,
      shippingNote: rule.shippingNote,
      shippingConfidence: rule.confidence,
    };
  });
}
