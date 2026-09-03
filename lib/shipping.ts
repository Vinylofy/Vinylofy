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
  freeShippingThresholdPrice: number | null;
};

function toFiniteNumber(value: number | string | null | undefined): number | null {
  if (value === null || value === undefined || value === "") return null;
  const parsed = typeof value === "string" ? Number(value) : value;
  return Number.isFinite(parsed) ? parsed : null;
}

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


    if (process.env.NODE_ENV !== "production") {
      console.log("[SHIPPING]", {
        shopId: offer.shopId,
        found: !!rule,
        price: offer.price,
      });
    }

    if (!rule) {
      return {
        ...offer,
        estimatedShippingPrice: null,
        estimatedTotalPrice: null,
        freeShippingApplied: false,
        shippingNote: null,
        shippingConfidence: null,
      freeShippingThresholdPrice: null,
      };
    }

    const priceCents = Math.round(offer.price * 100);
    const shippingCostCents = toFiniteNumber(rule.shippingCostCents);
    const freeShippingThresholdCents = toFiniteNumber(
      rule.freeShippingThresholdCents,
    );

    const free =
      freeShippingThresholdCents !== null &&
      priceCents >= freeShippingThresholdCents;

    // A missing tariff is unknown, not free shipping. Only a threshold that
    // was actually met can turn a known tariff into zero.
    const shippingCents = free ? 0 : shippingCostCents;

    return {
      ...offer,
      estimatedShippingPrice:
        shippingCents === null ? null : shippingCents / 100,
      estimatedTotalPrice:
        shippingCents === null ? null : (priceCents + shippingCents) / 100,
      freeShippingApplied: free,
      shippingNote: rule.shippingNote,
      shippingConfidence: rule.confidence,
      freeShippingThresholdPrice:
        freeShippingThresholdCents !== null
          ? freeShippingThresholdCents / 100
          : null,
    };
  });
}
