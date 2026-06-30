type ShopCountryInput = {
  name?: string | null;
  domain?: string | null;
};

const SHOP_COUNTRY_BY_NAME: Record<string, string> = {
  imusic: "DK",
  hhv: "DE",
  soundsdelft: "NL",
  soundshaarlem: "NL",
  soundsvenlo: "NL",
  bobsvinyl: "NL",
  dgmoutlet: "NL",
  northendhaarlem: "NL",
  velvetdelft: "NL",
  velvetdordrecht: "NL",
  groovespin: "CZ",
};

const SHOP_COUNTRY_BY_DOMAIN_FRAGMENT: Array<[string, string]> = [
  ["imusic.", "DK"],
  ["hhv.", "DE"],
  ["soundsdelft.nl", "NL"],
  ["soundshaarlem.nl", "NL"],
  ["sounds-venlo.nl", "NL"],
  ["bobsvinyl.nl", "NL"],
  ["dgmoutlet.nl", "NL"],
  ["northendhaarlem.nl", "NL"],
  ["velvetdelft.nl", "NL"],
  ["velvetdordrecht.nl", "NL"],
  ["groovespin.", "CZ"],
];

function normalizeShopKey(value: string | null | undefined): string {
  return (value ?? "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, "")
    .trim();
}

function normalizeDomain(value: string | null | undefined): string {
  return (value ?? "")
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .replace(/\/$/, "")
    .trim();
}

function inferCountryFromDomainTld(domain: string): string | null {
  const match = domain.match(/\.([a-z]{2})(?:\/|$)/i);
  const tld = match?.[1]?.toLowerCase();

  if (tld === "nl") return "NL";
  if (tld === "de") return "DE";
  if (tld === "dk") return "DK";
  if (tld === "cz") return "CZ";

  return null;
}

export function getShopCountryCode(shop: ShopCountryInput): string {
  const nameKey = normalizeShopKey(shop.name);
  const byName = SHOP_COUNTRY_BY_NAME[nameKey];

  if (byName) return byName;

  const domain = normalizeDomain(shop.domain);
  const byDomain = SHOP_COUNTRY_BY_DOMAIN_FRAGMENT.find(([fragment]) =>
    domain.includes(fragment),
  )?.[1];

  if (byDomain) return byDomain;

  return inferCountryFromDomainTld(domain) ?? "??";
}
