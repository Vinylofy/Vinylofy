import type { FtgEntityType } from "./types";

const MBID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function isValidArtistMbid(value: string): boolean {
  return MBID_PATTERN.test(value);
}

export function isValidTrail(trail: string[], maxLength = 24): boolean {
  return trail.length > 0 && trail.length <= maxLength && trail.every(isValidArtistMbid);
}

export function formatProductCount(count: number): string {
  return count === 1 ? "Vinylofy vond 1 titel" : `Vinylofy vond ${count} titels`;
}

export function formatEntityType(entityType: FtgEntityType): string {
  return entityType === "person" ? "Persoon" : "Groep";
}

export function buildGrooveHref(trailMbids: string[], nextMbid?: string): string {
  const segments = nextMbid ? [...trailMbids, nextMbid] : trailMbids;
  return `/follow-the-groove/${segments.map(encodeURIComponent).join("/")}`;
}
