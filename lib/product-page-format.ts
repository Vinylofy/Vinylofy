export function formatRelativeFreshness(iso: string | null | undefined): string | null {
  if (!iso) return null;

  const timestamp = new Date(iso).getTime();
  if (Number.isNaN(timestamp)) return null;

  const diffHours = (Date.now() - timestamp) / (1000 * 60 * 60);

  if (diffHours < 24) return "Vandaag gecontroleerd";
  if (diffHours < 48) return "1 dag geleden gecontroleerd";
  if (diffHours < 72) return "2 dagen geleden gecontroleerd";

  const diffDays = Math.floor(diffHours / 24);
  return `${diffDays} dagen geleden gecontroleerd`;
}

export function formatOfferDomain(value: string | null | undefined): string {
  if (!value) return "winkel";

  return value
    .replace(/^https?:\/\//i, "")
    .replace(/^www\./i, "")
    .replace(/\/$/, "");
}
