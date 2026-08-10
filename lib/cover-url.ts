export const PRODUCT_COVERS_BUCKET = "product-covers";
export const COVER_PLACEHOLDER_SRC =
  "/placeholders/vinylofy-cover-placeholder-white2.png";
export const COVER_DELIVERY_PREFIX = "/covers/";

const STORAGE_SEGMENT_PATTERN = /^[A-Za-z0-9._~-]+$/;

export type CoverUrlInput = {
  coverUrl?: string | null;
  storagePath?: string | null;
};

export function normalizeCoverStoragePath(
  value: string | null | undefined,
): string | null {
  const raw = value?.trim();
  if (
    !raw ||
    raw.startsWith("/") ||
    raw.endsWith("/") ||
    raw.includes("\\") ||
    raw.includes("?") ||
    raw.includes("#") ||
    /[\u0000-\u001F\u007F]/.test(raw)
  ) {
    return null;
  }

  const segments = raw.split("/");
  if (
    segments.some(
      (segment) =>
        !segment ||
        segment === "." ||
        segment === ".." ||
        !STORAGE_SEGMENT_PATTERN.test(segment),
    )
  ) {
    return null;
  }

  return segments.join("/");
}

function encodeStoragePath(storagePath: string): string {
  return storagePath
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
}

export function isLocalCoverAsset(
  value: string | null | undefined,
): value is string {
  const raw = value?.trim();
  if (
    !raw ||
    !raw.startsWith("/") ||
    raw.startsWith("//") ||
    raw.includes("\\") ||
    /[\u0000-\u001F\u007F]/.test(raw)
  ) {
    return false;
  }

  try {
    const parsed = new URL(raw, "https://vinylofy.local");
    return parsed.origin === "https://vinylofy.local";
  } catch {
    return false;
  }
}

export function buildProductCoverUrl(
  storagePath: string | null | undefined,
): string | null {
  const normalizedPath = normalizeCoverStoragePath(storagePath);
  if (!normalizedPath) {
    return null;
  }

  return `${COVER_DELIVERY_PREFIX}${encodeStoragePath(normalizedPath)}`;
}

export function isSafeCoverUrl(
  value: string | null | undefined,
): boolean {
  return isLocalCoverAsset(value);
}

export function resolveCoverUrl({
  coverUrl,
  storagePath,
}: CoverUrlInput): string {
  if (isLocalCoverAsset(coverUrl)) {
    return coverUrl;
  }

  return (
    buildProductCoverUrl(storagePath) ??
    COVER_PLACEHOLDER_SRC
  );
}
