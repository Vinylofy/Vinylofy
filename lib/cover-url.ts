const DEFAULT_SUPABASE_URL =
  process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";

export const PRODUCT_COVERS_BUCKET = "product-covers";
export const COVER_PLACEHOLDER_SRC =
  "/placeholders/vinylofy-cover-placeholder-white2.png";

const PUBLIC_OBJECT_PREFIX =
  `/storage/v1/object/public/${PRODUCT_COVERS_BUCKET}/`;
const STORAGE_SEGMENT_PATTERN = /^[A-Za-z0-9._~-]+$/;

export type CoverUrlInput = {
  coverUrl?: string | null;
  storagePath?: string | null;
  supabaseUrl?: string | null;
};

function parseSupabaseOrigin(value: string | null | undefined): string | null {
  const raw = value?.trim();
  if (!raw) {
    return null;
  }

  try {
    const parsed = new URL(raw);
    if (!["http:", "https:"].includes(parsed.protocol)) {
      return null;
    }
    if (
      parsed.username ||
      parsed.password ||
      parsed.search ||
      parsed.hash ||
      (parsed.pathname !== "/" && parsed.pathname !== "")
    ) {
      return null;
    }
    return parsed.origin;
  } catch {
    return null;
  }
}

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
    if (parsed.origin !== "https://vinylofy.local") {
      return false;
    }
    return !parsed.pathname
      .split("/")
      .some((segment) => segment === "." || segment === "..");
  } catch {
    return false;
  }
}

export function buildProductCoverUrl(
  storagePath: string | null | undefined,
  supabaseUrl: string | null | undefined = DEFAULT_SUPABASE_URL,
): string | null {
  const normalizedPath = normalizeCoverStoragePath(storagePath);
  const origin = parseSupabaseOrigin(supabaseUrl);
  if (!normalizedPath || !origin) {
    return null;
  }

  return `${origin}${PUBLIC_OBJECT_PREFIX}${encodeStoragePath(
    normalizedPath,
  )}`;
}

export function isSafeCoverUrl(
  value: string | null | undefined,
  storagePath?: string | null,
  supabaseUrl: string | null | undefined = DEFAULT_SUPABASE_URL,
): boolean {
  const raw = value?.trim();

  if (isLocalCoverAsset(value)) {
    return true;
  }

  const normalizedPath = normalizeCoverStoragePath(storagePath);
  const origin = parseSupabaseOrigin(supabaseUrl);
  if (!raw || !normalizedPath || !origin) {
    return false;
  }

  try {
    const parsed = new URL(raw);
    if (
      parsed.origin !== origin ||
      parsed.username ||
      parsed.password ||
      parsed.search ||
      parsed.hash
    ) {
      return false;
    }

    const expectedPathname =
      `${PUBLIC_OBJECT_PREFIX}${encodeStoragePath(normalizedPath)}`;
    return parsed.pathname === expectedPathname;
  } catch {
    return false;
  }
}

export function resolveCoverUrl({
  coverUrl,
  storagePath,
  supabaseUrl = DEFAULT_SUPABASE_URL,
}: CoverUrlInput): string {
  if (isLocalCoverAsset(coverUrl)) {
    return coverUrl;
  }

  return (
    buildProductCoverUrl(storagePath, supabaseUrl) ??
    COVER_PLACEHOLDER_SRC
  );
}
