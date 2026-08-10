import {
  PRODUCT_COVERS_BUCKET,
  normalizeCoverStoragePath,
} from "@/lib/cover-url";

export const runtime = "nodejs";

const PUBLIC_OBJECT_PREFIX =
  `/storage/v1/object/public/${PRODUCT_COVERS_BUCKET}/`;

const ALLOWED_IMAGE_CONTENT_TYPES = new Set([
  "image/webp",
  "image/jpeg",
  "image/png",
]);

function parseSupabaseOrigin(
  value: string | null | undefined,
): string | null {
  const raw = value?.trim();
  if (!raw) {
    return null;
  }

  try {
    const parsed = new URL(raw);

    if (
      !["http:", "https:"].includes(parsed.protocol) ||
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

function encodeStoragePath(storagePath: string): string {
  return storagePath
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
}

function errorResponse(status: number): Response {
  return new Response(null, {
    status,
    headers: {
      "Cache-Control": "no-store",
      "X-Content-Type-Options": "nosniff",
    },
  });
}

export async function GET(
  _request: Request,
  {
    params,
  }: {
    params: Promise<{ path: string[] }>;
  },
) {
  const { path } = await params;

  const storagePath = normalizeCoverStoragePath(
    path.join("/"),
  );

  if (!storagePath) {
    return errorResponse(404);
  }

  const supabaseOrigin = parseSupabaseOrigin(
    process.env.NEXT_PUBLIC_SUPABASE_URL,
  );

  if (!supabaseOrigin) {
    return errorResponse(503);
  }

  const upstreamUrl =
    `${supabaseOrigin}${PUBLIC_OBJECT_PREFIX}` +
    encodeStoragePath(storagePath);

  let upstream: Response;

  try {
    upstream = await fetch(upstreamUrl, {
      method: "GET",
      redirect: "error",
      cache: "no-store",
    });
  } catch {
    return errorResponse(502);
  }

  if (!upstream.ok || !upstream.body) {
    if (upstream.status === 404) {
      return errorResponse(404);
    }

    return errorResponse(502);
  }

  const contentType = upstream.headers
    .get("content-type")
    ?.split(";", 1)[0]
    ?.trim()
    ?.toLowerCase();

  if (
    !contentType ||
    !ALLOWED_IMAGE_CONTENT_TYPES.has(contentType)
  ) {
    return errorResponse(502);
  }

  return new Response(upstream.body, {
    status: 200,
    headers: {
      "Content-Type": contentType,
      "Cache-Control":
        "public, max-age=3600, s-maxage=3600",
      "X-Content-Type-Options": "nosniff",
    },
  });
}
