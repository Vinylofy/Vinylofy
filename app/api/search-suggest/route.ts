import { NextResponse } from "next/server";
import { createSupabaseServerClient } from "@/lib/supabase/server";

type ProductSuggestionRow = {
  id: string;
  artist: string;
  format_label: string | null;
};

type ArtistSuggestion = {
  id: string;
  kind: "artist";
  label: string;
  href: string;
  searchValue: string;
  bucket: number;
  score: number;
};

const BLACKLISTED_FORMAT_LABELS = new Set([
  "CD",
  "POSTER",
  "ACCESSORIES",
  "PHOTOBOOK",
  "BLUERAY",
  "BLURAY",
]);

function normalizeValue(value: string): string {
  return value.toLowerCase().replace(/\s+/g, " ").trim();
}

function isAllowedProduct(row: Pick<ProductSuggestionRow, "format_label">): boolean {
  return !BLACKLISTED_FORMAT_LABELS.has(
    (row.format_label ?? "").trim().toUpperCase(),
  );
}

function buildSearchHref(query: string): string {
  const params = new URLSearchParams();
  const trimmed = query.trim();

  if (trimmed) {
    params.set("q", trimmed);
  }

  return `/search${params.toString() ? `?${params.toString()}` : ""}`;
}

function scoreArtistSuggestion(
  artist: string,
  query: string,
): { bucket: number; score: number } {
  const value = normalizeValue(artist);
  const needle = normalizeValue(query);

  if (!needle) {
    return { bucket: 9, score: 0 };
  }

  if (value === needle) {
    return { bucket: 0, score: 3000 };
  }

  if (value.startsWith(needle)) {
    return {
      bucket: 1,
      score: value.startsWith(`${needle} `) ? 2600 : 2400,
    };
  }

  if (value.includes(needle)) {
    return {
      bucket: 2,
      score: value.includes(` ${needle}`) ? 1600 : 1400,
    };
  }

  return { bucket: 9, score: 0 };
}

async function collectRows(query: string): Promise<ProductSuggestionRow[]> {
  const supabase = createSupabaseServerClient();
  const trimmed = query.trim();
  const baseSelect = "id, artist, format_label";
  const rows = new Map<string, ProductSuggestionRow>();

  async function collect(
    request: PromiseLike<{ data: unknown; error: unknown }>,
  ): Promise<void> {
    const result = (await request) as { data: unknown; error: unknown };

    if (result.error) {
      throw result.error;
    }

    for (const row of (result.data ?? []) as ProductSuggestionRow[]) {
      if (!row.artist?.trim()) {
        continue;
      }

      rows.set(row.id, row);
    }
  }

  await Promise.all([
    collect(
      supabase
        .from("products")
        .select(baseSelect)
        .ilike("artist", `${trimmed}%`)
        .limit(16),
    ),
    collect(
      supabase
        .from("products")
        .select(baseSelect)
        .ilike("artist", `%${trimmed}%`)
        .limit(16),
    ),
  ]);

  return Array.from(rows.values()).filter(isAllowedProduct);
}

function buildArtistSuggestions(
  rows: ProductSuggestionRow[],
  query: string,
): ArtistSuggestion[] {
  const grouped = new Map<
    string,
    { artist: string; bucket: number; score: number }
  >();

  for (const row of rows) {
    const normalizedArtist = normalizeValue(row.artist);

    if (!normalizedArtist) {
      continue;
    }

    const { bucket, score } = scoreArtistSuggestion(row.artist, query);

    if (bucket > 2) {
      continue;
    }

    const existing = grouped.get(normalizedArtist);

    if (!existing) {
      grouped.set(normalizedArtist, {
        artist: row.artist,
        bucket,
        score,
      });
      continue;
    }

    if (bucket < existing.bucket || score > existing.score) {
      existing.bucket = bucket;
      existing.score = score;
    }

    if (row.artist.length < existing.artist.length) {
      existing.artist = row.artist;
    }
  }

  return Array.from(grouped.entries()).map(([key, value]) => ({
    id: `artist:${key}`,
    kind: "artist",
    label: value.artist,
    href: buildSearchHref(value.artist),
    searchValue: value.artist,
    bucket: value.bucket,
    score: value.score,
  }));
}

function sortSuggestions(a: ArtistSuggestion, b: ArtistSuggestion): number {
  if (a.bucket !== b.bucket) {
    return a.bucket - b.bucket;
  }

  if (b.score !== a.score) {
    return b.score - a.score;
  }

  return a.label.localeCompare(b.label, "nl", { sensitivity: "base" });
}

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const query = searchParams.get("q")?.trim() ?? "";

  if (query.length < 2) {
    return NextResponse.json({ suggestions: [] });
  }

  try {
    const rows = await collectRows(query);
    const suggestions = buildArtistSuggestions(rows, query)
      .sort(sortSuggestions)
      .slice(0, 5)
      .map((suggestion) => ({
        id: suggestion.id,
        kind: suggestion.kind,
        label: suggestion.label,
        href: suggestion.href,
        searchValue: suggestion.searchValue,
      }));

    return NextResponse.json({ suggestions });
  } catch (error) {
    console.error("search-suggest route failed", error);

    return NextResponse.json({ suggestions: [] }, { status: 200 });
  }
}
