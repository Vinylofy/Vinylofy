export type DutchchartsVinylTop33Item = {
  rank: number;
  artist: string;
  title: string;
};

const DUTCHCHARTS_VINYL_TOP33_URL =
  "https://www.dutchcharts.nl/weekchart.asp?cat=av";

function decodeHtmlEntities(value: string): string {
  return value
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#039;/g, "'")
    .replace(/&#39;/g, "'")
    .replace(/&rsquo;/g, "’")
    .replace(/&lsquo;/g, "‘")
    .replace(/&rdquo;/g, "”")
    .replace(/&ldquo;/g, "“")
    .replace(/&ndash;/g, "–")
    .replace(/&mdash;/g, "—")
    .replace(/&nbsp;/g, " ");
}

function cleanText(value: string): string {
  return decodeHtmlEntities(value)
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<br\s*\/?>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function decodeLatin1UrlComponent(value: string): string {
  const normalized = value.replace(/\+/g, " ");
  const bytes: number[] = [];

  for (let index = 0; index < normalized.length; index += 1) {
    const char = normalized[index];

    if (
      char === "%" &&
      /^[0-9A-Fa-f]{2}$/.test(normalized.slice(index + 1, index + 3))
    ) {
      bytes.push(Number.parseInt(normalized.slice(index + 1, index + 3), 16));
      index += 2;
      continue;
    }

    bytes.push(char.charCodeAt(0));
  }

  return new TextDecoder("iso-8859-1").decode(new Uint8Array(bytes)).trim();
}

function extractHrefParam(href: string, paramName: string): string {
  const decodedHref = decodeHtmlEntities(href);
  const queryIndex = decodedHref.indexOf("?");

  if (queryIndex < 0) return "";

  const parts = decodedHref.slice(queryIndex + 1).split("&");

  for (const part of parts) {
    const [key, ...rest] = part.split("=");

    if (key === paramName) {
      return decodeLatin1UrlComponent(rest.join("="));
    }
  }

  return "";
}

function extractFromAnchorHtml(anchorHtml: string): {
  artist: string;
  title: string;
} {
  const boldMatch = anchorHtml.match(
    /<b\b[^>]*>([\s\S]*?)<\/b>\s*<br\s*\/?>\s*([\s\S]*?)<\/a>/i,
  );

  if (!boldMatch) {
    return { artist: "", title: "" };
  }

  return {
    artist: cleanText(boldMatch[1]),
    title: cleanText(boldMatch[2]),
  };
}

export function parseDutchchartsVinylTop33Html(
  html: string,
): DutchchartsVinylTop33Item[] {
  const items: DutchchartsVinylTop33Item[] = [];

  const chartTitleRegex =
    /<div\b[^>]*class=["'][^"']*\bchart_title\b[^"']*["'][^>]*>\s*<a\b[^>]*href=["']([^"']*showitem\.asp[^"']*)["'][^>]*>([\s\S]*?)<\/a>\s*<\/div>/gi;

  let match: RegExpExecArray | null;

  while ((match = chartTitleRegex.exec(html)) !== null && items.length < 33) {
    const href = match[1];
    const anchorHtml = `<a>${match[2]}</a>`;
    const fromAnchor = extractFromAnchorHtml(anchorHtml);

    const artist = cleanText(
      fromAnchor.artist || extractHrefParam(href, "interpret"),
    );
    const title = cleanText(fromAnchor.title || extractHrefParam(href, "titel"));

    if (!artist || !title) continue;

    items.push({
      rank: items.length + 1,
      artist,
      title,
    });
  }

  if (items.length === 0) {
    throw new Error(
      "Dutchcharts Vinyl 33 bevatte geen parsebare chart_title items.",
    );
  }

  return items.slice(0, 33);
}

export async function getDutchchartsVinylTop33(): Promise<
  DutchchartsVinylTop33Item[]
> {
  const response = await fetch(DUTCHCHARTS_VINYL_TOP33_URL, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36",
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
    },
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(
      `Dutchcharts Vinyl 33 ophalen mislukt: HTTP ${response.status}`,
    );
  }

  const html = await response.text();
  return parseDutchchartsVinylTop33Html(html);
}
