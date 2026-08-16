function normalize(value: string): string {
  return value.toLowerCase().replace(/\s+/g, " ").trim();
}

export function resolveSearchGrooveSourceFromMatches(input: {
  query: string;
  artistFilter?: string;
  exactArtistNames: string[];
  resultArtistNames: string[];
}): string | null {
  const filter = input.artistFilter?.trim();
  if (filter) return filter;

  const exactQuery = normalize(input.query);
  if (!exactQuery) return null;

  const exactMatches = input.exactArtistNames.filter(
    (name) => normalize(name) === exactQuery,
  );
  if (exactMatches.length === 1) return exactMatches[0];
  if (exactMatches.length > 1) return null;

  const uniqueResultArtistsMap = new Map<string, string>();
  for (const name of input.resultArtistNames.filter(Boolean)) {
    const key = normalize(name);
    if (!uniqueResultArtistsMap.has(key)) uniqueResultArtistsMap.set(key, name.trim());
  }
  const uniqueResultArtists = [...uniqueResultArtistsMap.values()];
  return uniqueResultArtists.length === 1 ? uniqueResultArtists[0] : null;
}
