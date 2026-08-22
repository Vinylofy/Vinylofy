import type { FtgTrailItem } from "./types";

export type FtgTrailArtist = { mbid: string; name: string };

export function buildExplainedTrail(
  artists: FtgTrailArtist[],
  explanations: (string | null)[],
): FtgTrailItem[] {
  return artists.map((artist, index) => ({
    ...artist,
    explanation: index === 0 ? null : explanations[index] ?? null,
  }));
}

export function trailPrefix(trail: FtgTrailItem[], inclusiveIndex: number): FtgTrailItem[] {
  return trail.slice(0, Math.max(0, inclusiveIndex + 1));
}
