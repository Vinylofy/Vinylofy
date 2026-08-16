import type { FtgEntityType, FtgReasonCode } from "./types";

export type FtgRankingCandidate = {
  sourceArtistId: string;
  targetArtistId: string;
  displayName: string;
  musicbrainzArtistMbid: string;
  entityType: FtgEntityType;
  factual: boolean;
  factualMechanisms: string[];
  allowedEvidenceCount: number;
  uniqueRecordingCount: number;
  similarity: boolean;
  similarityPosition: number | null;
  similarityMatchScore: number | null;
  searchEligible: boolean;
  productCount: number;
};

const RECORDING_MECHANISMS = new Set(["artist_credit", "instrument", "vocal"]);

export function getCandidateTier(candidate: FtgRankingCandidate): 1 | 2 | 3 {
  if (candidate.factual && candidate.similarity) return 1;
  if (candidate.factual) return 2;
  return 3;
}

export function getReasonCodes(candidate: FtgRankingCandidate): FtgReasonCode[] {
  const reasons: FtgReasonCode[] = [];
  if (candidate.factual && candidate.similarity) reasons.push("factual_and_similarity");
  if (candidate.factualMechanisms.includes("membership")) reasons.push("membership");
  if (candidate.factualMechanisms.some((value) => RECORDING_MECHANISMS.has(value))) {
    reasons.push("recording_collaboration");
  }
  if (candidate.similarity && !candidate.factual) reasons.push("similar_artist");
  return reasons;
}

function pythonLikeCasefold(value: string): string {
  return value.toLowerCase().replaceAll("ß", "ss").replaceAll("ς", "σ");
}

function compareStrings(left: string, right: string): number {
  if (left === right) return 0;
  return left < right ? -1 : 1;
}

function compareCandidates(left: FtgRankingCandidate, right: FtgRankingCandidate): number {
  const leftTier = getCandidateTier(left);
  const rightTier = getCandidateTier(right);
  if (leftTier !== rightTier) return leftTier - rightTier;

  if (leftTier === 1 || leftTier === 3) {
    const leftPosition = left.similarityPosition ?? 2 ** 31;
    const rightPosition = right.similarityPosition ?? 2 ** 31;
    if (leftPosition !== rightPosition) return leftPosition - rightPosition;
  }

  return (
    compareStrings(pythonLikeCasefold(left.displayName), pythonLikeCasefold(right.displayName)) ||
    compareStrings(left.musicbrainzArtistMbid, right.musicbrainzArtistMbid)
  );
}

export function deduplicateCandidates(
  candidates: FtgRankingCandidate[],
): FtgRankingCandidate[] {
  const merged = new Map<string, FtgRankingCandidate>();

  for (const candidate of candidates) {
    const key = `${candidate.sourceArtistId}:${candidate.targetArtistId}`;
    const previous = merged.get(key);
    if (!previous) {
      merged.set(key, {
        ...candidate,
        factualMechanisms: [...new Set(candidate.factualMechanisms)].sort(),
      });
      continue;
    }

    if (
      previous.displayName !== candidate.displayName ||
      previous.musicbrainzArtistMbid !== candidate.musicbrainzArtistMbid ||
      previous.entityType !== candidate.entityType
    ) {
      throw new Error(`Conflicting identity for candidate ${candidate.targetArtistId}`);
    }

    const positions = [previous.similarityPosition, candidate.similarityPosition].filter(
      (value): value is number => value !== null,
    );

    merged.set(key, {
      ...previous,
      factual: previous.factual || candidate.factual,
      factualMechanisms: [
        ...new Set([...previous.factualMechanisms, ...candidate.factualMechanisms]),
      ].sort(),
      allowedEvidenceCount: Math.max(
        previous.factual ? previous.allowedEvidenceCount : 0,
        candidate.factual ? candidate.allowedEvidenceCount : 0,
      ),
      uniqueRecordingCount: Math.max(
        previous.factual ? previous.uniqueRecordingCount : 0,
        candidate.factual ? candidate.uniqueRecordingCount : 0,
      ),
      similarity: previous.similarity || candidate.similarity,
      similarityPosition: positions.length > 0 ? Math.min(...positions) : null,
      similarityMatchScore:
        previous.similarityMatchScore ?? candidate.similarityMatchScore,
      searchEligible: previous.searchEligible || candidate.searchEligible,
      productCount: Math.max(previous.productCount, candidate.productCount),
    });
  }

  return [...merged.values()];
}

export function rankCandidates(
  candidates: FtgRankingCandidate[],
  options: { mode: "trail" | "search"; limit: number },
): FtgRankingCandidate[] {
  if (options.limit < 0) throw new Error("Limit must be non-negative");

  const pool = deduplicateCandidates(candidates).filter(
    (candidate) => options.mode === "trail" || candidate.searchEligible,
  );
  let ranked = pool.sort(compareCandidates);

  if (
    options.limit >= 3 &&
    ranked.length >= 3 &&
    ranked.slice(0, 3).every((candidate) => candidate.factual)
  ) {
    const discovery = ranked.find(
      (candidate) =>
        !candidate.factual &&
        candidate.similarity &&
        candidate.similarityPosition === 1,
    );
    if (discovery) {
      ranked = [
        ...ranked.slice(0, 2),
        discovery,
        ...ranked.slice(2).filter((candidate) => candidate !== discovery),
      ];
    }
  }

  return ranked.slice(0, options.limit);
}
