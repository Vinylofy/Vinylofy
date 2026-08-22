import { getCandidateTier, type FtgRankingCandidate } from "./ranking";

export type FtgOnwardRelation = FtgRankingCandidate & {
  sourceArtistId: string;
};

export type FtgDestinationCandidate = FtgRankingCandidate & {
  direct: boolean;
  bridgeId: string | null;
  bridgeName: string | null;
  onwardCount: number;
  v3Order: number;
};

function compareStrings(left: string, right: string): number {
  if (left === right) return 0;
  return left < right ? -1 : 1;
}

function normalizeArtistFamilyName(value: string): string {
  return value
    .normalize("NFKC")
    .toLowerCase()
    .replace(/[‐‑‒–—]/g, "-")
    .replace(/[^\p{L}\p{N}&]+/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

const GENERIC_ENSEMBLE_SUFFIX = /^(?:(?:and|&) (?:his|her|their) )?(?:(?:famous|savoy) )?(?:orchestra|band|trio|quartet|quintet|sextet|septet|octet|eight)$/;

function isEnsembleVariantOf(candidateName: string, baseName: string): boolean {
  const candidate = normalizeArtistFamilyName(candidateName);
  const base = normalizeArtistFamilyName(baseName);
  if (!base || candidate === base || !candidate.startsWith(`${base} `)) return false;
  return GENERIC_ENSEMBLE_SUFFIX.test(candidate.slice(base.length + 1));
}

function suppressArtistFamilyVariants(
  candidates: FtgDestinationCandidate[],
  sourceArtistName: string,
): FtgDestinationCandidate[] {
  const withoutSourceVariants = candidates.filter(
    (candidate) =>
      !sourceArtistName || !isEnsembleVariantOf(candidate.displayName, sourceArtistName),
  );
  return withoutSourceVariants.filter(
    (candidate) => !withoutSourceVariants.some(
      (possibleBase) =>
        possibleBase.targetArtistId !== candidate.targetArtistId &&
        isEnsembleVariantOf(candidate.displayName, possibleBase.displayName),
    ),
  );
}

function compareDestinationBase(left: FtgDestinationCandidate, right: FtgDestinationCandidate): number {
  const tierOrder = getCandidateTier(left) - getCandidateTier(right);
  if (tierOrder) return tierOrder;
  const onwardOrder = right.onwardCount - left.onwardCount;
  if (onwardOrder) return onwardOrder;
  if (left.direct !== right.direct) return left.direct ? -1 : 1;
  if (left.v3Order !== right.v3Order) return left.v3Order - right.v3Order;
  return (
    compareStrings(left.displayName.toLowerCase(), right.displayName.toLowerCase()) ||
    compareStrings(left.musicbrainzArtistMbid, right.musicbrainzArtistMbid)
  );
}

function isMeaningfulBridge(candidate: FtgDestinationCandidate): boolean {
  // A strong direct relation is meaningful on its own. A lower-tier bridge
  // earns representation when it is a real project hub with multiple
  // evidence-backed children; a single weak similarity remains skippable.
  return candidate.direct && candidate.onwardCount > 0 &&
    (getCandidateTier(candidate) <= 2 || candidate.onwardCount >= 2);
}

function isBetterIndirect(
  next: FtgDestinationCandidate,
  previous: FtgDestinationCandidate,
): boolean {
  return compareDestinationBase(next, previous) < 0;
}

/**
 * Selects navigable destinations from the V3-ranked direct pool plus one
 * bounded bridge hop. V3 still supplies relation quality; this layer only
 * chooses which destinations deserve the five UI slots.
 */
export function selectNextDestinations(input: {
  sourceArtistId: string;
  sourceArtistName: string;
  direct: FtgRankingCandidate[];
  onward: Map<string, FtgOnwardRelation[]>;
  excludedArtistIds?: Set<string>;
  requireSearchEligible?: boolean;
  limit: number;
}): FtgDestinationCandidate[] {
  const excluded = input.excludedArtistIds ?? new Set<string>();
  const visible = (candidate: FtgRankingCandidate): boolean =>
    !input.requireSearchEligible || candidate.searchEligible;
  const direct = input.direct.filter(visible);
  const directIds = new Set(direct.map((candidate) => candidate.targetArtistId));
  const byDestination = new Map<string, FtgDestinationCandidate>();

  direct.forEach((candidate) => {
    if (candidate.targetArtistId === input.sourceArtistId || excluded.has(candidate.targetArtistId)) return;
    byDestination.set(candidate.targetArtistId, {
      ...candidate,
      direct: true,
      bridgeId: null,
      bridgeName: null,
      onwardCount: new Set(
        (input.onward.get(candidate.targetArtistId) ?? [])
          .map((relation) => relation.targetArtistId)
          .filter((targetId) => targetId !== input.sourceArtistId && !excluded.has(targetId)),
      ).size,
      v3Order: candidate.similarityPosition ?? 2 ** 31,
    });
  });

  input.direct.forEach((bridge) => {
    const onward = input.onward.get(bridge.targetArtistId) ?? [];
    const onwardIds = new Set(
      onward
        .map((relation) => relation.targetArtistId)
        .filter(
          (destinationId) =>
            destinationId !== input.sourceArtistId &&
            destinationId !== bridge.targetArtistId &&
            !directIds.has(destinationId) &&
            !excluded.has(destinationId),
        ),
    );
    for (const relation of onward) {
      const destinationId = relation.targetArtistId;
      if (
        !visible(relation) ||
        destinationId === input.sourceArtistId ||
        destinationId === bridge.targetArtistId ||
        directIds.has(destinationId) ||
        excluded.has(destinationId)
      ) continue;
      const indirect: FtgDestinationCandidate = {
        ...relation,
        sourceArtistId: input.sourceArtistId,
        direct: false,
        bridgeId: bridge.targetArtistId,
        bridgeName: bridge.displayName,
        onwardCount: onwardIds.size,
        v3Order: bridge.similarityPosition ?? 2 ** 31,
      };
      const previous = byDestination.get(destinationId);
      if (!previous || (!previous.direct && isBetterIndirect(indirect, previous))) {
        byDestination.set(destinationId, indirect);
      }
    }
  });

  const values = [...byDestination.values()];
  values.sort(compareDestinationBase);
  // Apply bridge representation after the transparent V3/onward ordering.
  // This local promotion is deliberately limited to a bridge's own children;
  // it cannot create an entity-type preference or a global quota.
  for (const bridge of values.filter(isMeaningfulBridge)) {
    const bridgeIndex = values.findIndex((candidate) => candidate.targetArtistId === bridge.targetArtistId);
    const firstChildIndex = values.findIndex((candidate) => candidate.bridgeId === bridge.targetArtistId);
    if (bridgeIndex < 0 || firstChildIndex < 0 || firstChildIndex > bridgeIndex) continue;
    values.splice(bridgeIndex, 1);
    values.splice(firstChildIndex, 0, bridge);
  }
  return suppressArtistFamilyVariants(values, input.sourceArtistName)
    .slice(0, Math.max(0, input.limit));
}
