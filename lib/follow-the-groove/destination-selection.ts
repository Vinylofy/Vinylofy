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

function compareDestination(left: FtgDestinationCandidate, right: FtgDestinationCandidate): number {
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

function isBetterIndirect(
  next: FtgDestinationCandidate,
  previous: FtgDestinationCandidate,
): boolean {
  return compareDestination(next, previous) < 0;
}

/**
 * Selects navigable destinations from the V3-ranked direct pool plus one
 * bounded bridge hop. V3 still supplies relation quality; this layer only
 * chooses which destinations deserve the five UI slots.
 */
export function selectNextDestinations(input: {
  sourceArtistId: string;
  direct: FtgRankingCandidate[];
  onward: Map<string, FtgOnwardRelation[]>;
  excludedArtistIds?: Set<string>;
  limit: number;
}): FtgDestinationCandidate[] {
  const excluded = input.excludedArtistIds ?? new Set<string>();
  const directIds = new Set(input.direct.map((candidate) => candidate.targetArtistId));
  const byDestination = new Map<string, FtgDestinationCandidate>();

  input.direct.forEach((candidate, index) => {
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
      v3Order: index,
    });
  });

  input.direct.forEach((bridge, bridgeIndex) => {
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
        v3Order: bridgeIndex,
      };
      const previous = byDestination.get(destinationId);
      if (!previous || (!previous.direct && isBetterIndirect(indirect, previous))) {
        byDestination.set(destinationId, indirect);
      }
    }
  });

  return [...byDestination.values()].sort(compareDestination).slice(0, Math.max(0, input.limit));
}
