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
  bridgeCount?: number;
  dedicatedSoloFallback?: boolean;
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

const DEDICATED_GENERIC_ENSEMBLE_SUFFIX = /^(?:(?:and|&) (?:his|her|their) )?(?:(?:famous|savoy) )?(?:orchestra|band|trio|quartet|quintet|sextet|septet|octet|nonet|eight|all stars|group)$/;

const DEDICATED_NAMED_PROJECT_SUFFIX = /^(?:(?:and|&) the .+|big band)$/;

function isEnsembleVariantOf(candidateName: string, baseName: string): boolean {
  const candidate = normalizeArtistFamilyName(candidateName);
  const base = normalizeArtistFamilyName(baseName);
  if (!base || candidate === base || !candidate.startsWith(`${base} `)) return false;
  return GENERIC_ENSEMBLE_SUFFIX.test(candidate.slice(base.length + 1));
}

function dedicatedFamilyKey(value: string): string | null {
  const normalized = normalizeArtistFamilyName(value).replace(/^the /, "");
  const parts = normalized.split(" ");
  for (let index = 1; index < parts.length; index += 1) {
    const suffix = parts.slice(index).join(" ");
    if (
      DEDICATED_GENERIC_ENSEMBLE_SUFFIX.test(suffix) ||
      DEDICATED_NAMED_PROJECT_SUFFIX.test(suffix)
    ) {
      return parts.slice(0, index).join(" ");
    }
  }
  return null;
}

function suppressArtistFamilyVariants(
  candidates: FtgDestinationCandidate[],
  visitedArtistNames: Iterable<string>,
): FtgDestinationCandidate[] {
  const visitedNames = [...visitedArtistNames];
  const withoutVisitedVariants = candidates.filter(
    (candidate) =>
      !visitedNames.some((visitedName) =>
        isEnsembleVariantOf(candidate.displayName, visitedName),
      ),
  );
  return withoutVisitedVariants.filter(
    (candidate) => !withoutVisitedVariants.some(
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
  visitedArtistNames?: Iterable<string>;
  direct: FtgRankingCandidate[];
  onward: Map<string, FtgOnwardRelation[]>;
  excludedArtistIds?: Set<string>;
  requireSearchEligible?: boolean;
  limit: number;
}): FtgDestinationCandidate[] {
  const excluded = input.excludedArtistIds ?? new Set<string>();
  const visible = (candidate: FtgRankingCandidate): boolean =>
    candidate.destinationOutputStatus === "proven_output" &&
    (!input.requireSearchEligible || candidate.searchEligible);
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
    if (excluded.has(bridge.targetArtistId)) return;
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
  return suppressArtistFamilyVariants(values, input.visitedArtistNames ?? [])
    .slice(0, Math.max(0, input.limit));
}

const RECORDING_EVIDENCE = new Set(["artist_credit", "instrument", "vocal"]);

function isDedicatedDestination(candidate: FtgRankingCandidate): boolean {
  if (candidate.destinationOutputStatus !== "proven_output" || candidate.productCount <= 0) {
    return false;
  }
  if (candidate.entityType === "group") return true;

  // Membership alone turns a person into a bridge, not a destination. A
  // person may be a destination when the data also proves participation in a
  // recording, which is the narrowest available evidence of an own musical
  // oeuvre and a concrete route to the destination.
  return candidate.factual && candidate.factualMechanisms.some((mechanism) =>
    RECORDING_EVIDENCE.has(mechanism),
  );
}

function isDedicatedSoloFallback(candidate: FtgRankingCandidate): boolean {
  return candidate.destinationOutputStatus === "proven_output" &&
    candidate.productCount > 0 &&
    candidate.entityType === "person" &&
    !candidate.factual &&
    candidate.similarity &&
    candidate.similarityPosition !== null;
}

function compareDedicatedFamilyRepresentatives(
  left: FtgDestinationCandidate,
  right: FtgDestinationCandidate,
): number {
  const factualOrder = Number(!left.factual) - Number(!right.factual);
  if (factualOrder) return factualOrder;

  const recordingOrder = Number(
    !left.factualMechanisms.some((mechanism) => RECORDING_EVIDENCE.has(mechanism)),
  ) - Number(
    !right.factualMechanisms.some((mechanism) => RECORDING_EVIDENCE.has(mechanism)),
  );
  if (recordingOrder) return recordingOrder;

  const similarityOrder = Number(!left.similarity) - Number(!right.similarity);
  if (similarityOrder) return similarityOrder;

  const leftPosition = left.similarityPosition ?? 2 ** 31;
  const rightPosition = right.similarityPosition ?? 2 ** 31;
  if (leftPosition !== rightPosition) return leftPosition - rightPosition;

  return compareStrings(left.displayName.toLowerCase(), right.displayName.toLowerCase());
}

function collapseDedicatedNameFamilies(
  candidates: FtgDestinationCandidate[],
  visitedArtistNames: Iterable<string>,
): FtgDestinationCandidate[] {
  const visitedFamilyKeys = new Set(
    [...visitedArtistNames].map((name) =>
      dedicatedFamilyKey(name) ?? normalizeArtistFamilyName(name).replace(/^the /, ""),
    ),
  );
  const representatives = new Map<string, FtgDestinationCandidate>();
  const unchanged: FtgDestinationCandidate[] = [];

  for (const candidate of candidates) {
    if (candidate.entityType !== "group") {
      unchanged.push(candidate);
      continue;
    }

    const key = dedicatedFamilyKey(candidate.displayName);
    const normalizedName = normalizeArtistFamilyName(candidate.displayName).replace(/^the /, "");
    if (!key) {
      if (visitedFamilyKeys.has(normalizedName)) continue;
      unchanged.push(candidate);
      continue;
    }
    if (visitedFamilyKeys.has(key)) continue;

    const previous = representatives.get(key);
    if (!previous || compareDedicatedFamilyRepresentatives(candidate, previous) < 0) {
      representatives.set(key, candidate);
    }
  }

  return [...unchanged, ...representatives.values()];
}

function suppressDedicatedArtistFamilyVariants(
  candidates: FtgDestinationCandidate[],
  visitedArtistNames: Iterable<string>,
): FtgDestinationCandidate[] {
  return suppressArtistFamilyVariants(
    collapseDedicatedNameFamilies(candidates, visitedArtistNames),
    [],
  );
}

function compareDedicatedDestinations(
  left: FtgDestinationCandidate,
  right: FtgDestinationCandidate,
): number {
  const routeClass = (candidate: FtgDestinationCandidate): number => {
    if (candidate.dedicatedSoloFallback) return 3;
    if (candidate.direct && candidate.factual) return 0;
    if (candidate.factual && candidate.factualMechanisms.some((mechanism) =>
      RECORDING_EVIDENCE.has(mechanism),
    )) return 0;
    if (candidate.direct && candidate.entityType === "group") return 1;
    if (candidate.factual) return 2;
    return 3;
  };
  const routeOrder = routeClass(left) - routeClass(right);
  if (routeOrder) return routeOrder;

  const independentBridgeOrder = (right.bridgeCount ?? 0) - (left.bridgeCount ?? 0);
  if (independentBridgeOrder) return independentBridgeOrder;

  const factualOrder = Number(!left.factual) - Number(!right.factual);
  if (factualOrder) return factualOrder;

  const entityOrder = Number(left.entityType !== "group") - Number(right.entityType !== "group");
  if (entityOrder) return entityOrder;

  const directOrder = Number(!left.direct) - Number(!right.direct);
  if (directOrder) return directOrder;

  const onwardOrder = right.onwardCount - left.onwardCount;
  if (onwardOrder) return onwardOrder;

  if (left.v3Order !== right.v3Order) return left.v3Order - right.v3Order;
  return (
    compareStrings(left.displayName.toLowerCase(), right.displayName.toLowerCase()) ||
    compareStrings(left.musicbrainzArtistMbid, right.musicbrainzArtistMbid)
  );
}

function selectWithBridgeDiversity(
  candidates: FtgDestinationCandidate[],
  limit: number,
  maxSimilarityOnly: number,
): FtgDestinationCandidate[] {
  const selected: FtgDestinationCandidate[] = [];
  const selectedIds = new Set<string>();
  const bridgeCounts = new Map<string, number>();
  let similarityOnlyCount = 0;
  const isSimilarityOnly = (candidate: FtgDestinationCandidate): boolean =>
    !candidate.factual || candidate.dedicatedSoloFallback === true;

  const addFromPass = (maxPerBridge: number): void => {
    for (const candidate of candidates) {
      if (selected.length >= limit || selectedIds.has(candidate.targetArtistId)) continue;
      if (isSimilarityOnly(candidate) && similarityOnlyCount >= maxSimilarityOnly) continue;
      if (candidate.bridgeId) {
        const count = bridgeCounts.get(candidate.bridgeId) ?? 0;
        if (count >= maxPerBridge) continue;
        bridgeCounts.set(candidate.bridgeId, count + 1);
      }
      selected.push(candidate);
      selectedIds.add(candidate.targetArtistId);
      if (isSimilarityOnly(candidate)) similarityOnlyCount += 1;
    }
  };

  addFromPass(1);
  if (selected.length < limit) addFromPass(2);
  const hasUnrepresentedBridge = candidates.some(
    (candidate) => candidate.bridgeId && !bridgeCounts.has(candidate.bridgeId),
  );
  if (selected.length < limit && !hasUnrepresentedBridge) addFromPass(Number.MAX_SAFE_INTEGER);
  return selected;
}

/**
 * Dedicated-route selection. The search surface intentionally continues to
 * use selectNextDestinations: this selector changes only the five standalone
 * destinations on /follow-the-groove/[...trail].
 */
export function selectDedicatedDestinations(input: {
  sourceArtistId: string;
  visitedArtistNames?: Iterable<string>;
  direct: FtgRankingCandidate[];
  onward: Map<string, FtgOnwardRelation[]>;
  excludedArtistIds?: Set<string>;
  limit: number;
}): FtgDestinationCandidate[] {
  const excluded = input.excludedArtistIds ?? new Set<string>();
  const directIds = new Set(input.direct.map((candidate) => candidate.targetArtistId));
  const directById = new Map(input.direct.map((candidate) => [candidate.targetArtistId, candidate]));
  const byDestination = new Map<string, FtgDestinationCandidate>();
  const bridgeIdsByDestination = new Map<string, Set<string>>();

  const onwardDestinations = (bridgeId: string): FtgOnwardRelation[] =>
    (input.onward.get(bridgeId) ?? []).filter(isDedicatedDestination);

  input.direct.forEach((candidate) => {
    if (
      candidate.targetArtistId === input.sourceArtistId ||
      excluded.has(candidate.targetArtistId) ||
      !isDedicatedDestination(candidate)
    ) return;

    byDestination.set(candidate.targetArtistId, {
      ...candidate,
      direct: true,
      bridgeId: null,
      bridgeName: null,
      onwardCount: new Set(
        onwardDestinations(candidate.targetArtistId)
          .map((relation) => relation.targetArtistId)
          .filter((targetId) => targetId !== input.sourceArtistId && !excluded.has(targetId)),
      ).size,
      v3Order: candidate.similarityPosition ?? 2 ** 31,
      bridgeCount: 0,
    });
  });

  // Every direct relation may be a bridge, including an unknown or
  // proven_bridge_only person. The bridge itself is never made visible here;
  // only its proven musical destination can enter the card pool.
  input.direct.forEach((bridge) => {
    if (excluded.has(bridge.targetArtistId)) return;
    const onward = onwardDestinations(bridge.targetArtistId);
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
        v3Order: bridge.similarityPosition ?? 2 ** 31,
        bridgeCount: 1,
      };
      const bridgeIds = bridgeIdsByDestination.get(destinationId) ?? new Set<string>();
      bridgeIds.add(bridge.targetArtistId);
      bridgeIdsByDestination.set(destinationId, bridgeIds);
      const previous = byDestination.get(destinationId);
      if (!previous || compareDedicatedDestinations(indirect, previous) < 0) {
        byDestination.set(destinationId, indirect);
      }
    }
  });

  // A proven solo artist may be a dedicated destination only when the
  // current data also gives a clear, evidence-backed route through a group.
  // This prevents similarity-only people from resurfacing as unexplained
  // cards while still allowing Miles Davis -> an ensemble -> Coltrane.
  input.direct.forEach((bridge) => {
    if (bridge.entityType !== "group" || excluded.has(bridge.targetArtistId)) return;
    for (const relation of input.onward.get(bridge.targetArtistId) ?? []) {
      const directCandidate = directById.get(relation.targetArtistId);
      if (
        !directCandidate ||
        !isDedicatedSoloFallback(directCandidate) ||
        relation.entityType !== "person" ||
        relation.destinationOutputStatus !== "proven_output" ||
        !relation.factual ||
        !relation.factualMechanisms.includes("membership") ||
        relation.targetArtistId === input.sourceArtistId ||
        excluded.has(relation.targetArtistId)
      ) continue;

      const fallback: FtgDestinationCandidate = {
        ...directCandidate,
        factual: true,
        factualMechanisms: relation.factualMechanisms,
        allowedEvidenceCount: relation.allowedEvidenceCount,
        uniqueRecordingCount: relation.uniqueRecordingCount,
        direct: false,
        bridgeId: bridge.targetArtistId,
        bridgeName: bridge.displayName,
        onwardCount: 0,
        v3Order: directCandidate.similarityPosition ?? 2 ** 31,
        bridgeCount: 1,
        dedicatedSoloFallback: true,
      };
      const bridgeIds = bridgeIdsByDestination.get(relation.targetArtistId) ?? new Set<string>();
      bridgeIds.add(bridge.targetArtistId);
      bridgeIdsByDestination.set(relation.targetArtistId, bridgeIds);
      const previous = byDestination.get(relation.targetArtistId);
      if (!previous || compareDedicatedDestinations(fallback, previous) < 0) {
        byDestination.set(relation.targetArtistId, fallback);
      }
    }
  });

  const ordered = suppressDedicatedArtistFamilyVariants(
    [...byDestination.entries()]
      .map(([destinationId, candidate]) => ({
        ...candidate,
        bridgeCount: bridgeIdsByDestination.get(destinationId)?.size ?? 0,
      }))
      .sort(compareDedicatedDestinations),
    input.visitedArtistNames ?? [],
  ).sort(compareDedicatedDestinations);
  const boundedLimit = Math.max(0, input.limit);
  const factualCount = ordered.filter((candidate) =>
    candidate.factual && !candidate.dedicatedSoloFallback,
  ).length;
  const maxSimilarityOnly = factualCount >= Math.max(0, boundedLimit - 2)
    ? Math.min(2, boundedLimit)
    : boundedLimit;

  return selectWithBridgeDiversity(ordered, boundedLimit, maxSimilarityOnly);
}
