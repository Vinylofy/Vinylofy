import { createSupabaseAdminClient } from "@/lib/supabase/admin";
import { getReasonCodes, rankCandidates, type FtgRankingCandidate } from "./ranking";
import { selectNextDestinations, type FtgOnwardRelation } from "./destination-selection";
import { resolveSearchGrooveSourceFromMatches } from "./search-source";
import { mapReasonLabel, type AllowedEvidence } from "./reasons";
import { isValidTrail } from "./presentation";
import { buildExplainedTrail } from "./trail";
import {
  resolveRepresentativeCovers,
  type ArtistProductLink,
  type CoverProduct,
} from "./representative-cover";
import {
  FTG_MAX_CANDIDATES,
  FTG_MAX_TRAIL_LENGTH,
  type FollowTheGroovePageData,
  type FtgArtistView,
  type FtgEntityType,
  type FtgOutputStatus,
} from "./types";

const BLACKLISTED_FORMAT_LABELS = new Set([
  "CD",
  "POSTER",
  "ACCESSORIES",
  "PHOTOBOOK",
  "BLUERAY",
  "BLURAY",
]);
const FTG_SEARCH_RANKING_POOL_LIMIT = 24;

type ArtistRow = {
  id: string;
  musicbrainz_artist_mbid: string;
  display_name: string;
  entity_type: FtgEntityType;
};
type EdgeRow = { id: string; artist_low_id: string; artist_high_id: string };
type SimilarityRow = {
  target_artist_id: string;
  position: number;
  match_score: number | string;
};
type SimilarityWithSourceRow = SimilarityRow & { source_artist_id: string };
type EvidenceRow = {
  edge_id: string;
  source_artist_id: string;
  target_artist_id: string;
  evidence_kind: "membership" | "artist_credit" | "instrument" | "vocal";
  classification: string;
  ended: boolean | null;
  recording_mbid: string | null;
};
type ProductArtistRow = {
  artist_id: string;
  product_id: string;
  credit_position: number | null;
};
type OutputStatusRow = { artist_id: string; status: FtgOutputStatus };
type ProductRow = {
  id: string;
  artist: string;
  format_label: string | null;
  cover_status: string | null;
  cover_storage_path: string | null;
  cover_review_status: string | null;
  metadata_raw: unknown;
};

function normalizeArtistLabel(value: string): string {
  return value.toLowerCase().replace(/\s+/g, " ").trim();
}

function escapeIlike(value: string): string {
  return value.replace(/[\\%_]/g, "\\$&");
}

function isAllowedProduct(product: ProductRow): boolean {
  return !BLACKLISTED_FORMAT_LABELS.has((product.format_label ?? "").trim().toUpperCase());
}

function buildSearchHref(searchArtist: string): string {
  const params = new URLSearchParams({
    q: searchArtist,
    artist_filter: searchArtist,
  });
  return `/search?${params.toString()}`;
}

function chunks<T>(values: T[], size: number): T[][] {
  const result: T[][] = [];
  for (let index = 0; index < values.length; index += size) {
    result.push(values.slice(index, index + size));
  }
  return result;
}

async function unwrap<T>(
  request: PromiseLike<{ data: unknown; error: unknown }>,
): Promise<T[]> {
  const { data, error } = await request;
  if (error) throw error;
  return (data ?? []) as T[];
}

function getFactualTarget(edge: EdgeRow, sourceArtistId: string): string {
  return edge.artist_low_id === sourceArtistId ? edge.artist_high_id : edge.artist_low_id;
}

async function loadPresentationData(
  artists: ArtistRow[],
): Promise<Map<string, FtgArtistView>> {
  const supabase = createSupabaseAdminClient();
  const artistIds = artists.map((artist) => artist.id);
  const links = await unwrap<ProductArtistRow>(
    supabase
      .from("product_artists")
      .select("artist_id, product_id, credit_position")
      .in("artist_id", artistIds)
      .limit(2000),
  );
  const productIds = [...new Set(links.map((link) => link.product_id))];
  const productChunks = chunks(productIds, 150);

  const [productPages, bestPricePages, freshPricePages] = await Promise.all([
    Promise.all(
      productChunks.map((ids) =>
        unwrap<ProductRow>(
          supabase
            .from("products")
            .select(
              "id, artist, format_label, cover_status, cover_storage_path, cover_review_status, metadata_raw",
            )
            .in("id", ids),
        ),
      ),
    ),
    Promise.all(
      productChunks.map((ids) =>
        unwrap<{ product_id: string; lowest_fresh_price: number | string | null }>(
          supabase
            .from("product_best_prices_v1")
            .select("product_id, lowest_fresh_price")
            .in("product_id", ids),
        ),
      ),
    ),
    Promise.all(
      productChunks.map((ids) =>
        unwrap<{ product_id: string }>(
          supabase
            .from("prices")
            .select("product_id")
            .in("product_id", ids)
            .eq("is_active", true)
            .in("availability", ["in_stock", "unknown"])
            .gte(
              "last_seen_at",
              new Date(Date.now() - 48 * 60 * 60 * 1000).toISOString(),
            ),
        ),
      ),
    ),
  ]);

  const products = productPages.flat();
  const productsById = new Map(products.map((product) => [product.id, product]));
  const currentlyAvailableProductIds = new Set([
    ...bestPricePages
      .flat()
      .filter((row) => row.lowest_fresh_price !== null)
      .map((row) => row.product_id),
    ...freshPricePages.flat().map((row) => row.product_id),
  ]);
  const coverLinks: ArtistProductLink[] = links.map((link) => ({
    artistId: link.artist_id,
    productId: link.product_id,
    creditPosition: link.credit_position,
  }));
  const coverProducts: CoverProduct[] = products.map((product) => ({
    id: product.id,
    artist: product.artist,
    coverStatus: product.cover_status,
    coverStoragePath: product.cover_storage_path,
    coverReviewStatus: product.cover_review_status,
    metadataRaw: product.metadata_raw,
  }));
  const artistNames = new Map(artists.map((artist) => [artist.id, artist.display_name]));
  const covers = resolveRepresentativeCovers(
    artistNames,
    coverLinks,
    coverProducts,
    currentlyAvailableProductIds,
  );

  const result = new Map<string, FtgArtistView>();
  for (const artist of artists) {
    const labelGroups = new Map<string, { label: string; productIds: Set<string> }>();
    for (const link of links) {
      if (link.artist_id !== artist.id) continue;
      const product = productsById.get(link.product_id);
      if (
        !product ||
        !isAllowedProduct(product) ||
        !currentlyAvailableProductIds.has(product.id)
      ) {
        continue;
      }
      const key = normalizeArtistLabel(product.artist);
      if (!key) continue;
      const existing = labelGroups.get(key) ?? { label: product.artist.trim(), productIds: new Set() };
      existing.productIds.add(product.id);
      if (product.artist.trim().length < existing.label.length) {
        existing.label = product.artist.trim();
      }
      labelGroups.set(key, existing);
    }

    const canonical = normalizeArtistLabel(artist.display_name);
    const labels = [...labelGroups.entries()].sort((left, right) => {
      const canonicalOrder = Number(right[0] === canonical) - Number(left[0] === canonical);
      if (canonicalOrder) return canonicalOrder;
      const countOrder = right[1].productIds.size - left[1].productIds.size;
      if (countOrder) return countOrder;
      return left[0].localeCompare(right[0]);
    });
    const selected = labels[0]?.[1] ?? null;
    const searchArtist = selected?.label ?? null;

    result.set(artist.id, {
      id: artist.id,
      mbid: artist.musicbrainz_artist_mbid,
      name: artist.display_name,
      entityType: artist.entity_type,
      representativeCover: covers.get(artist.id)!,
      productCount: selected?.productIds.size ?? 0,
      searchArtist,
      searchHref: searchArtist ? buildSearchHref(searchArtist) : null,
    });
  }
  return result;
}

async function loadTrailExplanations(trailArtists: ArtistRow[]): Promise<(string | null)[]> {
  if (trailArtists.length < 2) return trailArtists.map(() => null);
  const supabase = createSupabaseAdminClient();
  const sourceIds = trailArtists.slice(0, -1).map((artist) => artist.id);
  const [directEdges, directSimilarities] = await Promise.all([
    unwrap<EdgeRow>(
      supabase
        .from("artist_edges")
        .select("id, artist_low_id, artist_high_id")
        .or(`artist_low_id.in.(${sourceIds.join(",")}),artist_high_id.in.(${sourceIds.join(",")})`),
    ),
    unwrap<SimilarityWithSourceRow>(
      supabase
        .from("artist_similarity")
        .select("source_artist_id, target_artist_id, position, match_score")
        .in("source_artist_id", sourceIds)
        .eq("resolution_status", "resolved")
        .not("target_artist_id", "is", null),
    ),
  ]);
  const bridgeIds = [...new Set([
    ...directEdges.flatMap((edge) => [edge.artist_low_id, edge.artist_high_id]),
    ...directSimilarities.map((row) => row.target_artist_id),
  ].filter((id) => !sourceIds.includes(id)))];
  const [onwardEdges, onwardSimilarities] = bridgeIds.length
    ? await Promise.all([
        unwrap<EdgeRow>(
          supabase
            .from("artist_edges")
            .select("id, artist_low_id, artist_high_id")
            .or(`artist_low_id.in.(${bridgeIds.join(",")}),artist_high_id.in.(${bridgeIds.join(",")})`),
        ),
        unwrap<SimilarityWithSourceRow>(
          supabase
            .from("artist_similarity")
            .select("source_artist_id, target_artist_id, position, match_score")
            .in("source_artist_id", bridgeIds)
            .eq("resolution_status", "resolved")
            .not("target_artist_id", "is", null),
        ),
      ])
    : [[], []] as [EdgeRow[], SimilarityWithSourceRow[]];
  const edgeById = new Map(
    [...directEdges, ...onwardEdges].map((edge) => [edge.id, edge]),
  );
  const allEdges = [...edgeById.values()];
  const allSimilarities = [...directSimilarities, ...onwardSimilarities];
  const evidence = allEdges.length
    ? await unwrap<EvidenceRow>(
        supabase
          .from("artist_relation_evidence")
          .select("edge_id, source_artist_id, target_artist_id, evidence_kind, classification, ended, recording_mbid")
          .in("edge_id", allEdges.map((edge) => edge.id))
          .eq("classification", "allowed"),
      )
    : [];
  const artistIds = [...new Set([
    ...trailArtists.map((artist) => artist.id),
    ...allEdges.flatMap((edge) => [edge.artist_low_id, edge.artist_high_id]),
    ...allSimilarities.flatMap((row) => [row.source_artist_id, row.target_artist_id]),
  ])];
  const [artists, statuses] = await Promise.all([
    unwrap<ArtistRow>(
      supabase
        .from("artists")
        .select("id, musicbrainz_artist_mbid, display_name, entity_type")
        .in("id", artistIds),
    ),
    unwrap<OutputStatusRow>(
      supabase
        .from("artist_output_status")
        .select("artist_id, status")
        .in("artist_id", artistIds),
    ),
  ]);
  const artistsById = new Map(artists.map((artist) => [artist.id, artist]));
  const statusById = new Map(statuses.map((row) => [row.artist_id, row.status]));

  const relationEvidence = (sourceId: string, targetId: string) => evidence.filter(
    (row) =>
      (row.source_artist_id === sourceId && row.target_artist_id === targetId) ||
      (row.source_artist_id === targetId && row.target_artist_id === sourceId),
  );
  const candidateFor = (sourceId: string, targetId: string): FtgRankingCandidate | null => {
    const target = artistsById.get(targetId);
    if (!target) return null;
    const factualRows = relationEvidence(sourceId, targetId);
    const similarity = allSimilarities.find(
      (row) => row.source_artist_id === sourceId && row.target_artist_id === targetId,
    );
    if (factualRows.length === 0 && !similarity) return null;
    return {
      sourceArtistId: sourceId,
      targetArtistId: targetId,
      displayName: target.display_name,
      musicbrainzArtistMbid: target.musicbrainz_artist_mbid,
      entityType: target.entity_type,
      factual: factualRows.length > 0,
      factualMechanisms: [...new Set(factualRows.map((row) => row.evidence_kind))],
      allowedEvidenceCount: factualRows.length,
      uniqueRecordingCount: new Set(factualRows.map((row) => row.recording_mbid).filter(Boolean)).size,
      similarity: Boolean(similarity),
      similarityPosition: similarity?.position ?? null,
      similarityMatchScore: similarity ? Number(similarity.match_score) : null,
      searchEligible: false,
      productCount: 0,
      destinationOutputStatus: statusById.get(targetId) ?? "unknown",
    };
  };
  const targetIdsFor = (sourceId: string): string[] => [...new Set([
    ...allEdges.flatMap((edge) => {
      if (edge.artist_low_id === sourceId) return [edge.artist_high_id];
      if (edge.artist_high_id === sourceId) return [edge.artist_low_id];
      return [];
    }),
    ...allSimilarities
      .filter((row) => row.source_artist_id === sourceId)
      .map((row) => row.target_artist_id),
  ])];

  const explanations: (string | null)[] = [null];
  for (let index = 1; index < trailArtists.length; index += 1) {
    const source = trailArtists[index - 1];
    const destination = trailArtists[index];
    const direct = rankCandidates(
      targetIdsFor(source.id).flatMap((id) => {
        const candidate = candidateFor(source.id, id);
        return candidate ? [candidate] : [];
      }),
      { mode: "trail", limit: targetIdsFor(source.id).length },
    );
    const onward = new Map<string, FtgOnwardRelation[]>();
    for (const bridge of direct) {
      const relations = targetIdsFor(bridge.targetArtistId).flatMap((id) => {
        const candidate = candidateFor(bridge.targetArtistId, id);
        return candidate ? [candidate] : [];
      });
      onward.set(bridge.targetArtistId, relations);
    }
    const selected = selectNextDestinations({
      sourceArtistId: source.id,
      visitedArtistNames: [source.display_name],
      direct,
      onward,
      limit: Math.max(FTG_MAX_CANDIDATES, direct.length + artistIds.length),
    }).find((candidate) => candidate.targetArtistId === destination.id);
    if (!selected) {
      explanations.push(null);
      continue;
    }
    if (selected.bridgeName) {
      explanations.push(`Via ${selected.bridgeName}`);
      continue;
    }
    const reasonCode = getReasonCodes(selected)[0] ?? "similar_artist";
    explanations.push(mapReasonLabel({
      reasonCode,
      activeArtist: { id: source.id, name: source.display_name, entityType: source.entity_type },
      candidate: { id: destination.id, entityType: destination.entity_type },
      evidence: relationEvidence(source.id, destination.id).map((row) => ({
        sourceArtistId: row.source_artist_id,
        targetArtistId: row.target_artist_id,
        evidenceKind: row.evidence_kind,
        ended: row.ended,
      })),
    }));
  }
  return explanations;
}

export async function getFollowTheGroovePage(input: {
  trailMbids: string[];
  mode?: "trail" | "search";
  limit?: number;
  artistName?: string;
}): Promise<FollowTheGroovePageData | null> {
  const mode = input.mode ?? "trail";
  const limit = Math.min(Math.max(input.limit ?? FTG_MAX_CANDIDATES, 0), FTG_MAX_CANDIDATES);
  if (mode === "trail" && !isValidTrail(input.trailMbids, FTG_MAX_TRAIL_LENGTH)) return null;
  if (mode === "search" && input.artistName?.trim() === undefined && input.trailMbids.length === 0) return null;

  const supabase = createSupabaseAdminClient();
  const sourceRows = mode === "search" && input.artistName
    ? await unwrap<ArtistRow>(
        supabase
          .from("artists")
          .select("id, musicbrainz_artist_mbid, display_name, entity_type")
          .ilike("display_name", escapeIlike(input.artistName.trim()))
          .limit(2),
      )
    : [];
  if (mode === "search" && sourceRows.length !== 1) return null;
  const resolvedTrailMbids = mode === "search"
    ? [sourceRows[0]?.musicbrainz_artist_mbid ?? input.trailMbids.at(-1) ?? ""]
    : input.trailMbids;
  const activeArtistMbid = resolvedTrailMbids.at(-1)!;
  const trailRows = mode === "search" && sourceRows.length === 1
    ? sourceRows
    : await unwrap<ArtistRow>(
        supabase
          .from("artists")
          .select("id, musicbrainz_artist_mbid, display_name, entity_type")
          .in("musicbrainz_artist_mbid", [...new Set(resolvedTrailMbids)]),
      );
  const artistsByMbid = new Map(
    trailRows.map((artist) => [artist.musicbrainz_artist_mbid.toLowerCase(), artist]),
  );
  const activeArtist = artistsByMbid.get(activeArtistMbid.toLowerCase());
  if (!activeArtist || resolvedTrailMbids.some((mbid) => !artistsByMbid.has(mbid.toLowerCase()))) {
    return null;
  }

  const [edges, similarities] = await Promise.all([
    unwrap<EdgeRow>(
      supabase
        .from("artist_edges")
        .select("id, artist_low_id, artist_high_id")
        .or(`artist_low_id.eq.${activeArtist.id},artist_high_id.eq.${activeArtist.id}`),
    ),
    unwrap<SimilarityRow>(
      supabase
        .from("artist_similarity")
        .select("target_artist_id, position, match_score")
        .eq("source_artist_id", activeArtist.id)
        .eq("resolution_status", "resolved")
        .not("target_artist_id", "is", null),
    ),
  ]);
  const candidateIds = [
    ...new Set([
      ...edges.map((edge) => getFactualTarget(edge, activeArtist.id)),
      ...similarities.map((row) => row.target_artist_id),
    ]),
  ];

  const [candidateArtists, evidence] = await Promise.all([
    candidateIds.length
      ? unwrap<ArtistRow>(
          supabase
            .from("artists")
            .select("id, musicbrainz_artist_mbid, display_name, entity_type")
            .in("id", candidateIds),
        )
      : Promise.resolve([]),
    edges.length
      ? unwrap<EvidenceRow>(
          supabase
            .from("artist_relation_evidence")
            .select(
              "edge_id, source_artist_id, target_artist_id, evidence_kind, classification, ended, recording_mbid",
            )
            .in("edge_id", edges.map((edge) => edge.id))
            .eq("classification", "allowed"),
        )
      : Promise.resolve([]),
  ]);
  const [onwardEdges, onwardSimilarities] = await Promise.all([
    candidateIds.length
      ? unwrap<EdgeRow>(
          supabase
            .from("artist_edges")
            .select("id, artist_low_id, artist_high_id")
            .or(
              `artist_low_id.in.(${candidateIds.join(",")}),artist_high_id.in.(${candidateIds.join(",")})`,
            ),
        )
      : Promise.resolve([]),
    candidateIds.length
      ? unwrap<SimilarityRow & { source_artist_id: string }>(
          supabase
            .from("artist_similarity")
            .select("source_artist_id, target_artist_id, position, match_score")
            .in("source_artist_id", candidateIds)
            .eq("resolution_status", "resolved")
            .not("target_artist_id", "is", null),
        )
      : Promise.resolve([]),
  ]);
  const onwardEdgeIds = [...new Set(onwardEdges.map((edge) => edge.id))];
  const onwardEvidence = onwardEdgeIds.length
    ? await unwrap<EvidenceRow>(
        supabase
          .from("artist_relation_evidence")
          .select(
            "edge_id, source_artist_id, target_artist_id, evidence_kind, classification, ended, recording_mbid",
          )
          .in("edge_id", onwardEdgeIds)
          .eq("classification", "allowed"),
      )
    : [];
  const onwardTargetIds = [
    ...new Set([
      ...onwardEdges.flatMap((edge) => [edge.artist_low_id, edge.artist_high_id]),
      ...onwardSimilarities.map((row) => row.target_artist_id),
    ]),
  ];
  const onwardArtists = onwardTargetIds.length
    ? await unwrap<ArtistRow>(
        supabase
          .from("artists")
          .select("id, musicbrainz_artist_mbid, display_name, entity_type")
          .in("id", onwardTargetIds),
      )
    : [];
  const destinationArtistIds = [...new Set([...candidateIds, ...onwardTargetIds])];
  const outputStatuses = destinationArtistIds.length
    ? await unwrap<OutputStatusRow>(
        supabase
          .from("artist_output_status")
          .select("artist_id, status")
          .in("artist_id", destinationArtistIds),
      )
    : [];
  const outputStatusByArtistId = new Map(
    outputStatuses.map((row) => [row.artist_id, row.status]),
  );
  const candidatesById = new Map(candidateArtists.map((artist) => [artist.id, artist]));
  const searchPresentation = mode === "search"
    ? await loadPresentationData([activeArtist, ...candidateArtists, ...onwardArtists])
    : null;
  const similaritiesByTarget = new Map(similarities.map((row) => [row.target_artist_id, row]));
  const edgeTargetIds = new Set(edges.map((edge) => getFactualTarget(edge, activeArtist.id)));

  const rankingInput: FtgRankingCandidate[] = candidateIds.flatMap((candidateId) => {
    const candidate = candidatesById.get(candidateId);
    if (!candidate) return [];
    const candidateEvidence = evidence.filter(
      (row) =>
        (row.source_artist_id === activeArtist.id && row.target_artist_id === candidateId) ||
        (row.source_artist_id === candidateId && row.target_artist_id === activeArtist.id),
    );
    const mechanisms = [...new Set(candidateEvidence.map((row) => row.evidence_kind))];
    const similarity = similaritiesByTarget.get(candidateId);
    return [{
      sourceArtistId: activeArtist.id,
      targetArtistId: candidateId,
      displayName: candidate.display_name,
      musicbrainzArtistMbid: candidate.musicbrainz_artist_mbid,
      entityType: candidate.entity_type,
      factual: edgeTargetIds.has(candidateId),
      factualMechanisms: mechanisms,
      allowedEvidenceCount: candidateEvidence.length,
      uniqueRecordingCount: new Set(
        candidateEvidence.map((row) => row.recording_mbid).filter(Boolean),
      ).size,
      similarity: Boolean(similarity),
      similarityPosition: similarity?.position ?? null,
      similarityMatchScore: similarity ? Number(similarity.match_score) : null,
      searchEligible: mode === "search" &&
        outputStatusByArtistId.get(candidate.id) === "proven_output" &&
        (searchPresentation?.get(candidate.id)?.productCount ?? 0) > 0 &&
        searchPresentation?.get(candidate.id)?.searchHref !== null,
      productCount: searchPresentation?.get(candidate.id)?.productCount ?? 0,
      destinationOutputStatus: outputStatusByArtistId.get(candidate.id) ?? "unknown",
    }];
  });
  const ranked = rankCandidates(rankingInput, {
    mode,
    limit: mode === "search"
      ? Math.min(rankingInput.length, FTG_SEARCH_RANKING_POOL_LIMIT)
      : rankingInput.length,
  });
  const onwardArtistsById = new Map(onwardArtists.map((artist) => [artist.id, artist]));
  const onwardSimilaritiesBySource = new Map<string, typeof onwardSimilarities>();
  for (const similarity of onwardSimilarities) {
    const current = onwardSimilaritiesBySource.get(similarity.source_artist_id) ?? [];
    current.push(similarity);
    onwardSimilaritiesBySource.set(similarity.source_artist_id, current);
  }
  const onwardEdgesBySource = new Map<string, EdgeRow[]>();
  for (const edge of onwardEdges) {
    for (const sourceId of [edge.artist_low_id, edge.artist_high_id]) {
      if (!candidateIds.includes(sourceId)) continue;
      const current = onwardEdgesBySource.get(sourceId) ?? [];
      current.push(edge);
      onwardEdgesBySource.set(sourceId, current);
    }
  }
  const onwardRelations = new Map<string, FtgOnwardRelation[]>();
  for (const bridge of ranked) {
    const relations: FtgOnwardRelation[] = [];
    const bridgeEdges = onwardEdgesBySource.get(bridge.targetArtistId) ?? [];
    const bridgeEdgeTargets = new Set(
      bridgeEdges.map((edge) => getFactualTarget(edge, bridge.targetArtistId)),
    );
    const bridgeSimilarities = onwardSimilaritiesBySource.get(bridge.targetArtistId) ?? [];
    const bridgeSimilarityByTarget = new Map(
      bridgeSimilarities.map((similarity) => [similarity.target_artist_id, similarity]),
    );
    const relationIds = [...new Set([...bridgeEdgeTargets, ...bridgeSimilarityByTarget.keys()])];
    for (const targetId of relationIds) {
      const target = onwardArtistsById.get(targetId);
      if (!target || targetId === activeArtist.id) continue;
      const targetEvidence = onwardEvidence.filter(
        (row) =>
          row.edge_id &&
          bridgeEdges.some((edge) => edge.id === row.edge_id) &&
          ((row.source_artist_id === bridge.targetArtistId && row.target_artist_id === targetId) ||
            (row.target_artist_id === bridge.targetArtistId && row.source_artist_id === targetId)),
      );
      const factual = targetEvidence.length > 0 && bridgeEdgeTargets.has(targetId);
      const similarity = bridgeSimilarityByTarget.get(targetId);
      if (!factual && !similarity) continue;
      relations.push({
        sourceArtistId: bridge.targetArtistId,
        targetArtistId: targetId,
        displayName: target.display_name,
        musicbrainzArtistMbid: target.musicbrainz_artist_mbid,
        entityType: target.entity_type,
        factual,
        factualMechanisms: [...new Set(targetEvidence.map((row) => row.evidence_kind))],
        allowedEvidenceCount: targetEvidence.length,
        uniqueRecordingCount: new Set(targetEvidence.map((row) => row.recording_mbid).filter(Boolean)).size,
        similarity: Boolean(similarity),
        similarityPosition: similarity?.position ?? null,
        similarityMatchScore: similarity ? Number(similarity.match_score) : null,
        searchEligible: mode === "search" &&
          outputStatusByArtistId.get(target.id) === "proven_output" &&
          (searchPresentation?.get(target.id)?.productCount ?? 0) > 0 &&
          searchPresentation?.get(target.id)?.searchHref !== null,
        productCount: searchPresentation?.get(target.id)?.productCount ?? 0,
        destinationOutputStatus: outputStatusByArtistId.get(target.id) ?? "unknown",
      });
    }
    onwardRelations.set(bridge.targetArtistId, relations);
  }
  const selected = selectNextDestinations({
    sourceArtistId: activeArtist.id,
    visitedArtistNames: trailRows.map((artist) => artist.display_name),
    direct: ranked,
    onward: onwardRelations,
    excludedArtistIds: new Set(
      resolvedTrailMbids
        .slice(0, -1)
        .map((mbid) => artistsByMbid.get(mbid.toLowerCase())?.id)
        .filter((id): id is string => Boolean(id)),
    ),
    requireSearchEligible: mode === "search",
    limit,
  });
  const selectedArtists = selected.flatMap((candidate) => {
    const artist = candidatesById.get(candidate.targetArtistId) ?? onwardArtistsById.get(candidate.targetArtistId);
    return artist ? [artist] : [];
  });
  const views = await loadPresentationData([activeArtist, ...selectedArtists]);
  const activeView = views.get(activeArtist.id);
  if (!activeView) return null;

  const allowedEvidence: AllowedEvidence[] = evidence
    .filter((row) =>
      row.classification === "allowed" &&
      ["membership", "artist_credit", "instrument", "vocal"].includes(row.evidence_kind),
    )
    .map((row) => ({
      sourceArtistId: row.source_artist_id,
      targetArtistId: row.target_artist_id,
      evidenceKind: row.evidence_kind,
      ended: row.ended,
    }));

  const candidateViews = selected.map((candidate, index) => {
    const view = views.get(candidate.targetArtistId);
    if (!view) return null;
    const reasonCode = getReasonCodes(candidate)[0] ?? "similar_artist";
    if (!reasonCode) {
      throw new Error(`Candidate ${candidate.targetArtistId} has no supported V1 reason`);
    }
    return {
      ...view,
      rank: index + 1,
      reasonCode,
      bridgeName: candidate.bridgeName,
      reasonLabel: candidate.bridgeName ? `Via ${candidate.bridgeName}` : mapReasonLabel({
        reasonCode,
        activeArtist: {
          id: activeArtist.id,
          name: activeArtist.display_name,
          entityType: activeArtist.entity_type,
        },
        candidate: { id: view.id, entityType: view.entityType },
        evidence: allowedEvidence,
      }),
    };
  }).filter((candidate): candidate is NonNullable<typeof candidate> => Boolean(candidate));
  const orderedTrailArtists = resolvedTrailMbids.map(
    (mbid) => artistsByMbid.get(mbid.toLowerCase())!,
  );
  const trailExplanations = mode === "trail"
    ? await loadTrailExplanations(orderedTrailArtists)
    : orderedTrailArtists.map(() => null);

  return {
    artist: activeView,
    candidates: candidateViews,
    trail: buildExplainedTrail(
      orderedTrailArtists.map((artist) => ({
        mbid: artist.musicbrainz_artist_mbid,
        name: artist.display_name,
      })),
      trailExplanations,
    ),
  };
}

export async function resolveSearchGrooveSource(input: {
  query: string;
  artistFilter?: string;
  resultArtistNames: string[];
}): Promise<string | null> {
  if (input.artistFilter?.trim()) return input.artistFilter.trim();
  if (!input.query.trim()) return null;
  const supabase = createSupabaseAdminClient();
  const exactRows = await unwrap<{ display_name: string }>(
    supabase
      .from("artists")
      .select("display_name")
      .ilike("display_name", escapeIlike(input.query.trim()))
      .limit(2),
  );
  return resolveSearchGrooveSourceFromMatches({
    query: input.query,
    exactArtistNames: exactRows.map((row) => row.display_name),
    resultArtistNames: input.resultArtistNames,
  });
}
