import { createSupabaseAdminClient } from "@/lib/supabase/admin";
import { getReasonCodes, rankCandidates, type FtgRankingCandidate } from "./ranking";
import { mapReasonLabel, type AllowedEvidence } from "./reasons";
import { isValidTrail } from "./presentation";
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
} from "./types";

const BLACKLISTED_FORMAT_LABELS = new Set([
  "CD",
  "POSTER",
  "ACCESSORIES",
  "PHOTOBOOK",
  "BLUERAY",
  "BLURAY",
]);

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

export async function getFollowTheGroovePage(input: {
  trailMbids: string[];
  mode?: "trail";
  limit?: number;
}): Promise<FollowTheGroovePageData | null> {
  const mode = input.mode ?? "trail";
  const limit = Math.min(Math.max(input.limit ?? FTG_MAX_CANDIDATES, 0), FTG_MAX_CANDIDATES);
  if (mode !== "trail" || !isValidTrail(input.trailMbids, FTG_MAX_TRAIL_LENGTH)) return null;

  const activeArtistMbid = input.trailMbids.at(-1)!;
  const supabase = createSupabaseAdminClient();
  const trailRows = await unwrap<ArtistRow>(
    supabase
      .from("artists")
      .select("id, musicbrainz_artist_mbid, display_name, entity_type")
      .in("musicbrainz_artist_mbid", [...new Set(input.trailMbids)]),
  );
  const artistsByMbid = new Map(
    trailRows.map((artist) => [artist.musicbrainz_artist_mbid.toLowerCase(), artist]),
  );
  const activeArtist = artistsByMbid.get(activeArtistMbid.toLowerCase());
  if (!activeArtist || input.trailMbids.some((mbid) => !artistsByMbid.has(mbid.toLowerCase()))) {
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
  const candidatesById = new Map(candidateArtists.map((artist) => [artist.id, artist]));
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
      searchEligible: false,
      productCount: 0,
    }];
  });
  const ranked = rankCandidates(rankingInput, { mode, limit });
  const rankedArtists = ranked.map((candidate) => candidatesById.get(candidate.targetArtistId)!);
  const views = await loadPresentationData([activeArtist, ...rankedArtists]);
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

  const candidateViews = ranked.map((candidate, index) => {
    const view = views.get(candidate.targetArtistId)!;
    const reasonCode = getReasonCodes(candidate)[0];
    if (!reasonCode) {
      throw new Error(`Candidate ${candidate.targetArtistId} has no supported V1 reason`);
    }
    return {
      ...view,
      rank: index + 1,
      reasonCode,
      reasonLabel: mapReasonLabel({
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
  });

  return {
    artist: activeView,
    candidates: candidateViews,
    trail: input.trailMbids.map((mbid) => {
      const artist = artistsByMbid.get(mbid.toLowerCase())!;
      return { mbid: artist.musicbrainz_artist_mbid, name: artist.display_name };
    }),
  };
}
