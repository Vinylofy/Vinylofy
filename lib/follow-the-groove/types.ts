export const FTG_MAX_CANDIDATES = 5;
export const FTG_MAX_TRAIL_LENGTH = 24;

export type FtgEntityType = "person" | "group";
export type FtgOutputStatus = "proven_output" | "unknown" | "proven_bridge_only";

export type FtgReasonCode =
  | "factual_and_similarity"
  | "membership"
  | "recording_collaboration"
  | "similar_artist";

export type RepresentativeCover = {
  src: string;
  isPlaceholder: boolean;
  alt: string;
};

export type FtgArtistView = {
  id: string;
  mbid: string;
  name: string;
  entityType: FtgEntityType;
  representativeCover: RepresentativeCover;
  productCount: number;
  searchArtist: string | null;
  searchHref: string | null;
};

export type FtgCandidateView = FtgArtistView & {
  rank: number;
  reasonCode: FtgReasonCode;
  reasonLabel: string;
  bridgeName?: string | null;
};

export type FtgTrailItem = {
  mbid: string;
  name: string;
  explanation: string | null;
};

export type FollowTheGroovePageData = {
  artist: FtgArtistView;
  candidates: FtgCandidateView[];
  trail: FtgTrailItem[];
};
