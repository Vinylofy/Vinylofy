import type { FtgEntityType, FtgReasonCode } from "./types";

export type AllowedEvidence = {
  sourceArtistId: string;
  targetArtistId: string;
  evidenceKind: "membership" | "artist_credit" | "instrument" | "vocal";
  ended: boolean | null;
};

type ReasonInput = {
  reasonCode: FtgReasonCode;
  activeArtist: { id: string; name: string; entityType: FtgEntityType };
  candidate: { id: string; entityType: FtgEntityType };
  evidence: AllowedEvidence[];
};

const RECORDING_EVIDENCE = new Set(["artist_credit", "instrument", "vocal"]);

export function mapReasonLabel(input: ReasonInput): string {
  if (input.reasonCode === "factual_and_similarity") {
    return "Feitelijke én muzikale connectie";
  }
  if (input.reasonCode === "similar_artist") return "Muzikaal verwant";
  if (input.reasonCode === "recording_collaboration") {
    return "Werkten samen op een opname";
  }

  const membership = input.evidence.filter(
    (row) =>
      row.evidenceKind === "membership" &&
      ((row.sourceArtistId === input.activeArtist.id &&
        row.targetArtistId === input.candidate.id) ||
        (row.sourceArtistId === input.candidate.id &&
          row.targetArtistId === input.activeArtist.id)),
  );
  const isCurrent = membership.some((row) => row.ended === false);
  const isFormer = membership.length > 0 && membership.every((row) => row.ended === true);

  if (input.candidate.entityType === "person" && input.activeArtist.entityType === "group") {
    if (isCurrent) return `Lid van ${input.activeArtist.name}`;
    if (isFormer) return `Voormalig lid van ${input.activeArtist.name}`;
    if (membership.length > 0) return `Bandlid van ${input.activeArtist.name}`;
  }
  if (input.activeArtist.entityType === "person" && input.candidate.entityType === "group") {
    if (isCurrent) return `Band waarvan ${input.activeArtist.name} lid is`;
    if (isFormer) return `Band waarvan ${input.activeArtist.name} lid was`;
    if (membership.length > 0) return `Band waar ${input.activeArtist.name} deel van uitmaakte`;
  }

  return "Bandconnectie";
}

export function mapDedicatedReasonLabel(
  input: ReasonInput & {
    bridgeName?: string | null;
    bridgeMechanisms?: string[];
    destinationName?: string;
  },
): string {
  if (input.bridgeName) {
    if (
      input.destinationName &&
      input.bridgeMechanisms?.some((mechanism) => RECORDING_EVIDENCE.has(mechanism))
    ) {
      return `Via ${input.bridgeName}, die opnam met ${input.destinationName}`;
    }
    return `Via ${input.bridgeName}`;
  }
  return mapReasonLabel(input);
}
