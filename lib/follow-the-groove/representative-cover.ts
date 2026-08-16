import {
  buildProductCoverUrl,
  COVER_PLACEHOLDER_SRC,
  normalizeCoverStoragePath,
} from "../cover-url";
import type { RepresentativeCover } from "./types";

export type ArtistProductLink = {
  artistId: string;
  productId: string;
  creditPosition: number | null;
};

export type CoverProduct = {
  id: string;
  artist: string;
  coverStatus: string | null;
  coverStoragePath: string | null;
  coverReviewStatus: string | null;
  metadataRaw: unknown;
};

function isAlbum(product: CoverProduct): boolean {
  if (!product.metadataRaw || typeof product.metadataRaw !== "object") return false;
  const selected = (product.metadataRaw as Record<string, unknown>).selected_release;
  if (!selected || typeof selected !== "object") return false;
  const releaseGroup = (selected as Record<string, unknown>)["release-group"];
  if (!releaseGroup || typeof releaseGroup !== "object") return false;
  return (releaseGroup as Record<string, unknown>)["primary-type"] === "Album";
}

export function resolveRepresentativeCovers(
  artistNames: Map<string, string>,
  links: ArtistProductLink[],
  products: CoverProduct[],
  currentlyAvailableProductIds: Set<string>,
): Map<string, RepresentativeCover> {
  const productsById = new Map(products.map((product) => [product.id, product]));
  const linksByArtist = new Map<string, ArtistProductLink[]>();
  for (const link of links) {
    const existing = linksByArtist.get(link.artistId) ?? [];
    existing.push(link);
    linksByArtist.set(link.artistId, existing);
  }

  const result = new Map<string, RepresentativeCover>();
  for (const [artistId, artistName] of artistNames) {
    const candidates = (linksByArtist.get(artistId) ?? [])
      .map((link) => ({ link, product: productsById.get(link.productId) }))
      .filter(
        (value): value is { link: ArtistProductLink; product: CoverProduct } =>
          Boolean(
            value.product &&
              value.product.coverStatus === "ready" &&
              value.product.coverReviewStatus !== "rejected" &&
              normalizeCoverStoragePath(value.product.coverStoragePath),
          ),
      )
      .sort((left, right) => {
        const approved = Number(right.product.coverReviewStatus === "approved") -
          Number(left.product.coverReviewStatus === "approved");
        if (approved) return approved;
        const primary = Number(right.link.creditPosition === 1) -
          Number(left.link.creditPosition === 1);
        if (primary) return primary;
        const album = Number(isAlbum(right.product)) - Number(isAlbum(left.product));
        if (album) return album;
        const available = Number(currentlyAvailableProductIds.has(right.product.id)) -
          Number(currentlyAvailableProductIds.has(left.product.id));
        if (available) return available;
        return left.product.id.localeCompare(right.product.id);
      });

    const selected = candidates[0]?.product;
    const src = selected ? buildProductCoverUrl(selected.coverStoragePath) : null;
    result.set(artistId, {
      src: src ?? COVER_PLACEHOLDER_SRC,
      isPlaceholder: !src,
      alt: src
        ? `Albumcover bij ${artistName}`
        : `Geen albumcover beschikbaar voor ${artistName}`,
    });
  }
  return result;
}
