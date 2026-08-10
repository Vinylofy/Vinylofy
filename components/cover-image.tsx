import type {
  DOMAttributes,
  ImgHTMLAttributes,
} from "react";

import { CoverImageClient } from "./cover-image-client";
import { resolveCoverUrl } from "../lib/cover-url";

type SafeImageAttributes = Omit<
  ImgHTMLAttributes<HTMLImageElement>,
  "alt" | "src" | (keyof DOMAttributes<HTMLImageElement>)
>;

export type CoverImageProps = SafeImageAttributes & {
  alt: string;
  src?: string | null;
  storagePath?: string | null;
};

export function CoverImage({
  alt,
  src,
  storagePath,
  ...imageProps
}: CoverImageProps) {
  const resolvedSrc = resolveCoverUrl({
    coverUrl: src,
    storagePath,
  });

  return (
    <CoverImageClient
      {...imageProps}
      alt={alt}
      resolvedSrc={resolvedSrc}
    />
  );
}
