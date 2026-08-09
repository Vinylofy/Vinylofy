"use client";

import {
  useState,
  type ImgHTMLAttributes,
  type SyntheticEvent,
} from "react";

import {
  COVER_PLACEHOLDER_SRC,
  resolveCoverUrl,
} from "../lib/cover-url";

export type CoverImageProps = Omit<
  ImgHTMLAttributes<HTMLImageElement>,
  "alt" | "src"
> & {
  alt: string;
  src?: string | null;
  storagePath?: string | null;
};

export function CoverImage({
  alt,
  src,
  storagePath,
  onError,
  ...imageProps
}: CoverImageProps) {
  const resolvedSrc = resolveCoverUrl({
    coverUrl: src,
    storagePath,
  });
  const [failedSrc, setFailedSrc] = useState<string | null>(null);

  const showPlaceholder =
    resolvedSrc === COVER_PLACEHOLDER_SRC ||
    failedSrc === resolvedSrc;
  const displayedSrc = showPlaceholder
    ? COVER_PLACEHOLDER_SRC
    : resolvedSrc;

  function handleError(
    event: SyntheticEvent<HTMLImageElement, Event>,
  ) {
    onError?.(event);

    if (
      displayedSrc !== COVER_PLACEHOLDER_SRC &&
      failedSrc !== resolvedSrc
    ) {
      setFailedSrc(resolvedSrc);
    }
  }

  return (
    // Native img keeps the validated Supabase origin independent of
    // Next.js remotePatterns. The helper has already rejected all
    // unexpected remote sources.
    // eslint-disable-next-line @next/next/no-img-element
    <img
      {...imageProps}
      alt={alt}
      src={displayedSrc}
      onError={handleError}
      data-cover-fallback={showPlaceholder ? "true" : "false"}
    />
  );
}
