"use client";

import {
  useState,
  type DOMAttributes,
  type ImgHTMLAttributes,
} from "react";

import { COVER_PLACEHOLDER_SRC } from "../lib/cover-url";

type SafeImageAttributes = Omit<
  ImgHTMLAttributes<HTMLImageElement>,
  "alt" | "src" | (keyof DOMAttributes<HTMLImageElement>)
>;

export type CoverImageClientProps = SafeImageAttributes & {
  alt: string;
  resolvedSrc: string;
};

export function CoverImageClient({
  alt,
  resolvedSrc,
  ...imageProps
}: CoverImageClientProps) {
  const [failedSrc, setFailedSrc] = useState<string | null>(null);

  const showPlaceholder =
    resolvedSrc === COVER_PLACEHOLDER_SRC ||
    failedSrc === resolvedSrc;

  const displayedSrc = showPlaceholder
    ? COVER_PLACEHOLDER_SRC
    : resolvedSrc;

  function handleError() {
    if (
      displayedSrc !== COVER_PLACEHOLDER_SRC &&
      failedSrc !== resolvedSrc
    ) {
      setFailedSrc(resolvedSrc);
    }
  }

  return (
    // The client receives only a resolved same-origin path or placeholder.
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
