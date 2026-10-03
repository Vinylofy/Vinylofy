"use client";

import { createContext, useContext, useEffect, useRef, useState } from "react";
import {
  consumeReleaseSource,
  consumeSearchSource,
  rememberSearchSource,
  trackPriceHistoryView,
  trackFtgArtistView,
  trackReleaseView,
  trackSearch,
  type AnalyticsSource,
} from "@/lib/analytics";

const ReleaseSourceContext = createContext<AnalyticsSource | null>(null);

export function ReleaseAnalyticsProvider({ children }: { children: React.ReactNode }) {
  const [source] = useState<AnalyticsSource | null>(() =>
    typeof window === "undefined"
      ? null
      : consumeReleaseSource() ?? "release_detail",
  );

  return (
    <ReleaseSourceContext.Provider value={source}>
      {children}
    </ReleaseSourceContext.Provider>
  );
}

export function useReleaseAnalyticsSource(): AnalyticsSource | null {
  return useContext(ReleaseSourceContext);
}

export function SearchPageAnalytics({
  query,
  resultCount,
}: {
  query: string;
  resultCount: number;
}) {
  const sentQueryRef = useRef<string | null>(null);

  useEffect(() => {
    if (!query || sentQueryRef.current === query) return;
    sentQueryRef.current = query;
    const source = consumeSearchSource() ?? "unknown";
    trackSearch({
      query,
      resultCount,
      source,
      searchType: "products",
    });
    rememberSearchSource(source);
  }, [query, resultCount]);

  return null;
}

export function ReleaseAnalytics({
  releaseId,
  ean,
  artistName,
  releaseTitle,
  offerCount,
  lowestPrice,
  source,
}: {
  releaseId: string;
  ean?: string | null;
  artistName: string;
  releaseTitle: string;
  offerCount: number;
  lowestPrice: number | null;
  source?: AnalyticsSource;
}) {
  const contextSource = useReleaseAnalyticsSource();
  const sentReleaseRef = useRef<string | null>(null);

  useEffect(() => {
    if (!contextSource || sentReleaseRef.current === releaseId) return;
    sentReleaseRef.current = releaseId;
    trackReleaseView({
      releaseId,
      ean,
      artistName,
      releaseTitle,
      offerCount,
      lowestPrice,
      source: source ?? contextSource,
    });
  }, [artistName, contextSource, ean, lowestPrice, offerCount, releaseId, releaseTitle, source]);

  return null;
}

export function PriceHistoryAnalytics({
  releaseId,
  ean,
  artistName,
  releaseTitle,
  source,
}: {
  releaseId: string;
  ean?: string | null;
  artistName: string;
  releaseTitle: string;
  source?: AnalyticsSource;
}) {
  const contextSource = useReleaseAnalyticsSource();
  const sentReleaseRef = useRef<string | null>(null);

  useEffect(() => {
    if (!contextSource || sentReleaseRef.current === releaseId) return;
    sentReleaseRef.current = releaseId;
    trackPriceHistoryView({
      releaseId,
      ean,
      artistName,
      releaseTitle,
      source: source ?? contextSource,
    });
  }, [artistName, contextSource, ean, releaseId, releaseTitle, source]);

  return null;
}

export function FtgArtistAnalytics({
  startArtistId,
  startArtistName,
  artistId,
  artistName,
  depth,
}: {
  startArtistId: string;
  startArtistName: string;
  artistId: string;
  artistName: string;
  depth: number;
}) {
  const sentArtistRef = useRef<string | null>(null);

  useEffect(() => {
    const eventKey = `${artistId}:${depth}`;
    if (sentArtistRef.current === eventKey) return;
    sentArtistRef.current = eventKey;
    trackFtgArtistView({
      startArtistId,
      startArtistName,
      artistId,
      artistName,
      depth,
    });
  }, [artistId, artistName, depth, startArtistId, startArtistName]);

  return null;
}
