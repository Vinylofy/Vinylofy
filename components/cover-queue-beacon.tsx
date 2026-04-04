"use client";

import { useEffect, useMemo, useRef } from "react";

type CoverQueueBeaconSource = "homepage" | "search" | "detail" | "featured";

type CoverQueueBeaconProps = {
  productIds: string[];
  source: CoverQueueBeaconSource;
  priorityBump?: number;
};

export function CoverQueueBeacon({
  productIds,
  source,
  priorityBump = 400,
}: CoverQueueBeaconProps) {
  const fired = useRef(false);

  const uniqueIds = useMemo(
    () => Array.from(new Set(productIds.filter(Boolean))).slice(0, 50),
    [productIds],
  );

  useEffect(() => {
    if (fired.current) return;
    if (uniqueIds.length === 0) return;

    fired.current = true;

    fetch("/api/covers/queue", {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({
        productIds: uniqueIds,
        source,
        priorityBump,
      }),
      keepalive: true,
    }).catch(() => {
      // intentionally swallow: page render should never fail because queueing fails
    });
  }, [uniqueIds, source, priorityBump]);

  return null;
}
