"use client";

import Link from "next/link";
import type { ReactNode } from "react";
import { rememberReleaseSource } from "@/lib/analytics";

export function ReleaseSourceLink({
  href,
  external = false,
  className,
  children,
}: {
  href: string;
  external?: boolean;
  className?: string;
  children: ReactNode;
}) {
  const remember = () => rememberReleaseSource("new_releases");

  if (external) {
    return (
      <a href={href} target="_blank" rel="noreferrer" className={className} onClick={remember}>
        {children}
      </a>
    );
  }

  return (
    <Link href={href} className={className} onClick={remember}>
      {children}
    </Link>
  );
}
