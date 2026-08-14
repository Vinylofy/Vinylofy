import type {
  Metadata,
} from "next";

import Link from "next/link";

import {
  notFound,
} from "next/navigation";

import {
  isValidCoverReviewToken,
} from "@/lib/cover-review-auth";

import {
  resolveCoverUrl,
} from "@/lib/cover-url";

import {
  createSupabaseAdminClient,
} from "@/lib/supabase/admin";

import CoverReviewGrid, {
  type CoverReviewProduct,
} from "./cover-review-grid";

import styles from "./cover-review.module.css";

export const runtime =
  "nodejs";

export const dynamic =
  "force-dynamic";

export const revalidate = 0;

export const metadata:
  Metadata = {
    title: "Cover Review",
    robots: {
      index: false,
      follow: false,
      nocache: true,
    },
    referrer:
      "no-referrer",
  };

const PAGE_SIZE = 120;

const LETTERS = [
  "ALL",
  "A",
  "B",
  "C",
  "D",
  "E",
  "F",
  "G",
  "H",
  "I",
  "J",
  "K",
  "L",
  "M",
  "N",
  "O",
  "P",
  "Q",
  "R",
  "S",
  "T",
  "U",
  "V",
  "W",
  "X",
  "Y",
  "Z",
] as const;

type Letter =
  (typeof LETTERS)[number];

type ProductRow = {
  id: string;
  ean: string | null;
  artist: string | null;
  title: string | null;
  format_label:
    string | null;
  cover_url:
    string | null;
  cover_storage_path:
    string | null;
};

function firstValue(
  value:
    | string
    | string[]
    | undefined,
): string | undefined {
  return Array.isArray(
    value,
  )
    ? value[0]
    : value;
}

function normalizeLetter(
  value:
    | string
    | string[]
    | undefined,
): Letter {
  const candidate =
    firstValue(value)
      ?.trim()
      .toUpperCase() ??
    "ALL";

  return LETTERS.includes(
    candidate as Letter,
  )
    ? (candidate as Letter)
    : "ALL";
}

function normalizePage(
  value:
    | string
    | string[]
    | undefined,
): number {
  const parsed =
    Number.parseInt(
      firstValue(value) ??
        "1",
      10,
    );

  if (
    !Number.isFinite(
      parsed,
    ) ||
    parsed < 1
  ) {
    return 1;
  }

  return parsed;
}

export default async function CoverReviewPage({
  params,
  searchParams,
}: {
  params: Promise<{
    token: string;
  }>;
  searchParams: Promise<
    Record<
      string,
      | string
      | string[]
      | undefined
    >
  >;
}) {
  const { token } =
    await params;

  if (
    !isValidCoverReviewToken(
      token,
    )
  ) {
    notFound();
  }

  const queryParams =
    await searchParams;

  const letter =
    normalizeLetter(
      queryParams.letter,
    );

  const page =
    normalizePage(
      queryParams.page,
    );

  const from =
    (page - 1) *
    PAGE_SIZE;

  const to =
    from +
    PAGE_SIZE -
    1;

  const supabase =
    createSupabaseAdminClient();

  let query =
    supabase
      .from("products")
      .select(
        "id, ean, artist, title, format_label, cover_url, cover_storage_path",
        {
          count: "exact",
        },
      )
      .eq(
        "cover_status",
        "ready",
      )
      .not(
        "cover_storage_path",
        "is",
        null,
      );

  if (
    letter !== "ALL"
  ) {
    query =
      query.ilike(
        "artist",
        `${letter}%`,
      );
  }

  const {
    data: dataRaw,
    error,
    count,
  } =
    await query
      .order(
        "artist",
        {
          ascending: true,
        },
      )
      .order(
        "title",
        {
          ascending: true,
        },
      )
      .range(
        from,
        to,
      );

  if (error) {
    throw new Error(
      `Cover review query failed: ${error.message}`,
    );
  }

  const rows =
    dataRaw as unknown as
      ProductRow[];

  const products:
    CoverReviewProduct[] =
    rows.map(
      (row) => ({
        id: row.id,
        ean: row.ean,
        artist:
          row.artist?.trim() ||
          "Onbekende artiest",
        title:
          row.title?.trim() ||
          "Onbekende titel",
        formatLabel:
          row.format_label,
        coverSrc:
          resolveCoverUrl({
            coverUrl:
              row.cover_url,
            storagePath:
              row.cover_storage_path,
          }),
      }),
    );

  const total =
    count ?? 0;

  const lastPage =
    Math.max(
      1,
      Math.ceil(
        total /
          PAGE_SIZE,
      ),
    );

  const basePath =
    `/rvw/${token}/artwork`;

  return (
    <main
      className={
        styles.page
      }
    >
      <header
        className={
          styles.header
        }
      >
        <div>
          <span
            className={
              styles.privateLabel
            }
          >
            PRIVÉ · COVER QA
          </span>

          <h1
            className={
              styles.heading
            }
          >
            Vinylofy Cover Review
          </h1>

          <p
            className={
              styles.summary
            }
          >
            {total.toLocaleString(
              "nl-NL",
            )}{" "}
            covers
            {letter !==
            "ALL"
              ? ` · artiest ${letter}`
              : ""}
            {" · "}
            pagina {page} van{" "}
            {lastPage}
          </p>
        </div>
      </header>

      <nav
        className={
          styles.alphabet
        }
        aria-label="Artiestenindex"
      >
        {LETTERS.map(
          (item) => (
            <Link
              key={item}
              href={`${basePath}?letter=${item}&page=1`}
              className={[
                styles.letter,
                item ===
                letter
                  ? styles.letterActive
                  : "",
              ]
                .filter(
                  Boolean,
                )
                .join(
                  " ",
                )}
            >
              {item ===
              "ALL"
                ? "ALLE"
                : item}
            </Link>
          ),
        )}
      </nav>

      <div
        className={
          styles.paginationTop
        }
      >
        {page > 1 ? (
          <Link
            className={
              styles.pageButton
            }
            href={`${basePath}?letter=${letter}&page=${page - 1}`}
          >
            ← Vorige
          </Link>
        ) : (
          <span />
        )}

        {page <
        lastPage ? (
          <Link
            className={
              styles.pageButton
            }
            href={`${basePath}?letter=${letter}&page=${page + 1}`}
          >
            Volgende →
          </Link>
        ) : null}
      </div>

      <CoverReviewGrid
        token={token}
        products={
          products
        }
      />

      <div
        className={
          styles.paginationBottom
        }
      >
        {page > 1 ? (
          <Link
            className={
              styles.pageButton
            }
            href={`${basePath}?letter=${letter}&page=${page - 1}`}
          >
            ← Vorige
          </Link>
        ) : (
          <span />
        )}

        <span
          className={
            styles.pageIndicator
          }
        >
          {page} /{" "}
          {lastPage}
        </span>

        {page <
        lastPage ? (
          <Link
            className={
              styles.pageButton
            }
            href={`${basePath}?letter=${letter}&page=${page + 1}`}
          >
            Volgende →
          </Link>
        ) : (
          <span />
        )}
      </div>
    </main>
  );
}
