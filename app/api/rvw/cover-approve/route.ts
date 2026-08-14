import { NextResponse } from "next/server";
import { z } from "zod";

import {
  isValidCoverReviewToken,
} from "@/lib/cover-review-auth";

import {
  createSupabaseAdminClient,
} from "@/lib/supabase/admin";

export const runtime = "nodejs";

const SHA256_RE =
  /^[0-9a-f]{64}$/;

const PayloadSchema = z.object({
  productId:
    z.string().uuid(),
  coverSha256:
    z.string().regex(SHA256_RE),
});

type ReviewProductRow = {
  id: string;
  cover_status: string | null;
  cover_storage_path: string | null;
  cover_sha256: string | null;
};

type ApprovedRow = {
  id: string;
  cover_review_status: string | null;
  cover_review_sha256: string | null;
  cover_reviewed_at: string | null;
};

function hidden404() {
  return new NextResponse(
    null,
    {
      status: 404,
      headers: {
        "Cache-Control":
          "no-store",
        "X-Robots-Tag":
          "noindex, nofollow, noarchive",
      },
    },
  );
}

function conflict() {
  return NextResponse.json(
    {
      ok: false,
      error:
        "cover_changed_refresh",
    },
    {
      status: 409,
      headers: {
        "Cache-Control":
          "no-store",
        "X-Robots-Tag":
          "noindex, nofollow, noarchive",
      },
    },
  );
}

export async function POST(
  request: Request,
) {
  const token =
    request.headers.get(
      "x-cover-review-token",
    );

  if (
    !isValidCoverReviewToken(
      token,
    )
  ) {
    return hidden404();
  }

  const fetchSite =
    request.headers.get(
      "sec-fetch-site",
    );

  if (
    fetchSite &&
    fetchSite !== "same-origin"
  ) {
    return hidden404();
  }

  let payload:
    z.infer<typeof PayloadSchema>;

  try {
    payload =
      PayloadSchema.parse(
        await request.json(),
      );
  } catch {
    return NextResponse.json(
      {
        ok: false,
        error:
          "invalid_request",
      },
      {
        status: 400,
        headers: {
          "Cache-Control":
            "no-store",
        },
      },
    );
  }

  const supabase =
    createSupabaseAdminClient();

  const {
    data: productRaw,
    error: readError,
  } =
    await supabase
      .from("products")
      .select(
        "id, cover_status, cover_storage_path, cover_sha256",
      )
      .eq(
        "id",
        payload.productId,
      )
      .maybeSingle();

  if (readError) {
    console.error(
      "cover approve lookup failed",
      readError,
    );

    return NextResponse.json(
      {
        ok: false,
        error:
          "product_lookup_failed",
      },
      {
        status: 500,
        headers: {
          "Cache-Control":
            "no-store",
        },
      },
    );
  }

  const product =
    productRaw as unknown as
      ReviewProductRow | null;

  if (!product) {
    return hidden404();
  }

  if (
    product.cover_status !==
      "ready" ||
    !product.cover_storage_path
      ?.trim() ||
    product.cover_sha256 !==
      payload.coverSha256
  ) {
    return conflict();
  }

  const reviewedAt =
    new Date().toISOString();

  const {
    data: approvedRaw,
    error: writeError,
  } =
    await supabase
      .from("products")
      .update({
        cover_review_status:
          "approved",
        cover_review_sha256:
          payload.coverSha256,
        cover_reviewed_at:
          reviewedAt,
      })
      .eq(
        "id",
        payload.productId,
      )
      .eq(
        "cover_status",
        "ready",
      )
      .eq(
        "cover_sha256",
        payload.coverSha256,
      )
      .not(
        "cover_storage_path",
        "is",
        null,
      )
      .select(
        "id, cover_review_status, cover_review_sha256, cover_reviewed_at",
      )
      .maybeSingle();

  if (
    writeError ||
    !approvedRaw
  ) {
    if (writeError) {
      console.error(
        "cover approve write failed",
        writeError,
      );
    }

    return conflict();
  }

  const approved =
    approvedRaw as unknown as
      ApprovedRow;

  const valid =
    approved.cover_review_status ===
      "approved" &&
    approved.cover_review_sha256 ===
      payload.coverSha256 &&
    Boolean(
      approved.cover_reviewed_at,
    );

  if (!valid) {
    console.error(
      "cover approve final guard failed",
      approved,
    );

    return NextResponse.json(
      {
        ok: false,
        error:
          "approval_verify_failed",
      },
      {
        status: 500,
        headers: {
          "Cache-Control":
            "no-store",
        },
      },
    );
  }

  return NextResponse.json(
    {
      ok: true,
      status: "approved",
      coverSha256:
        payload.coverSha256,
    },
    {
      status: 200,
      headers: {
        "Cache-Control":
          "no-store",
        "X-Robots-Tag":
          "noindex, nofollow, noarchive",
      },
    },
  );
}
