import { NextResponse } from "next/server";
import { z } from "zod";

import {
  isValidCoverReviewToken,
} from "@/lib/cover-review-auth";

import {
  createSupabaseAdminClient,
} from "@/lib/supabase/admin";

export const runtime = "nodejs";

const PayloadSchema = z.object({
  productId: z.string().uuid(),
});

type ReviewProductRow = {
  id: string;
  ean: string | null;
  artist: string | null;
  title: string | null;
  cover_status: string | null;
  cover_url: string | null;
  cover_storage_path: string | null;
};

type FinalProductRow = {
  cover_status: string | null;
  cover_url: string | null;
  cover_storage_path: string | null;
  cover_error_code: string | null;
};

function hidden404() {
  return new NextResponse(
    null,
    {
      status: 404,
      headers: {
        "Cache-Control": "no-store",
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
    error: productReadError,
  } =
    await supabase
      .from("products")
      .select(
        "id, ean, artist, title, cover_status, cover_url, cover_storage_path",
      )
      .eq(
        "id",
        payload.productId,
      )
      .maybeSingle();

  if (productReadError) {
    console.error(
      "cover review lookup failed",
      productReadError,
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

  /*
   * Eerst product veilig BLOCKED
   * en covervelden leegmaken.
   *
   * Daarna candidate permanent
   * REJECTED en queue FAILED.
   */

  const now =
    new Date().toISOString();

  const {
    data: blockedRaw,
    error: productWriteError,
  } =
    await supabase
      .from("products")
      .update({
        cover_url: null,
        cover_storage_path: null,
        cover_source: null,
        cover_source_url: null,
        cover_source_shop_id:
          null,
        cover_sha256: null,
        cover_mime_type: null,
        cover_byte_size: null,
        cover_width: null,
        cover_height: null,
        cover_status: "blocked",
        cover_needs_refresh:
          false,
        cover_locked_at: null,
        cover_locked_by: null,
        cover_error_code:
          "manual_wrong_cover",
        cover_error_message:
          "Handmatig afgekeurd via private cover review.",
        updated_at: now,
      })
      .eq(
        "id",
        payload.productId,
      )
      .select("id")
      .maybeSingle();

  if (
    productWriteError ||
    !blockedRaw
  ) {
    console.error(
      "cover review block failed",
      productWriteError,
    );

    return NextResponse.json(
      {
        ok: false,
        error:
          "product_block_failed",
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

  const {
    data: rejectedRaw,
    error: candidateWriteError,
  } =
    await supabase
      .from(
        "product_cover_candidates",
      )
      .update({
        candidate_status:
          "rejected",
        is_selected: false,
        last_checked_at: now,
        last_error_code:
          "manual_wrong_cover",
        last_error_message:
          "Handmatig afgekeurd via private cover review.",
        updated_at: now,
      })
      .eq(
        "product_id",
        payload.productId,
      )
      .eq(
        "is_selected",
        true,
      )
      .select("id");

  const rejectedRows =
    rejectedRaw as unknown as
      Array<{ id: string }> |
      null;

  if (candidateWriteError) {
    console.error(
      "cover review candidate reject failed",
      candidateWriteError,
    );
  }

  const {
    error: queueWriteError,
  } =
    await supabase
      .from(
        "product_cover_queue",
      )
      .update({
        status: "failed",
        claimed_by: null,
        claimed_at: null,
        next_attempt_at: null,
        last_error_code:
          "manual_wrong_cover",
        last_error_message:
          "Handmatig afgekeurd via private cover review.",
        updated_at: now,
      })
      .eq(
        "product_id",
        payload.productId,
      );

  if (queueWriteError) {
    console.error(
      "cover review queue close failed",
      queueWriteError,
    );
  }

  const {
    data: finalRaw,
    error: finalProductError,
  } =
    await supabase
      .from("products")
      .select(
        "cover_status, cover_url, cover_storage_path, cover_error_code",
      )
      .eq(
        "id",
        payload.productId,
      )
      .maybeSingle();

  const finalProduct =
    finalRaw as unknown as
      FinalProductRow | null;

  const {
    count: selectedAfter,
    error: selectedCheckError,
  } =
    await supabase
      .from(
        "product_cover_candidates",
      )
      .select(
        "id",
        {
          count: "exact",
          head: true,
        },
      )
      .eq(
        "product_id",
        payload.productId,
      )
      .eq(
        "is_selected",
        true,
      );

  const productSafe =
    !finalProductError &&
    finalProduct !== null &&
    finalProduct.cover_status ===
      "blocked" &&
    finalProduct.cover_url ===
      null &&
    finalProduct
      .cover_storage_path ===
      null &&
    finalProduct
      .cover_error_code ===
      "manual_wrong_cover";

  if (!productSafe) {
    console.error(
      "cover review final guard failed",
      {
        finalProductError,
        finalProduct,
      },
    );

    return NextResponse.json(
      {
        ok: false,
        error:
          "final_product_guard_failed",
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

  const cleanupOk =
    !candidateWriteError &&
    !queueWriteError &&
    !selectedCheckError &&
    (selectedAfter ?? 0) === 0;

  return NextResponse.json(
    {
      ok: true,
      cleanupOk,
      rejectedCandidates:
        rejectedRows?.length ?? 0,
      selectedAfter:
        selectedAfter ?? null,
      product: {
        id: product.id,
        ean: product.ean,
        artist: product.artist,
        title: product.title,
      },
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
