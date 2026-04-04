import { NextResponse } from "next/server";
import { z } from "zod";
import { createSupabaseAdminClient } from "@/lib/supabase/admin";

export const runtime = "nodejs";

const QueuePayloadSchema = z.object({
  productIds: z.array(z.string().uuid()).max(50).optional(),
  eans: z.array(z.string().min(8).max(32)).max(50).optional(),
  source: z.enum(["homepage", "search", "detail", "featured", "manual_seed"]),
  priorityBump: z.number().int().min(0).max(100000).optional(),
  requestedBy: z.string().max(100).optional(),
});

export async function POST(request: Request) {
  try {
    const json = await request.json();
    const payload = QueuePayloadSchema.parse(json);

    const productIds = Array.from(new Set(payload.productIds ?? []));
    const eans = Array.from(new Set(payload.eans ?? []));
    const priorityBump = payload.priorityBump ?? 400;
    const requestedBy = payload.requestedBy ?? "web";

    if (productIds.length === 0 && eans.length === 0) {
      return NextResponse.json({ queued: 0 }, { status: 200 });
    }

    const supabase = createSupabaseAdminClient();

    if (productIds.length > 0) {
      const { data, error } = await supabase.rpc("queue_cover_for_products", {
        _product_ids: productIds,
        _source: payload.source,
        _priority_bump: priorityBump,
        _requested_by: requestedBy,
      });

      if (error) {
        console.error("queue_cover_for_products failed", error);
        return NextResponse.json({ error: "queue_failed" }, { status: 500 });
      }

      return NextResponse.json({ queued: data ?? 0 }, { status: 200 });
    }

    const { data, error } = await supabase.rpc("queue_cover_for_eans", {
      _eans: eans,
      _source: payload.source,
      _priority_bump: priorityBump,
      _requested_by: requestedBy,
    });

    if (error) {
      console.error("queue_cover_for_eans failed", error);
      return NextResponse.json({ error: "queue_failed" }, { status: 500 });
    }

    return NextResponse.json({ queued: data ?? 0 }, { status: 200 });
  } catch (error) {
    console.error("cover queue route failed", error);
    return NextResponse.json({ error: "invalid_request" }, { status: 400 });
  }
}
