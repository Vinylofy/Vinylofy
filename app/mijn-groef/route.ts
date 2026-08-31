import { NextResponse, type NextRequest } from "next/server";

const coverReviewPath =
  "/rvw/4cd7a8cf23027e2a1f0cafb75483c4a638fee4bac4533a07a6d6c5ce1bc834a2/artwork";

export function GET(request: NextRequest) {
  return NextResponse.redirect(new URL(coverReviewPath, request.url));
}
