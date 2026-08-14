import {
  createHash,
  timingSafeEqual,
} from "node:crypto";

const EXPECTED_TOKEN_HASH =
  "27b44a2b4364d3d203b0b34f507c3eddc69409fa7b27d2147dc0008254807912";

const TOKEN_RE =
  /^[0-9a-f]{64}$/;

export function isValidCoverReviewToken(
  value: string | null | undefined,
): boolean {
  const token =
    value?.trim().toLowerCase();

  if (
    !token ||
    !TOKEN_RE.test(token)
  ) {
    return false;
  }

  const actual =
    createHash("sha256")
      .update(token, "utf8")
      .digest();

  const expected =
    Buffer.from(
      EXPECTED_TOKEN_HASH,
      "hex",
    );

  if (
    actual.length !==
    expected.length
  ) {
    return false;
  }

  return timingSafeEqual(
    actual,
    expected,
  );
}
