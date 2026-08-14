"use client";

import Image from "next/image";

import {
  useState,
} from "react";

import styles from "./cover-review.module.css";

export type CoverReviewProduct = {
  id: string;
  ean: string | null;
  artist: string;
  title: string;
  formatLabel: string | null;
  coverSrc: string;
  coverSha256: string | null;
};

type CardStatus =
  | "idle"
  | "approving"
  | "rejecting"
  | "approved"
  | "rejected"
  | "error";

export default function CoverReviewGrid({
  token,
  products,
}: {
  token: string;
  products:
    CoverReviewProduct[];
}) {
  const [
    states,
    setStates,
  ] = useState<
    Record<
      string,
      CardStatus
    >
  >({});

  const [
    messages,
    setMessages,
  ] = useState<
    Record<string, string>
  >({});

  function setBusy(
    productId: string,
    status:
      | "approving"
      | "rejecting",
  ) {
    setStates((old) => ({
      ...old,
      [productId]:
        status,
    }));

    setMessages((old) => ({
      ...old,
      [productId]: "",
    }));
  }

  function setError(
    productId: string,
    error: unknown,
  ) {
    setStates((old) => ({
      ...old,
      [productId]:
        "error",
    }));

    setMessages((old) => ({
      ...old,
      [productId]:
        error instanceof Error
          ? error.message
          : "Onbekende fout",
    }));
  }

  async function approve(
    product:
      CoverReviewProduct,
  ) {
    if (
      !product.coverSha256
    ) {
      return;
    }

    setBusy(
      product.id,
      "approving",
    );

    try {
      const response =
        await fetch(
          "/api/rvw/cover-approve",
          {
            method: "POST",
            credentials:
              "same-origin",
            headers: {
              "Content-Type":
                "application/json",
              "X-Cover-Review-Token":
                token,
            },
            body:
              JSON.stringify({
                productId:
                  product.id,
                coverSha256:
                  product.coverSha256,
              }),
          },
        );

      const result =
        await response
          .json()
          .catch(() => null);

      if (
        !response.ok ||
        !result?.ok
      ) {
        throw new Error(
          result?.error ??
            `HTTP ${response.status}`,
        );
      }

      setStates((old) => ({
        ...old,
        [product.id]:
          "approved",
      }));
    } catch (error) {
      setError(
        product.id,
        error,
      );
    }
  }

  async function disqualify(
    product:
      CoverReviewProduct,
  ) {
    setBusy(
      product.id,
      "rejecting",
    );

    try {
      const response =
        await fetch(
          "/api/rvw/cover-disqualify",
          {
            method: "POST",
            credentials:
              "same-origin",
            headers: {
              "Content-Type":
                "application/json",
              "X-Cover-Review-Token":
                token,
            },
            body:
              JSON.stringify({
                productId:
                  product.id,
                coverSha256:
                  product.coverSha256,
              }),
          },
        );

      const result =
        await response
          .json()
          .catch(() => null);

      if (
        !response.ok ||
        !result?.ok
      ) {
        throw new Error(
          result?.error ??
            `HTTP ${response.status}`,
        );
      }

      setStates((old) => ({
        ...old,
        [product.id]:
          "rejected",
      }));
    } catch (error) {
      setError(
        product.id,
        error,
      );
    }
  }

  if (
    products.length === 0
  ) {
    return (
      <div
        className={
          styles.empty
        }
      >
        Geen covers meer in deze selectie.
      </div>
    );
  }

  return (
    <div
      className={
        styles.grid
      }
    >
      {products.map(
        (product) => {
          const state =
            states[
              product.id
            ] ?? "idle";

          if (
            state ===
              "approved" ||
            state ===
              "rejected"
          ) {
            return null;
          }

          const busy =
            state ===
              "approving" ||
            state ===
              "rejecting";

          return (
            <article
              key={
                product.id
              }
              className={
                styles.card
              }
            >
              <div
                className={
                  styles.imageWrap
                }
              >
                <Image
                  src={
                    product.coverSrc
                  }
                  alt={`${product.artist} — ${product.title}`}
                  width={320}
                  height={320}
                  className={
                    styles.cover
                  }
                  sizes="(max-width: 600px) 46vw, (max-width: 1000px) 23vw, 180px"
                  unoptimized
                />
              </div>

              <div
                className={
                  styles.info
                }
              >
                <strong
                  className={
                    styles.artist
                  }
                >
                  {
                    product.artist
                  }
                </strong>

                <span
                  className={
                    styles.title
                  }
                >
                  {
                    product.title
                  }
                </span>

                {product.formatLabel ? (
                  <span
                    className={
                      styles.meta
                    }
                  >
                    {
                      product.formatLabel
                    }
                  </span>
                ) : null}

                <span
                  className={
                    styles.ean
                  }
                >
                  EAN{" "}
                  {
                    product.ean ??
                    "—"
                  }
                </span>

                <div
                  className={
                    styles.actions
                  }
                >
                  <button
                    type="button"
                    className={
                      styles.approveButton
                    }
                    disabled={
                      busy ||
                      !product.coverSha256
                    }
                    title={
                      product.coverSha256
                        ? "Deze exacte cover goedkeuren"
                        : "Deze cover heeft nog geen SHA256 en kan daarom niet definitief worden goedgekeurd"
                    }
                    onClick={() =>
                      approve(
                        product,
                      )
                    }
                  >
                    {state ===
                    "approving"
                      ? "BEZIG…"
                      : product.coverSha256
                        ? "✓ GOEDKEUREN"
                        : "GEEN HASH"}
                  </button>

                  <button
                    type="button"
                    className={
                      styles.rejectButton
                    }
                    disabled={
                      busy
                    }
                    onClick={() =>
                      disqualify(
                        product,
                      )
                    }
                  >
                    {state ===
                    "rejecting"
                      ? "BEZIG…"
                      : "✕ AFKEUREN"}
                  </button>
                </div>

                {messages[
                  product.id
                ] ? (
                  <span
                    className={
                      styles.error
                    }
                  >
                    {
                      messages[
                        product.id
                      ]
                    }
                  </span>
                ) : null}
              </div>
            </article>
          );
        },
      )}
    </div>
  );
}
