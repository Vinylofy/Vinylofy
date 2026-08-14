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
};

type CardStatus =
  | "idle"
  | "busy"
  | "done"
  | "safe-warning"
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

  async function disqualify(
    product:
      CoverReviewProduct,
  ) {
    const current =
      states[product.id];

    if (
      current === "busy" ||
      current === "done" ||
      current ===
        "safe-warning"
    ) {
      return;
    }

    setStates((old) => ({
      ...old,
      [product.id]: "busy",
    }));

    setMessages((old) => ({
      ...old,
      [product.id]: "",
    }));

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

      if (result.cleanupOk) {
        setStates((old) => ({
          ...old,
          [product.id]:
            "done",
        }));

        setMessages(
          (old) => ({
            ...old,
            [product.id]:
              `Afgekeurd · ${result.rejectedCandidates ?? 0} candidate(s)`,
          }),
        );

        return;
      }

      setStates((old) => ({
        ...old,
        [product.id]:
          "safe-warning",
      }));

      setMessages(
        (old) => ({
          ...old,
          [product.id]:
            "Cover verwijderd en product geblokkeerd.",
        }),
      );
    } catch (error) {
      setStates((old) => ({
        ...old,
        [product.id]:
          "error",
      }));

      setMessages(
        (old) => ({
          ...old,
          [product.id]:
            error instanceof Error
              ? error.message
              : "Onbekende fout",
        }),
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
        Geen covers in deze selectie.
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

          const removed =
            state === "done" ||
            state ===
              "safe-warning";

          return (
            <article
              key={
                product.id
              }
              className={[
                styles.card,
                removed
                  ? styles.cardRejected
                  : "",
              ]
                .filter(
                  Boolean,
                )
                .join(" ")}
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

                {removed ? (
                  <div
                    className={
                      styles.overlay
                    }
                  >
                    AFGEKEURD
                  </div>
                ) : null}
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

                <button
                  type="button"
                  className={
                    styles.rejectButton
                  }
                  disabled={
                    state ===
                      "busy" ||
                    removed
                  }
                  onClick={() =>
                    disqualify(
                      product,
                    )
                  }
                >
                  {state ===
                  "busy"
                    ? "BEZIG…"
                    : removed
                      ? "AFGEKEURD"
                      : "AFKEUREN"}
                </button>

                {messages[
                  product.id
                ] ? (
                  <span
                    className={
                      state ===
                      "error"
                        ? styles.error
                        : styles.result
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
