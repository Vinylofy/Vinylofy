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

  const [
    selectedIds,
    setSelectedIds,
  ] = useState<
    Set<string>
  >(
    () => new Set(),
  );

  const [
    bulkBusy,
    setBulkBusy,
  ] = useState(false);

  const [
    bulkMessage,
    setBulkMessage,
  ] = useState("");

  function stateFor(
    productId: string,
  ): CardStatus {
    return (
      states[productId] ??
      "idle"
    );
  }

  const visibleProducts =
    products.filter(
      (product) => {
        const state =
          stateFor(
            product.id,
          );

        return (
          state !==
            "approved" &&
          state !==
            "rejected"
        );
      },
    );

  const selectableProducts =
    visibleProducts.filter(
      (product) => {
        const state =
          stateFor(
            product.id,
          );

        return (
          Boolean(
            product.coverSha256,
          ) &&
          state !==
            "approving" &&
          state !==
            "rejecting"
        );
      },
    );

  const selectedProducts =
    selectableProducts.filter(
      (product) =>
        selectedIds.has(
          product.id,
        ),
    );

  const allSelected =
    selectableProducts.length >
      0 &&
    selectedProducts.length ===
      selectableProducts.length;

  function toggleSelected(
    productId: string,
  ) {
    if (bulkBusy) {
      return;
    }

    setSelectedIds(
      (old) => {
        const next =
          new Set(old);

        if (
          next.has(
            productId,
          )
        ) {
          next.delete(
            productId,
          );
        } else {
          next.add(
            productId,
          );
        }

        return next;
      },
    );
  }

  function toggleSelectAll() {
    if (bulkBusy) {
      return;
    }

    if (allSelected) {
      setSelectedIds(
        new Set(),
      );

      return;
    }

    setSelectedIds(
      new Set(
        selectableProducts.map(
          (product) =>
            product.id,
        ),
      ),
    );
  }

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

  function removeSelection(
    productId: string,
  ) {
    setSelectedIds(
      (old) => {
        const next =
          new Set(old);

        next.delete(
          productId,
        );

        return next;
      },
    );
  }

  async function requestApprove(
    product:
      CoverReviewProduct,
  ) {
    if (
      !product.coverSha256
    ) {
      throw new Error(
        "Geen SHA256 beschikbaar",
      );
    }

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
  }

  async function approve(
    product:
      CoverReviewProduct,
  ) {
    if (
      !product.coverSha256 ||
      bulkBusy
    ) {
      return;
    }

    setBusy(
      product.id,
      "approving",
    );

    try {
      await requestApprove(
        product,
      );

      setStates((old) => ({
        ...old,
        [product.id]:
          "approved",
      }));

      removeSelection(
        product.id,
      );
    } catch (error) {
      setError(
        product.id,
        error,
      );
    }
  }

  async function approveSelected() {
    if (
      bulkBusy ||
      selectedProducts.length ===
        0
    ) {
      return;
    }

    const queue = [
      ...selectedProducts,
    ];

    const failedIds:
      string[] = [];

    let cursor = 0;

    setBulkBusy(true);

    setBulkMessage(
      `Bezig met ${queue.length} geselecteerde covers…`,
    );

    setStates((old) => {
      const next = {
        ...old,
      };

      for (
        const product
        of queue
      ) {
        next[
          product.id
        ] =
          "approving";
      }

      return next;
    });

    async function worker() {
      while (true) {
        const index =
          cursor++;

        if (
          index >=
          queue.length
        ) {
          return;
        }

        const product =
          queue[index];

        try {
          await requestApprove(
            product,
          );

          setStates(
            (old) => ({
              ...old,
              [product.id]:
                "approved",
            }),
          );

          setMessages(
            (old) => ({
              ...old,
              [product.id]:
                "",
            }),
          );
        } catch (error) {
          failedIds.push(
            product.id,
          );

          setError(
            product.id,
            error,
          );
        }
      }
    }

    const workerCount =
      Math.min(
        6,
        queue.length,
      );

    await Promise.all(
      Array.from(
        {
          length:
            workerCount,
        },
        () => worker(),
      ),
    );

    const successCount =
      queue.length -
      failedIds.length;

    setSelectedIds(
      new Set(
        failedIds,
      ),
    );

    setBulkBusy(false);

    if (
      failedIds.length >
        0
    ) {
      setBulkMessage(
        `${successCount} goedgekeurd · ${failedIds.length} mislukt. De mislukte selectie blijft aangevinkt.`,
      );

      return;
    }

    setBulkMessage(
      `${successCount} covers goedgekeurd. Volgende batch wordt geladen…`,
    );

    /*
     * De pending dataset krimpt door
     * goedkeuring. Terug naar pagina 1
     * voorkomt dat offset-paginering
     * niet-beoordeelde covers overslaat.
     */
    window.setTimeout(
      () => {
        const url =
          new URL(
            window.location.href,
          );

        url.searchParams.set(
          "page",
          "1",
        );

        window.location.assign(
          url.toString(),
        );
      },
      500,
    );
  }

  async function disqualify(
    product:
      CoverReviewProduct,
  ) {
    if (bulkBusy) {
      return;
    }

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

      removeSelection(
        product.id,
      );
    } catch (error) {
      setError(
        product.id,
        error,
      );
    }
  }

  if (
    visibleProducts.length ===
      0
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
    <>
      <div
        className={
          styles.bulkToolbar
        }
      >
        <label
          className={
            styles.selectAll
          }
        >
          <input
            type="checkbox"
            checked={
              allSelected
            }
            disabled={
              bulkBusy ||
              selectableProducts.length ===
                0
            }
            onChange={
              toggleSelectAll
            }
          />

          <span>
            SELECT ALL
          </span>
        </label>

        <span
          className={
            styles.selectionCount
          }
        >
          {
            selectedProducts.length
          }{" "}
          geselecteerd van{" "}
          {
            selectableProducts.length
          }
        </span>

        <button
          type="button"
          className={
            styles.bulkApproveButton
          }
          disabled={
            bulkBusy ||
            selectedProducts.length ===
              0
          }
          onClick={
            approveSelected
          }
        >
          {bulkBusy
            ? "BEZIG…"
            : `✓ SELECTIE GOEDKEUREN (${selectedProducts.length})`}
        </button>

        {bulkMessage ? (
          <span
            className={
              styles.bulkMessage
            }
          >
            {bulkMessage}
          </span>
        ) : null}
      </div>

      <div
        className={
          styles.grid
        }
      >
        {visibleProducts.map(
          (product) => {
            const state =
              stateFor(
                product.id,
              );

            const busy =
              state ===
                "approving" ||
              state ===
                "rejecting";

            const selected =
              selectedIds.has(
                product.id,
              );

            return (
              <article
                key={
                  product.id
                }
                className={[
                  styles.card,
                  selected
                    ? styles.cardSelected
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

                  <label
                    className={
                      styles.titleSelect
                    }
                  >
                    <input
                      type="checkbox"
                      checked={
                        selected
                      }
                      disabled={
                        bulkBusy ||
                        busy ||
                        !product.coverSha256
                      }
                      onChange={() =>
                        toggleSelected(
                          product.id,
                        )
                      }
                      aria-label={`Selecteer ${product.artist} — ${product.title}`}
                    />

                    <span
                      className={
                        styles.title
                      }
                    >
                      {
                        product.title
                      }
                    </span>
                  </label>

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
                        bulkBusy ||
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
                        bulkBusy ||
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
    </>
  );
}
