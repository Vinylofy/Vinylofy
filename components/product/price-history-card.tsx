"use client";

import { useMemo, useState } from "react";
import {
  formatEuro,
  type PriceHistoryWindow,
  type ProductPriceHistoryPoint,
} from "@/lib/vinylofy-data";

type PriceHistoryCardProps = {
  currentPrice: number | null;
  points: ProductPriceHistoryPoint[];
  defaultWindow?: PriceHistoryWindow;
};

const WINDOW_LABELS: Record<PriceHistoryWindow, string> = {
  "30d": "30D",
  "90d": "90D",
  "1y": "1Y",
};

const WINDOW_DAYS: Record<PriceHistoryWindow, number> = {
  "30d": 30,
  "90d": 90,
  "1y": 365,
};

function parseIsoDay(value: string): Date {
  return new Date(`${value}T00:00:00Z`);
}

function filterWindow(
  points: ProductPriceHistoryPoint[],
  window: PriceHistoryWindow,
): ProductPriceHistoryPoint[] {
  if (points.length === 0) return [];

  const lastPoint = parseIsoDay(points[points.length - 1].day);
  const minDate = new Date(lastPoint);
  minDate.setUTCDate(minDate.getUTCDate() - (WINDOW_DAYS[window] - 1));

  return points.filter((point) => parseIsoDay(point.day) >= minDate);
}

function formatMonthLabel(value: string) {
  return new Intl.DateTimeFormat("nl-NL", { month: "short" })
    .format(parseIsoDay(value))
    .replace(".", "");
}

function formatDayLabel(value: string) {
  return new Intl.DateTimeFormat("nl-NL", {
    day: "numeric",
    month: "short",
    year: "numeric",
  }).format(parseIsoDay(value));
}

function formatAxisEuro(value: number) {
  return new Intl.NumberFormat("nl-NL", {
    style: "currency",
    currency: "EUR",
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
}

export function PriceHistoryCard({
  currentPrice,
  points,
  defaultWindow = "90d",
}: PriceHistoryCardProps) {
  const [window, setWindow] = useState<PriceHistoryWindow>(defaultWindow);

  const filtered = useMemo(() => filterWindow(points, window), [points, window]);

  const chart = useMemo(() => {
    if (filtered.length === 0) return null;

    const width = 760;
    const height = 280;
    const padding = { top: 18, right: 16, bottom: 34, left: 62 };
    const innerWidth = width - padding.left - padding.right;
    const innerHeight = height - padding.top - padding.bottom;

    const values = filtered.map((point) => point.price);
    const minValue = Math.min(...values);
    const maxValue = Math.max(...values);
    const spread = Math.max(maxValue - minValue, Math.max(maxValue * 0.08, 1));
    const yMin = Math.max(0, minValue - spread * 0.22);
    const yMax = maxValue + spread * 0.22;
    const valueRange = Math.max(yMax - yMin, 1);

    const x = (index: number) => {
      if (filtered.length === 1) return padding.left + innerWidth / 2;
      return padding.left + (index / (filtered.length - 1)) * innerWidth;
    };

    const y = (value: number) =>
      padding.top + ((yMax - value) / valueRange) * innerHeight;

    const linePath = filtered
      .map((point, index) => `${index === 0 ? "M" : "L"} ${x(index)} ${y(point.price)}`)
      .join(" ");

    const areaPath = filtered.length
      ? `${linePath} L ${x(filtered.length - 1)} ${padding.top + innerHeight} L ${x(0)} ${
          padding.top + innerHeight
        } Z`
      : "";

    const tickValues = [yMin, yMin + valueRange / 2, yMax];
    const xLabelIndexes = Array.from(
      new Set([0, Math.max(0, Math.floor((filtered.length - 1) / 2)), filtered.length - 1]),
    );

    return {
      width,
      height,
      padding,
      linePath,
      areaPath,
      tickValues,
      xLabelIndexes,
      y,
      x,
    };
  }, [filtered]);

  const lowestInWindow = filtered.length > 0 ? Math.min(...filtered.map((point) => point.price)) : null;
  const maxShopCount = filtered.reduce((max, point) => Math.max(max, point.shopCount), 0);
  const latestPoint = filtered[filtered.length - 1] ?? null;

  return (
    <section className="rounded-[28px] border border-[rgba(234,88,12,0.22)] bg-white p-5 shadow-sm md:p-6">
      <div className="flex flex-col gap-4 border-b border-[rgba(234,88,12,0.18)] pb-5 md:flex-row md:items-end md:justify-between">
        <div>
          <h2 className="text-2xl font-semibold tracking-tight text-[color:var(--foreground)]">
            Prijsontwikkeling
          </h2>
          <p className="mt-1 text-sm text-neutral-500">
            Gebaseerd op waargenomen laagste dagprijs per product.
          </p>
        </div>

        <div className="inline-flex w-fit rounded-full border border-[rgba(234,88,12,0.18)] bg-[#fffaf6] p-1">
          {(["30d", "90d", "1y"] as PriceHistoryWindow[]).map((option) => {
            const active = option === window;

            return (
              <button
                key={option}
                type="button"
                onClick={() => setWindow(option)}
                className={`rounded-full px-4 py-2 text-sm font-medium transition ${
                  active
                    ? "bg-orange-500 text-white shadow-sm"
                    : "text-neutral-700 hover:bg-orange-50"
                }`}
              >
                {WINDOW_LABELS[option]}
              </button>
            );
          })}
        </div>
      </div>

      {filtered.length < 2 || !chart ? (
        <div className="py-10 text-sm text-neutral-500">
          Nog te weinig prijshistorie beschikbaar om een grafiek te tonen.
        </div>
      ) : (
        <div className="pt-6">
          <svg viewBox={`0 0 ${chart.width} ${chart.height}`} className="h-auto w-full">
            {chart.tickValues.map((tick) => {
              const yPosition = chart.y(tick);
              return (
                <g key={tick}>
                  <line
                    x1={chart.padding.left}
                    x2={chart.width - chart.padding.right}
                    y1={yPosition}
                    y2={yPosition}
                    stroke="rgba(58,36,22,0.12)"
                    strokeDasharray="4 6"
                  />
                  <text
                    x={14}
                    y={yPosition + 5}
                    fontSize="14"
                    fill="rgba(58,36,22,0.78)"
                  >
                    {formatAxisEuro(tick)}
                  </text>
                </g>
              );
            })}

            <path d={chart.areaPath} fill="rgba(234,88,12,0.16)" />
            <path
              d={chart.linePath}
              fill="none"
              stroke="#ea580c"
              strokeWidth="4"
              strokeLinecap="round"
              strokeLinejoin="round"
            />

            {filtered.map((point, index) => {
              if (index !== filtered.length - 1) return null;

              return (
                <g key={`${point.day}-${point.price}`}>
                  <circle cx={chart.x(index)} cy={chart.y(point.price)} r="7" fill="#ffffff" />
                  <circle cx={chart.x(index)} cy={chart.y(point.price)} r="4" fill="#ea580c" />
                </g>
              );
            })}

            {chart.xLabelIndexes.map((index) => {
              const point = filtered[index];
              return (
                <text
                  key={`${point.day}-${index}`}
                  x={chart.x(index)}
                  y={chart.height - 8}
                  textAnchor={index === 0 ? "start" : index === filtered.length - 1 ? "end" : "middle"}
                  fontSize="14"
                  fill="rgba(58,36,22,0.72)"
                >
                  {formatMonthLabel(point.day)}
                </text>
              );
            })}
          </svg>
        </div>
      )}

      <div className="mt-6 grid gap-3 md:grid-cols-3">
        <div className="rounded-3xl border border-[rgba(234,88,12,0.28)] bg-[#fffaf6] px-5 py-4 text-center">
          <p className="text-sm text-neutral-600">Nu</p>
          <p className="mt-2 text-3xl font-semibold tracking-tight text-[color:var(--foreground)]">
            {formatEuro(currentPrice ?? latestPoint?.price ?? null)}
          </p>
        </div>

        <div className="rounded-3xl border border-[rgba(234,88,12,0.28)] bg-[#fffaf6] px-5 py-4 text-center">
          <p className="text-sm text-neutral-600">Laagste prijs in {WINDOW_LABELS[window]}</p>
          <p className="mt-2 text-3xl font-semibold tracking-tight text-[color:var(--foreground)]">
            {formatEuro(lowestInWindow)}
          </p>
        </div>

        <div className="rounded-3xl border border-[rgba(234,88,12,0.28)] bg-[#fffaf6] px-5 py-4 text-center">
          <p className="text-sm text-neutral-600">Gezien bij</p>
          <p className="mt-2 text-3xl font-semibold tracking-tight text-[color:var(--foreground)]">
            {maxShopCount} winkels
          </p>
        </div>
      </div>

      {latestPoint ? (
        <p className="mt-4 text-center text-xs text-neutral-500">
          Laatste datapunt: {formatDayLabel(latestPoint.day)}
        </p>
      ) : null}
    </section>
  );
}
