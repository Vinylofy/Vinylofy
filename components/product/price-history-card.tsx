"use client";

import { useMemo, useState } from "react";
import { formatEuro, type ProductPriceHistoryPoint } from "@/lib/vinylofy-data";

type PriceHistoryCardProps = {
  currentPrice: number | null;
  points: ProductPriceHistoryPoint[];
};

const CHART_DAYS = 10;
const CHART_LABEL = "10D";
const MIN_POINTS_FOR_CHART = 2;

function parseIsoDay(value: string): Date {
  return new Date(`${value}T00:00:00Z`);
}

function filterLastDays(points: ProductPriceHistoryPoint[], days: number): ProductPriceHistoryPoint[] {
  if (points.length === 0) return [];

  const lastPoint = parseIsoDay(points[points.length - 1].day);
  const minDate = new Date(lastPoint);
  minDate.setUTCDate(minDate.getUTCDate() - (days - 1));

  return points.filter((point) => parseIsoDay(point.day) >= minDate);
}

function formatShortDayLabel(value: string) {
  return new Intl.DateTimeFormat("nl-NL", {
    day: "numeric",
    month: "short",
  })
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

export function PriceHistoryCard({ currentPrice, points }: PriceHistoryCardProps) {
  const [activeIndex, setActiveIndex] = useState<number | null>(null);

  const filtered = useMemo(() => filterLastDays(points, CHART_DAYS), [points]);
  const activePoint = activeIndex !== null ? filtered[activeIndex] : filtered[filtered.length - 1] ?? null;
  const latestPoint = filtered[filtered.length - 1] ?? null;

  const lowestInWindow = filtered.reduce<number | null>((lowest, point) => {
    if (lowest === null || point.price < lowest) return point.price;
    return lowest;
  }, null);

  const maxShopCount = filtered.reduce((highest, point) => Math.max(highest, point.shopCount), 0);
  const hasEnoughPoints = filtered.length >= MIN_POINTS_FOR_CHART;
  const isCurrentAtPeriodLow = currentPrice !== null && lowestInWindow !== null && currentPrice <= lowestInWindow;

  const chart = useMemo(() => {
    if (!hasEnoughPoints) return null;

    const width = 920;
    const height = 232;
    const padding = { top: 16, right: 18, bottom: 28, left: 58 };

    const min = Math.min(...filtered.map((point) => point.price));
    const max = Math.max(...filtered.map((point) => point.price));
    const spread = Math.max(max - min, 2);
    const yMin = Math.max(0, Math.floor((min - spread * 0.18) * 2) / 2);
    const yMax = Math.ceil((max + spread * 0.18) * 2) / 2;

    const x = (index: number) => {
      if (filtered.length === 1) return padding.left;
      return padding.left + (index / (filtered.length - 1)) * (width - padding.left - padding.right);
    };

    const y = (price: number) => {
      const ratio = (price - yMin) / Math.max(yMax - yMin, 1);
      return height - padding.bottom - ratio * (height - padding.top - padding.bottom);
    };

    const linePath = filtered
      .map((point, index) => `${index === 0 ? "M" : "L"} ${x(index)} ${y(point.price)}`)
      .join(" ");

    const areaPath = [
      `M ${x(0)} ${height - padding.bottom}`,
      ...filtered.map((point, index) => `L ${x(index)} ${y(point.price)}`),
      `L ${x(filtered.length - 1)} ${height - padding.bottom}`,
      "Z",
    ].join(" ");

    const tickValues = Array.from({ length: 4 }, (_, index) => {
      const ratio = index / 3;
      return yMin + (yMax - yMin) * (1 - ratio);
    }).map((value) => Math.round(value));

    const xLabelIndexes = Array.from(new Set([0, Math.floor((filtered.length - 1) / 2), filtered.length - 1]));

    return { width, height, padding, x, y, linePath, areaPath, tickValues, xLabelIndexes };
  }, [filtered, hasEnoughPoints]);

  return (
    <section className="rounded-xl border border-[rgba(230,126,34,0.16)] bg-white p-4 shadow-sm md:p-5">
      <div className="flex flex-col gap-2.5 border-b border-[rgba(63,38,22,0.08)] pb-3">
        <div className="flex flex-wrap items-center gap-2">
          <h2 className="text-lg font-semibold tracking-tight text-[#3f2616]">Prijsontwikkeling</h2>
          <span className="rounded-full border border-[rgba(230,126,34,0.22)] bg-[#fff7f0] px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.12em] text-[#c46817]">
            Laatste {CHART_LABEL}
          </span>
          {isCurrentAtPeriodLow ? (
            <span className="rounded-full border border-[rgba(230,126,34,0.22)] bg-[#fff7f0] px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.12em] text-[#c46817]">
              Periode-laag
            </span>
          ) : null}
        </div>
        <p className="text-sm text-[#7d6b5d]">
          Tijdelijke validatieversie op basis van de laagste waargenomen dagprijs van de laatste 10 dagen.
        </p>
      </div>

      {points.length === 0 ? (
        <div className="mt-4 rounded-xl border border-dashed border-[rgba(230,126,34,0.28)] bg-[#fffaf6] px-4 py-6 text-sm leading-6 text-[#7d6b5d]">
          Nog geen prijshistorie beschikbaar voor dit product. Zodra Vinylofy voldoende dagwaarnemingen heeft verzameld, verschijnt hier de 10-daagse prijsgrafiek.
        </div>
      ) : !hasEnoughPoints ? (
        <div className="mt-4 rounded-xl border border-dashed border-[rgba(230,126,34,0.28)] bg-[#fffaf6] px-4 py-6 text-sm leading-6 text-[#7d6b5d]">
          Er is al prijshistorie gevonden, maar nog te weinig voor een betrouwbare 10-daagse grafiek. Kom later terug voor een vollediger beeld.
        </div>
      ) : chart ? (
        <div className="mt-4 rounded-xl border border-[rgba(63,38,22,0.08)] bg-[#fffdfb] p-2.5 md:p-3">
          <svg viewBox={`0 0 ${chart.width} ${chart.height}`} className="h-auto w-full overflow-visible">
            {chart.tickValues.map((tick) => {
              const yPosition = chart.y(tick);
              return (
                <g key={tick}>
                  <line
                    x1={chart.padding.left}
                    x2={chart.width - chart.padding.right}
                    y1={yPosition}
                    y2={yPosition}
                    stroke="rgba(63,38,22,0.10)"
                    strokeDasharray="4 6"
                  />
                  <text x={8} y={yPosition + 4} fontSize="11" fill="rgba(63,38,22,0.68)">
                    {formatAxisEuro(tick)}
                  </text>
                </g>
              );
            })}

            <path d={chart.areaPath} fill="rgba(230,126,34,0.16)" />
            <path
              d={chart.linePath}
              fill="none"
              stroke="#e67e22"
              strokeWidth="3"
              strokeLinecap="round"
              strokeLinejoin="round"
            />

            {filtered.map((point, index) => {
              const x = chart.x(index);
              const y = chart.y(point.price);
              const isActive = index === (activeIndex ?? filtered.length - 1);

              return (
                <g key={`${point.day}-${point.price}-${index}`}>
                  <circle cx={x} cy={y} r={isActive ? 4.5 : 0} fill="#ffffff" stroke="#e67e22" strokeWidth="2.5" />
                  <circle
                    cx={x}
                    cy={y}
                    r={12}
                    fill="transparent"
                    onMouseEnter={() => setActiveIndex(index)}
                    onFocus={() => setActiveIndex(index)}
                    onMouseLeave={() => setActiveIndex(null)}
                    tabIndex={0}
                  >
                    <title>
                      {formatDayLabel(point.day)} — {formatEuro(point.price)}
                    </title>
                  </circle>
                </g>
              );
            })}

            {chart.xLabelIndexes.map((index) => {
              const point = filtered[index];
              return (
                <text
                  key={`${point.day}-${index}`}
                  x={chart.x(index)}
                  y={chart.height - 7}
                  textAnchor={index === 0 ? "start" : index === filtered.length - 1 ? "end" : "middle"}
                  fontSize="11"
                  fill="rgba(63,38,22,0.68)"
                >
                  {formatShortDayLabel(point.day)}
                </text>
              );
            })}
          </svg>

          {activePoint ? (
            <div className="mt-2.5 flex flex-wrap items-center justify-between gap-2 rounded-xl border border-[rgba(230,126,34,0.18)] bg-[#fffaf6] px-3 py-2 text-xs text-[#7d6b5d] md:text-sm">
              <div>
                <span className="font-semibold text-[#3f2616]">{formatDayLabel(activePoint.day)}</span>
                <span className="mx-1.5 text-[#c7b6aa]">•</span>
                <span>Laagste dagprijs {formatEuro(activePoint.price)}</span>
              </div>
              <div className="font-medium">Gezien bij {activePoint.shopCount} winkels</div>
            </div>
          ) : null}
        </div>
      ) : null}

      <div className="mt-4 grid gap-2 md:grid-cols-3">
        <div className="rounded-xl border border-[rgba(230,126,34,0.24)] bg-[#fffaf6] px-3 py-3 text-center">
          <p className="text-[11px] uppercase tracking-[0.08em] text-[#8a7769]">Nu</p>
          <p className="mt-1.5 text-lg font-semibold tracking-tight text-[#3f2616] md:text-xl">
            {formatEuro(currentPrice ?? latestPoint?.price ?? null)}
          </p>
        </div>

        <div className="rounded-xl border border-[rgba(230,126,34,0.24)] bg-[#fffaf6] px-3 py-3 text-center">
          <p className="text-[11px] uppercase tracking-[0.08em] text-[#8a7769]">Laagste prijs in {CHART_LABEL}</p>
          <p className="mt-1.5 text-lg font-semibold tracking-tight text-[#3f2616] md:text-xl">
            {formatEuro(lowestInWindow)}
          </p>
        </div>

        <div className="rounded-xl border border-[rgba(230,126,34,0.24)] bg-[#fffaf6] px-3 py-3 text-center">
          <p className="text-[11px] uppercase tracking-[0.08em] text-[#8a7769]">Gezien bij</p>
          <p className="mt-1.5 text-lg font-semibold tracking-tight text-[#3f2616] md:text-xl">
            {maxShopCount} winkels
          </p>
        </div>
      </div>
    </section>
  );
}
