"use client";

import { useMemo, useState } from "react";

import {
  formatEuro,
  type ProductPriceHistoryPoint,
} from "@/lib/vinylofy-data";

type PriceHistoryCardProps = {
  currentPrice: number | null;
  points: ProductPriceHistoryPoint[];
  asOfDay?: string;
};

const CHART_DAYS = 30;
const CHART_LABEL = "30D";
const MIN_POINTS_FOR_CHART = 2;
const MS_PER_DAY = 24 * 60 * 60 * 1000;

const NL_SHORT_MONTHS = [
  "jan",
  "feb",
  "mrt",
  "apr",
  "mei",
  "jun",
  "jul",
  "aug",
  "sep",
  "okt",
  "nov",
  "dec",
];

function parseIsoDay(value: string): Date {
  return new Date(`${value}T00:00:00Z`);
}

function formatIsoDay(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function addUtcDays(date: Date, days: number): Date {
  const next = new Date(date);
  next.setUTCDate(next.getUTCDate() + days);
  return next;
}

function dayDiff(from: string, to: string): number {
  return Math.round(
    (parseIsoDay(to).getTime() - parseIsoDay(from).getTime()) / MS_PER_DAY,
  );
}

function formatShortDayLabel(value: string) {
  const date = parseIsoDay(value);
  return `${date.getUTCDate()} ${NL_SHORT_MONTHS[date.getUTCMonth()]}`;
}

function formatDayLabel(value: string) {
  const date = parseIsoDay(value);
  return `${date.getUTCDate()} ${NL_SHORT_MONTHS[date.getUTCMonth()]} ${date.getUTCFullYear()}`;
}

function formatAxisEuro(value: number) {
  return `€ ${Math.round(value)}`;
}

function getWindowEnd(points: ProductPriceHistoryPoint[], asOfDay?: string): string | null {
  if (asOfDay) return asOfDay;
  return points[points.length - 1]?.day ?? null;
}

function filterLastDays(
  points: ProductPriceHistoryPoint[],
  days: number,
  asOfDay?: string,
): {
  filtered: ProductPriceHistoryPoint[];
  windowStart: string | null;
  windowEnd: string | null;
} {
  const sorted = points.slice().sort((a, b) => a.day.localeCompare(b.day));
  const windowEnd = getWindowEnd(sorted, asOfDay);

  if (!windowEnd) {
    return { filtered: [], windowStart: null, windowEnd: null };
  }

  const endDate = parseIsoDay(windowEnd);
  const startDate = addUtcDays(endDate, -(days - 1));
  const windowStart = formatIsoDay(startDate);

  return {
    filtered: sorted.filter((point) => point.day >= windowStart && point.day <= windowEnd),
    windowStart,
    windowEnd,
  };
}

function splitIntoDailySegments(
  points: ProductPriceHistoryPoint[],
): ProductPriceHistoryPoint[][] {
  const segments: ProductPriceHistoryPoint[][] = [];

  for (const point of points) {
    const activeSegment = segments[segments.length - 1];
    const previousPoint = activeSegment?.[activeSegment.length - 1];

    if (!activeSegment || !previousPoint) {
      segments.push([point]);
      continue;
    }

    if (dayDiff(previousPoint.day, point.day) === 1) {
      activeSegment.push(point);
    } else {
      segments.push([point]);
    }
  }

  return segments;
}

export function PriceHistoryCard({
  currentPrice,
  points,
  asOfDay,
}: PriceHistoryCardProps) {
  const [activeIndex, setActiveIndex] = useState<number | null>(null);

  const { filtered, windowStart, windowEnd } = useMemo(
    () => filterLastDays(points, CHART_DAYS, asOfDay),
    [points, asOfDay],
  );

  const activePoint =
    activeIndex !== null ? filtered[activeIndex] : filtered[filtered.length - 1] ?? null;
  const latestPoint = filtered[filtered.length - 1] ?? null;
  const lowestInWindow = filtered.reduce<number | null>((lowest, point) => {
    if (lowest === null || point.price < lowest) return point.price;
    return lowest;
  }, null);
  const maxShopCount = filtered.reduce(
    (highest, point) => Math.max(highest, point.shopCount),
    0,
  );
  const hasEnoughPoints = filtered.length >= MIN_POINTS_FOR_CHART;
  const isCurrentAtPeriodLow =
    currentPrice !== null && lowestInWindow !== null && currentPrice <= lowestInWindow;

  const chart = useMemo(() => {
    if (!hasEnoughPoints || !windowStart || !windowEnd) return null;

    const width = 920;
    const height = 330;
    const padding = { top: 22, right: 22, bottom: 36, left: 74 };
    const min = Math.min(...filtered.map((point) => point.price));
    const max = Math.max(...filtered.map((point) => point.price));
    const spread = Math.max(max - min, 2);
    const yMin = Math.max(0, Math.floor((min - spread * 0.18) * 2) / 2);
    const yMax = Math.ceil((max + spread * 0.18) * 2) / 2;
    const startDate = parseIsoDay(windowStart);
    const totalDays = Math.max(dayDiff(windowStart, windowEnd), 1);

    const x = (day: string) => {
      const offset = Math.max(0, Math.min(dayDiff(windowStart, day), totalDays));
      return (
        padding.left +
        (offset / totalDays) * (width - padding.left - padding.right)
      );
    };

    const y = (price: number) => {
      const ratio = (price - yMin) / Math.max(yMax - yMin, 1);
      return height - padding.bottom - ratio * (height - padding.top - padding.bottom);
    };

    const segments = splitIntoDailySegments(filtered);
    const linePaths = segments
      .filter((segment) => segment.length >= 2)
      .map((segment) =>
        segment
          .map((point, index) => `${index === 0 ? "M" : "L"} ${x(point.day)} ${y(point.price)}`)
          .join(" "),
      );

    const areaPaths = segments
      .filter((segment) => segment.length >= 2)
      .map((segment) => {
        const first = segment[0];
        const last = segment[segment.length - 1];

        return [
          `M ${x(first.day)} ${height - padding.bottom}`,
          ...segment.map((point) => `L ${x(point.day)} ${y(point.price)}`),
          `L ${x(last.day)} ${height - padding.bottom}`,
          "Z",
        ].join(" ");
      });

    const tickValues = Array.from({ length: 4 }, (_, index) => {
      const ratio = index / 3;
      return yMin + (yMax - yMin) * (1 - ratio);
    }).map((value) => Math.round(value));

    const xLabelDays = [
      windowStart,
      formatIsoDay(addUtcDays(startDate, Math.floor(totalDays / 2))),
      windowEnd,
    ];

    return {
      width,
      height,
      padding,
      x,
      y,
      linePaths,
      areaPaths,
      tickValues,
      xLabelDays,
    };
  }, [filtered, hasEnoughPoints, windowStart, windowEnd]);

  return (
    <section className="rounded-xl border border-[rgba(230,126,34,0.16)] bg-white p-4 shadow-sm md:p-5">
      <div className="flex flex-col gap-2.5 md:flex-row md:items-start md:justify-between">
        <div>
          <h2 className="text-lg font-semibold tracking-tight text-[#3f2616]">
            Prijsontwikkeling
          </h2>
          <p className="mt-1 text-sm text-[#7d6b5d]">
            Laagste waargenomen dagprijs in de laatste 30 dagen. Dagen zonder datapunt worden niet verbonden.
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <span className="rounded-full border border-[rgba(230,126,34,0.18)] bg-[#fffaf6] px-2.5 py-1 text-xs font-medium text-[#8a5a34]">
            Laatste {CHART_LABEL}
          </span>
          {isCurrentAtPeriodLow ? (
            <span className="rounded-full bg-[#e67e22] px-2.5 py-1 text-xs font-medium text-white">
              Laagste punt in {CHART_LABEL}
            </span>
          ) : null}
        </div>
      </div>

      {points.length === 0 ? (
        <div className="mt-4 rounded-xl border border-dashed border-[rgba(230,126,34,0.28)] bg-[#fffaf6] px-4 py-6 text-sm text-[#7d6b5d]">
          Nog geen prijshistorie beschikbaar voor dit product.
        </div>
      ) : !hasEnoughPoints ? (
        <div className="mt-4 rounded-xl border border-dashed border-[rgba(230,126,34,0.28)] bg-[#fffaf6] px-4 py-6 text-sm text-[#7d6b5d]">
          Er is al prijshistorie gevonden, maar nog te weinig voor een betrouwbare 30-daagse grafiek.
        </div>
      ) : chart ? (
        <>
          <div className="mt-4 rounded-2xl bg-[#fffaf6] px-2 py-3 md:px-4">
            <svg
              viewBox={`0 0 ${chart.width} ${chart.height}`}
              className="h-auto w-full overflow-visible"
              role="img"
              aria-label="Prijsontwikkeling van de laatste 30 dagen"
            >
              <defs>
                <linearGradient id="price-history-area" x1="0" x2="0" y1="0" y2="1">
                  <stop offset="0%" stopColor="#e67e22" stopOpacity="0.22" />
                  <stop offset="100%" stopColor="#e67e22" stopOpacity="0.02" />
                </linearGradient>
              </defs>

              <g>
                {chart.tickValues.map((tick, index) => {
                  const yPosition = chart.y(tick);

                  return (
                    <g key={`y-tick-${index}-${tick}`}>
                      <line
                        x1={chart.padding.left}
                        x2={chart.width - chart.padding.right}
                        y1={yPosition}
                        y2={yPosition}
                        stroke="rgba(63,38,22,0.08)"
                        strokeWidth="1"
                      />
                      <text
                        x={chart.padding.left - 14}
                        y={yPosition + 4}
                        textAnchor="end"
                        className="fill-[#8a7769] text-[22px]"
                      >
                        {formatAxisEuro(tick)}
                      </text>
                    </g>
                  );
                })}
              </g>

              {chart.areaPaths.map((areaPath, index) => (
                <path key={`area-${index}`} d={areaPath} fill="url(#price-history-area)" />
              ))}

              {chart.linePaths.map((linePath, index) => (
                <path
                  key={`line-${index}`}
                  d={linePath}
                  fill="none"
                  stroke="#e67e22"
                  strokeWidth="5"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              ))}

              <g>
                {filtered.map((point, index) => {
                  const cx = chart.x(point.day);
                  const cy = chart.y(point.price);
                  const isActive = index === (activeIndex ?? filtered.length - 1);

                  return (
                    <circle
                      key={point.day}
                      cx={cx}
                      cy={cy}
                      r={isActive ? 8 : 6}
                      fill={isActive ? "#e67e22" : "#fffaf6"}
                      stroke="#e67e22"
                      strokeWidth="4"
                      aria-label={`${formatDayLabel(point.day)} — ${formatEuro(point.price)}`}
                      onMouseEnter={() => setActiveIndex(index)}
                      onFocus={() => setActiveIndex(index)}
                      onMouseLeave={() => setActiveIndex(null)}
                      tabIndex={0}
                    >
                    </circle>
                  );
                })}
              </g>

              <g>
                {chart.xLabelDays.map((day) => (
                  <text
                    key={day}
                    x={chart.x(day)}
                    y={chart.height - 6}
                    textAnchor="middle"
                    className="fill-[#8a7769] text-[22px]"
                  >
                    {formatShortDayLabel(day)}
                  </text>
                ))}
              </g>
            </svg>
          </div>

          {activePoint ? (
            <div className="mt-3 rounded-xl border border-[rgba(63,38,22,0.08)] bg-[#fffdfb] px-3.5 py-3">
              <p className="text-sm font-medium text-[#3f2616]">
                {formatDayLabel(activePoint.day)} • Laagste dagprijs{" "}
                {formatEuro(activePoint.price)}
              </p>
              <p className="mt-1 text-xs text-[#8a7769]">
                Gezien bij {activePoint.shopCount}{" "}
                {activePoint.shopCount === 1 ? "winkel" : "winkels"}
              </p>
            </div>
          ) : null}
        </>
      ) : null}

      <div className="mt-4 grid gap-2 sm:grid-cols-3">
        <div className="rounded-xl border border-[rgba(230,126,34,0.18)] bg-[#fffaf6] px-3 py-3">
          <p className="text-xs uppercase tracking-[0.08em] text-[#8a7769]">Nu</p>
          <p className="mt-1.5 text-lg font-semibold text-[#3f2616]">
            {formatEuro(currentPrice ?? latestPoint?.price ?? null)}
          </p>
        </div>
        <div className="rounded-xl border border-[rgba(230,126,34,0.18)] bg-[#fffaf6] px-3 py-3">
          <p className="text-xs uppercase tracking-[0.08em] text-[#8a7769]">
            Laagste prijs in {CHART_LABEL}
          </p>
          <p className="mt-1.5 text-lg font-semibold text-[#3f2616]">
            {formatEuro(lowestInWindow)}
          </p>
        </div>
        <div className="rounded-xl border border-[rgba(230,126,34,0.18)] bg-[#fffaf6] px-3 py-3">
          <p className="text-xs uppercase tracking-[0.08em] text-[#8a7769]">
            Gezien bij
          </p>
          <p className="mt-1.5 text-lg font-semibold text-[#3f2616]">
            {maxShopCount} {maxShopCount === 1 ? "winkel" : "winkels"}
          </p>
        </div>
      </div>
    </section>
  );
}
