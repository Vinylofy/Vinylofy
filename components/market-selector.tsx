"use client";

import { useEffect, useId, useRef, useState } from "react";
import { Check, ChevronDown } from "lucide-react";
import type { PublicMarket } from "@/lib/markets";
import { trackCountryChange } from "@/lib/analytics";

function MarketFlag({ code }: { code: string }) {
  const className = "h-4 w-6 shrink-0 rounded-[2px] ring-1 ring-black/10";

  if (code === "NL") {
    return (
      <svg aria-hidden="true" className={className} viewBox="0 0 30 20">
        <path fill="#AE1C28" d="M0 0h30v7H0z" />
        <path fill="#fff" d="M0 7h30v6H0z" />
        <path fill="#21468B" d="M0 13h30v7H0z" />
      </svg>
    );
  }
  if (code === "BE") {
    return (
      <svg aria-hidden="true" className={className} viewBox="0 0 30 20">
        <path fill="#111" d="M0 0h10v20H0z" />
        <path fill="#F2C500" d="M10 0h10v20H10z" />
        <path fill="#EF3340" d="M20 0h10v20H20z" />
      </svg>
    );
  }
  if (code === "GB") {
    return (
      <svg aria-hidden="true" className={className} viewBox="0 0 30 20">
        <path fill="#012169" d="M0 0h30v20H0z" />
        <path d="M0 0l30 20M30 0L0 20" stroke="#fff" strokeWidth="5" />
        <path d="M0 0l30 20M30 0L0 20" stroke="#C8102E" strokeWidth="2" />
        <path d="M15 0v20M0 10h30" stroke="#fff" strokeWidth="7" />
        <path d="M15 0v20M0 10h30" stroke="#C8102E" strokeWidth="3" />
      </svg>
    );
  }
  if (code === "US") {
    return (
      <svg aria-hidden="true" className={className} viewBox="0 0 30 20">
        <path fill="#fff" d="M0 0h30v20H0z" />
        {Array.from({ length: 7 }, (_, index) => (
          <path key={index} fill="#B31942" d={`M0 ${index * 40 / 13}h30v${20 / 13}H0z`} />
        ))}
        <path fill="#0A3161" d="M0 0h13v11H0z" />
        {[2, 5, 8, 11].flatMap((x) => [2, 5, 8].map((y) => (
          <circle key={`${x}-${y}`} cx={x} cy={y} r="0.65" fill="#fff" />
        )))}
      </svg>
    );
  }

  return (
    <span aria-hidden="true" className={`${className} flex items-center justify-center bg-neutral-100 text-[9px] font-bold text-neutral-700`}>
      {code}
    </span>
  );
}

export function MarketSelector({ markets, selected }: {
  markets: PublicMarket[];
  selected: string;
}) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement>(null);
  const buttonRef = useRef<HTMLButtonElement>(null);
  const menuId = useId();
  const current = markets.find((market) => market.country_code === selected) ?? markets[0];

  useEffect(() => {
    if (!open) return;

    function closeOnOutsideClick(event: PointerEvent) {
      if (event.target instanceof Node && !rootRef.current?.contains(event.target)) {
        setOpen(false);
      }
    }
    function closeOnEscape(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setOpen(false);
        buttonRef.current?.focus();
      }
    }

    document.addEventListener("pointerdown", closeOnOutsideClick);
    document.addEventListener("keydown", closeOnEscape);
    return () => {
      document.removeEventListener("pointerdown", closeOnOutsideClick);
      document.removeEventListener("keydown", closeOnEscape);
    };
  }, [open]);

  if (!current) return null;

  function changeMarket(code: string) {
    setOpen(false);
    if (code === selected) return;
    trackCountryChange(selected, code);
    const url = new URL(window.location.href);
    url.searchParams.set("market", code);
    window.location.assign(`${url.pathname}${url.search}${url.hash}`);
  }

  return (
    <div ref={rootRef} className="relative z-40 inline-flex shrink-0 text-left">
      <button
        ref={buttonRef}
        type="button"
        aria-label={`Land voor aanbiedingen: ${current.name}`}
        aria-expanded={open}
        aria-controls={menuId}
        onClick={() => setOpen((value) => !value)}
        className="inline-flex min-h-10 items-center gap-2 rounded-full border border-neutral-200 bg-white px-3 py-1.5 text-sm font-medium text-neutral-800 shadow-sm transition hover:border-neutral-300 hover:bg-neutral-50 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-orange-600"
      >
        <MarketFlag code={current.country_code} />
        <span>{current.name}</span>
        <ChevronDown aria-hidden="true" className={`h-3.5 w-3.5 text-neutral-500 transition-transform ${open ? "rotate-180" : ""}`} />
      </button>

      {open ? (
        <div
          id={menuId}
          role="group"
          aria-label="Land voor aanbiedingen"
          className="absolute right-0 top-full mt-2 w-56 rounded-2xl border border-neutral-200 bg-white p-1.5 shadow-lg"
        >
          {markets.map((market) => (
            <button
              key={market.country_code}
              type="button"
              onClick={() => changeMarket(market.country_code)}
              aria-current={market.country_code === selected ? "true" : undefined}
              className="flex w-full items-center gap-3 rounded-xl px-3 py-2.5 text-left text-sm text-neutral-800 hover:bg-orange-50 focus-visible:outline focus-visible:outline-2 focus-visible:outline-orange-600"
            >
              <MarketFlag code={market.country_code} />
              <span className="flex-1">{market.name}</span>
              {market.country_code === selected ? <Check aria-hidden="true" className="h-4 w-4 text-orange-600" /> : null}
            </button>
          ))}
        </div>
      ) : null}
    </div>
  );
}
