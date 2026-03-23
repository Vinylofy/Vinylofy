import Link from "next/link";

type QuickLink = {
  href: string;
  label: string;
  Illustration: () => JSX.Element;
};

const links: QuickLink[] = [
  {
    href: "/nieuwe-releases",
    label: "Nieuwe releases",
    Illustration: NewReleaseIllustration,
  },
  {
    href: "/top-25",
    label: "Populaire vinyl",
    Illustration: Top25Illustration,
  },
  {
    href: "/topdeals",
    label: "BestOffers",
    Illustration: DealIllustration,
  },
];

export function HomeActionCards() {
  return (
    <nav
      aria-label="Snelle navigatie"
      className="mx-auto grid w-full max-w-[920px] grid-cols-3 gap-4 md:gap-8"
    >
      {links.map(({ href, label, Illustration }) => (
        <Link
          key={href}
          href={href}
          className="group flex flex-col items-center justify-start text-center"
        >
          <div className="flex h-[88px] items-center justify-center md:h-[96px]">
            <Illustration />
          </div>

          <span className="mt-2 text-sm font-medium text-neutral-700 transition group-hover:text-orange-600 md:text-base">
            {label}
          </span>
        </Link>
      ))}
    </nav>
  );
}

function NewReleaseIllustration() {
  return (
    <div className="relative h-[64px] w-[96px] md:h-[72px] md:w-[108px]">
      <div className="absolute right-0 top-[10px] h-[46px] w-[46px] rounded-full border-[6px] border-neutral-900 bg-neutral-950 shadow-sm md:h-[52px] md:w-[52px]" />
      <div className="absolute right-[18px] top-[22px] h-[12px] w-[12px] rounded-full border-[3px] border-neutral-100 bg-orange-500 md:right-[20px] md:top-[24px] md:h-[14px] md:w-[14px]" />

      <div className="absolute left-0 top-0 h-[58px] w-[52px] rotate-[-4deg] overflow-hidden rounded-md border border-sky-200 bg-gradient-to-br from-sky-300 via-sky-500 to-sky-700 shadow-sm md:h-[64px] md:w-[58px]">
        <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(255,255,255,0.28),transparent_28%),radial-gradient(circle_at_bottom_left,rgba(255,255,255,0.14),transparent_34%)]" />
        <div className="absolute right-1.5 top-1.5 h-2 w-2 rounded-full bg-white/80" />
        <div className="absolute left-2.5 top-4 h-6 w-6 rounded-full bg-yellow-200 shadow-sm md:left-3 md:top-4 md:h-7 md:w-7" />
        <div className="absolute left-[15px] top-[11px] text-sm text-white md:left-[17px] md:top-[13px] md:text-base">
          ★
        </div>
      </div>
    </div>
  );
}

function Top25Illustration() {
  return (
    <div className="relative flex h-[64px] w-[96px] items-center justify-center md:h-[72px] md:w-[108px]">
      <div className="absolute left-1 top-0 text-sm text-yellow-500 md:text-base">♪</div>
      <div className="absolute right-1 top-0 text-base text-orange-500 md:text-lg">♫</div>

      <div className="absolute left-1/2 top-1/2 h-[38px] w-[82px] -translate-x-1/2 -translate-y-1/2 rotate-[-6deg] rounded-lg bg-gradient-to-r from-orange-700 via-orange-500 to-red-500 shadow-sm md:h-[42px] md:w-[90px]" />
      <div className="absolute left-1/2 top-1/2 h-[38px] w-[82px] -translate-x-1/2 -translate-y-1/2 rotate-[4deg] rounded-lg border border-orange-100 bg-white/70 md:h-[42px] md:w-[90px]" />

      <div className="relative z-10 rounded-lg bg-gradient-to-r from-orange-600 to-orange-500 px-3 py-1.5 text-center text-lg font-black tracking-tight text-white shadow-sm md:px-3.5 md:py-1.5 md:text-xl">
        TOP 25
      </div>
    </div>
  );
}

function DealIllustration() {
  return (
    <div className="relative flex h-[64px] w-[96px] items-center justify-center md:h-[72px] md:w-[108px]">
      <svg
        viewBox="0 0 220 140"
        className="h-[56px] w-[84px] drop-shadow-sm transition group-hover:scale-[1.02] md:h-[62px] md:w-[94px]"
        aria-hidden="true"
      >
        <g transform="translate(8,8) rotate(-13 100 60)">
          <path
            d="M14 20C14 12.268 20.268 6 28 6H154L190 42V98C190 105.732 183.732 112 176 112H28C20.268 112 14 105.732 14 98V20Z"
            fill="#e24a1b"
          />
          <path
            d="M154 6L190 42H164C158.477 42 154 37.523 154 32V6Z"
            fill="#f78f57"
          />
          <circle cx="164" cy="28" r="10" fill="#fff5ef" />
          <circle cx="164" cy="28" r="4.4" fill="#d76c3a" />
          <text
            x="98"
            y="74"
            textAnchor="middle"
            fontSize="42"
            fontWeight="900"
            fill="#ffffff"
            fontFamily="Arial, Helvetica, sans-serif"
          >
            DEAL
          </text>
        </g>
      </svg>
    </div>
  );
}